"""
Carga avulsa dos extratos bancários d'O Closet (EMP013) para a tabela
temporária `importacoes` — a mesma que as importações do Fluxo de Caixa enchem
e que a tela de transferência leva para `lancamentos`.

Ainda não é rotina da tela: um dia vira a opção "Importar OFX / CSV". Por ora:

    python3 carga_extratos_ocloset.py <pasta>            # só simula
    python3 carga_extratos_ocloset.py <pasta> --aplicar  # grava

Arquivos lidos na pasta (em qualquer subpasta):
    *.ofx  -> Itaú (BANKID 0341), Sicredi (748) ou Banco do Brasil (1)
    *.pdf  -> extrato da conta Mercado Pago (só entram os movimentos que o
              relatório de vendas não traz: os PIX de saque para os bancos)
    *.csv  -> relatório de vendas do Mercado Pago, ou a planilha do caixa em
              dinheiro da loja (cabeçalho Data;Origem;Entrada;Saida)

Cada linha leva em `complemento` a marca "EXTRATO <conta> <id>". Rodar de novo
apaga primeiro as linhas com essa marca da mesma conta nos meses do arquivo:
não duplica, e não encosta no que veio de outra importação.

Pré-classificação: as regras de `REGRAS` (primeira que casa decide) e, para o
que sobrar, o motor de sempre (`classificar_lancamentos_importados`, que lê
`classificacoes_automaticas`). O que nenhum dos dois reconhece fica em branco
para ser classificado na tela.

Mercado Pago: cada venda vira duas linhas — o valor bruto como receita e a
taxa como tarifa (4/11) —, que somadas dão o líquido creditado na conta.
"""

import csv
import io
import os
import re
import sys
import unicodedata
from datetime import date, datetime
from decimal import Decimal

from psycopg2.extras import execute_values

from db import get_connection


COD_EMPRESA = "EMP013"
COD_FILIAL = 1

CONTAS_OFX = {"0341": "Itaú CC", "748": "Sicredi CC", "1": "BB CC"}
CONTA_MERCADO_PAGO = "Mercado Pago"
CONTA_CAIXA = "Caixa Dinheiro"

# CNPJ da própria O Closet (RB COMERCIO DE CONFECCOES): PIX entre as contas dela.
CNPJ_PROPRIO = "26298072000150"


def _norm(texto):
    texto = unicodedata.normalize("NFKD", str(texto or ""))
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    return re.sub(r"[^A-Z0-9]+", " ", texto.upper()).strip()


# (trecho normalizado, sinal exigido: "+", "-" ou None, grupo, conta)
# A ordem importa: a primeira regra que casa decide.
REGRAS = [
    # entre contas da própria empresa
    ("RB COMERCIO DE CONFECCOES", None, 7, 1),
    (CNPJ_PROPRIO, None, 7, 1),
    ("CONFECCOES LTDA", None, 7, 1),    # nome quebrado na virada de página do PDF

    # recebíveis de cartão
    ("STONE DEB", "+", 1, 6),
    ("STONE ANTEC", "+", 1, 3),
    ("STONE", "+", 1, 3),
    ("RECEBIMENTO REDE", "+", 1, 3),
    ("LIQ COBRANCA SIMPLES", "+", 1, 2),

    # empréstimos e financiamentos
    ("LIBERACAO CREDITO", "+", 2, 14),       # antecipação de recebíveis
    ("CRED COBRANCA DESCONTADA", "+", 2, 14),
    ("RENDIMENTOS", "+", 2, 4),
    ("MULTA", "-", 4, 15),
    ("JUROS", "-", 4, 15),
    ("AMORTIZACAO CONTRATO", "-", 5, 7),
    ("LIQUIDACAO DE PARCELA", "-", 5, 7),
    ("PARCIAL GIRO", "-", 5, 7),
    ("PARCELA GIRO", "-", 5, 7),
    ("RENEGOCIA", "-", 5, 7),
    ("RENEGOCIACAO", "-", 5, 7),
    ("PRONAMPE", "-", 5, 7),

    # banco: impostos e tarifas
    ("IOF", "-", 4, 1),
    ("TARIFA", "-", 4, 11),
    ("TAR", "-", 4, 11),
    ("CUSTAS", "-", 4, 11),

    # impostos
    ("DARF", "-", 4, 1),
    ("SEFAZ", "-", 4, 1),
    ("SEFRNIC", "-", 4, 1),

    # sócios: o que entra é aporte (2/1); o que sai é pagamento de
    # empréstimo de sócio (5/2) — saída não é aporte negativo
    ("MICHELLE BARBOSA NASSER", "+", 2, 1),
    ("MICHELLE BARBOSA NASSER", "-", 5, 2),
    ("ROSANNE BASTOS", "+", 2, 1),
    ("ROSANNE BASTOS", "-", 5, 2),
    ("MERCADO PAGO INSTITUICAO", "-", 5, 2),   # cartão Mercado Pago da Rosanne (julho)

    # BB: aplicação automática e tentativas de débito do cartão estornadas
    ("RENDE FACIL", None, 7, 1),
    ("PAGTO CARTAO CREDITO", None, 5, 11),
    ("ESTORNO DE DEBITO", None, 5, 11),
    ("DEP DINHEIRO", "+", 7, 1),        # dinheiro do caixa da loja (a venda já está lá)
    ("CIELO", "+", 1, 4),
    ("DAS", "-", 4, 1),

    # pessoal — funcionárias da folha de julho/agosto
    ("HEVILYN", "-", 4, 2),
    ("ANNE BEATRIZ", "-", 4, 2),
    ("LIVIA MARIA DE MENEZES", "-", 4, 2),
    ("MARIA EDUARDA DA SILVA REBOUCAS", "-", 4, 2),
    ("MARIA TASSILA", "-", 4, 2),
    ("LETICIA DA SILVA PEDRA", "-", 4, 2),
    ("MARIA CLARA OLIVEIRA DE SOUSA", "-", 4, 2),
    ("MABILY", "-", 4, 2),
    ("SAMARA MENDES", "-", 4, 2),
    ("THUANNY NADJA", "-", 4, 2),
    ("LUCCA CAMINHA", "-", 5, 2),       # pagamento a sócio
    ("DEB CTA FATURA", "-", 5, 11),     # fatura do cartão Sicredi
    ("INTEGR CAPITAL SUBSCRITO", "-", 5, 13),  # cota da cooperativa
    ("CEF MATRIZ", "-", 4, 2),          # guia de FGTS

    # fornecedores de mercadoria
    ("LIVE ROUPAS", "-", 3, 1),
    ("AUTHEN", "-", 3, 1),
    ("RECCO", "-", 3, 1),
    ("PINK CHEEKS", "-", 3, 1),
    ("NS VESTUARIO", "-", 3, 1),
    ("BANCO CSF", "-", 3, 1),           # cartão Sam's Club, gastos da loja
    ("VINDI", "-", 3, 2),               # frete (Melhor Envio)
    ("BEE DELIVERY", "-", 3, 2),

    # despesas
    ("SECRAN", "-", 4, 4),
    ("CA SERVICOS DE INFORMATICA", "-", 4, 4),
    ("CAMARA DIRIG", "-", 4, 4),        # CDL
    ("LISBOA MARCENARIA", "-", 4, 4),
    ("ATELIE BENDITO CACTO", "-", 4, 4),
    ("CLARO", "-", 4, 8),
    ("UNIDAS LOCADORA", "-", 4, 14),
    ("GRAFICA PEDRO", "-", 4, 13),
    ("GRID COMUNICACAO", "-", 4, 13),
    ("TATIANA VITORIA", "-", 4, 13),    # provador (TATI)
    ("GERANDO TRAFEGO", "-", 4, 13),
    ("CYM PRODUCOES", "-", 4, 13),

    # PIX de cliente
    ("PIX RECEBIDO", "+", 1, 8),
    ("RECEBIMENTO PIX", "+", 1, 8),
]


def classificar(historico, valor):
    texto = " " + _norm(historico) + " "
    for trecho, sinal, grupo, conta in REGRAS:
        if sinal == "+" and valor <= 0:
            continue
        if sinal == "-" and valor >= 0:
            continue
        if " " + _norm(trecho) + " " in texto:
            return grupo, conta
    return None, None


# --------------------------------------------------------------------------
# Leitura
# --------------------------------------------------------------------------

def _ler_texto(caminho):
    bruto = open(caminho, "rb").read()
    for cod in ("utf-8-sig", "cp1252"):
        try:
            return bruto.decode(cod)
        except UnicodeDecodeError:
            continue
    return bruto.decode("latin-1")


def _tag(bloco, nome):
    m = re.search(rf"<{nome}>([^<\r\n]*)", bloco)
    return m.group(1).strip() if m else ""


def ler_ofx(caminho):
    texto = _ler_texto(caminho)
    bankid = _tag(texto, "BANKID").lstrip("0") or "0"
    conta = CONTAS_OFX.get(_tag(texto, "BANKID")) or CONTAS_OFX.get(bankid)
    if not conta:
        raise ValueError(f"{caminho}: banco {_tag(texto, 'BANKID')} sem conta cadastrada em CONTAS_OFX")

    linhas = []
    for bloco in re.findall(r"<STMTTRN>(.*?)</STMTTRN>", texto, re.S):
        d = _tag(bloco, "DTPOSTED")[:8]
        # BB manda a operação em NAME e o detalhe em MEMO; os outros, tudo em MEMO
        memo = " - ".join(p for p in (_tag(bloco, "NAME"), _tag(bloco, "MEMO")) if p)
        memo = re.sub(r"\s+", " ", memo).strip()
        linhas.append({
            "conta_banco": conta,
            "data": date(int(d[:4]), int(d[4:6]), int(d[6:8])),
            "historico": memo or "(sem descrição)",
            "valor": Decimal(_tag(bloco, "TRNAMT")),
            "id": _tag(bloco, "FITID"),
        })
    saldo = _tag(texto, "BALAMT")
    return conta, linhas, Decimal(saldo) if saldo else None


def _dec_br(texto):
    texto = (texto or "").strip()
    return Decimal(texto.replace(".", "").replace(",", ".")) if texto else Decimal("0")


def _forma_mercado_pago(r):
    """(rótulo, grupo, conta) da receita, pelo meio de pagamento."""
    meio = (r["PAYMENT_METHOD_DETAIL"] or "").strip()
    bandeira = (r["PAYMENT_METHOD"] or "").strip()
    if meio == "Pix":
        return "Pix", 1, 8
    if meio == "Saldo em conta":
        return "Saldo em conta", 1, 4
    if meio == "Cartão de débito":
        return f"Débito {bandeira}", 1, 6
    if meio == "Cartão de crédito":
        return f"Crédito {bandeira}", 1, 3
    # "Point;Master;;" — bandeira na coluna do meio e taxa de 0,99%: é débito (Maestro)
    return f"Débito {meio}", 1, 6


def ler_csv_mercado_pago(caminho):
    texto = _ler_texto(caminho).strip()
    linhas = []
    for r in csv.DictReader(io.StringIO(texto), delimiter=";"):
        if not r.get("OPERATION_DATETIME"):
            continue
        dt = datetime.strptime(r["OPERATION_DATETIME"].strip()[:10], "%d-%m-%Y").date()
        bruto, taxa = _dec_br(r["GROSS_VALUE"]), _dec_br(r["SALES_DISCOUNTS"])
        forma, grupo, conta = _forma_mercado_pago(r)
        canal = (r["CHARGE_METHOD"] or "").strip()
        pid = r["PAYMENT_ID"].strip()
        tipo = r["MOVEMENT_TYPE"].strip()

        if tipo == "Reembolso":
            # devolução de venda reduz a receita (mesma regra de julho: 1/1)
            linhas.append({"conta_banco": CONTA_MERCADO_PAGO, "data": dt, "valor": bruto,
                           "historico": f"DEVOLUCAO MERCADO PAGO {canal} {forma} #{pid}",
                           "id": f"{pid}-V", "grupo": 1, "conta": 1})
        else:
            linhas.append({"conta_banco": CONTA_MERCADO_PAGO, "data": dt, "valor": bruto,
                           "historico": f"VENDA MERCADO PAGO {canal} {forma} #{pid}",
                           "id": f"{pid}-V", "grupo": grupo, "conta": conta})
        if taxa:
            linhas.append({"conta_banco": CONTA_MERCADO_PAGO, "data": dt, "valor": taxa,
                           "historico": f"TAXA MERCADO PAGO {canal} {forma} #{pid}",
                           "id": f"{pid}-T", "grupo": 4, "conta": 11})
    return CONTA_MERCADO_PAGO, linhas, None


def ler_pdf_mercado_pago(caminho, ids_vendas):
    """
    Extrato da conta Mercado Pago. Cada venda já veio do relatório (bruto +
    taxa = a "Liberação de dinheiro" daqui, com o mesmo ID da operação), então
    daqui só entra o que não está lá — na prática, os saques por PIX.
    """
    import subprocess
    texto = subprocess.run(["pdftotext", "-layout", caminho, "-"],
                           capture_output=True, text=True, check=True).stdout
    linhas = []
    for bloco in re.split(r"\n\s*\n", texto):
        m = re.search(r"(\d\d-\d\d-\d{4})\s+.*?\s+(\d{9,})\s+R\$ (-?[\d.]+,\d\d)", bloco)
        if not m or m.group(2) in ids_vendas:
            continue
        desc = " ".join(re.sub(r"\d\d-\d\d-\d{4}|\d{9,}|R\$ -?[\d.]+,\d\d", "", l).strip()
                        for l in bloco.split("\n"))
        linhas.append({"conta_banco": CONTA_MERCADO_PAGO,
                       "data": datetime.strptime(m.group(1), "%d-%m-%Y").date(),
                       "historico": re.sub(r"\s+", " ", desc).strip().upper(),
                       "valor": _dec_br(m.group(3)), "id": m.group(2)})
    return CONTA_MERCADO_PAGO + " (saques)", linhas, None


# Caixa em dinheiro: entrada sem regra é venda à vista (1/1).
REGRAS_CAIXA = [
    ("SALDO", None, None, None),        # saldo do mês anterior não é movimento
    ("SEM ORIGEM", None, None, None),
    ("LUCCA", "-", 5, 2),               # retirada do sócio
    ("DEPOSITO", "-", 7, 1),            # vai para o banco ("Dep dinheiro" no BB)
    ("TROCO", "-", 1, 1),               # devolve parte da venda
    ("ALMOCO", "-", 4, 2),
    ("ASO", "-", 4, 2),
    ("VIGIA", "-", 4, 4),
    ("IMPRESSORA", "-", 4, 4),
    ("PAPEL OFICIO", "-", 4, 4),
    ("PAPEL SEDA", "-", 3, 1),          # embalagem, como as sacolas
    ("BAZAR", "-", 4, 6),
    ("FROTA", "-", 4, 14),
]


def ler_csv_caixa(caminho):
    linhas = []
    for r in csv.DictReader(io.StringIO(_ler_texto(caminho).strip()), delimiter=";"):
        origem = r["Origem"].strip()
        valor = _dec_br(r["Entrada"]) - _dec_br(r["Saida"])
        texto = " " + _norm(origem) + " "
        regra = next((x for x in REGRAS_CAIXA if " " + _norm(x[0]) + " " in texto
                      and (x[1] is None or (x[1] == "-") == (valor < 0))), None)
        if regra and regra[0] == "SALDO":
            continue
        grupo, conta = (regra[2], regra[3]) if regra else ((1, 1) if valor > 0 else (None, None))
        linhas.append({"conta_banco": CONTA_CAIXA,
                       "data": datetime.strptime(r["Data"].strip(), "%d/%m/%Y").date(),
                       "historico": f"CAIXA DINHEIRO {origem.upper()}", "valor": valor,
                       "id": f"{len(linhas) + 1:03d}", "grupo": grupo, "conta": conta})
    return CONTA_CAIXA, linhas, None


# --------------------------------------------------------------------------
# Gravação
# --------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    pasta, aplicar = sys.argv[1], "--aplicar" in sys.argv

    arquivos, pdfs_mp = [], []
    for raiz, _, nomes in os.walk(pasta):
        for n in sorted(nomes):
            ext = n.lower().rsplit(".", 1)[-1]
            if ext == "ofx":
                arquivos.append(ler_ofx(os.path.join(raiz, n)))
            elif ext == "csv":
                caminho = os.path.join(raiz, n)
                cabecalho = _ler_texto(caminho).strip().split("\n", 1)[0]
                leitor = ler_csv_caixa if cabecalho.startswith("Data;Origem") else ler_csv_mercado_pago
                arquivos.append(leitor(caminho))
            elif ext == "pdf" and "mercado" in raiz.lower():
                pdfs_mp.append(os.path.join(raiz, n))
    ids_vendas = {l["id"][:-2] for c, ls, _ in arquivos if c == CONTA_MERCADO_PAGO for l in ls}
    for caminho in pdfs_mp:
        arquivos.append(ler_pdf_mercado_pago(caminho, ids_vendas))

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT cod_grupo, cod_conta, descricao FROM contas_gerenciais WHERE cod_empresa = %s",
                (COD_EMPRESA,))
    nomes_conta = {(g, c): d for g, c, d in cur.fetchall()}
    cur.execute("SELECT nome_filial FROM filiais WHERE cod_empresa = %s AND cod_filial = %s",
                (COD_EMPRESA, COD_FILIAL))
    nome_filial = cur.fetchone()[0]

    registros = []
    for conta_banco, linhas, saldo in arquivos:
        sem = 0
        for l in linhas:
            if "grupo" not in l:
                l["grupo"], l["conta"] = classificar(l["historico"], l["valor"])
            sem += l["grupo"] is None
            registros.append(l)
        total = sum(l["valor"] for l in linhas)
        print(f"{conta_banco:12} {len(linhas):4} linhas  movimento {total:>12,.2f}"
              f"  sem classificação: {sem}" + (f"  saldo final no arquivo {saldo:,.2f}" if saldo is not None else ""))

    print()
    resumo = {}
    for r in registros:
        chave = (r["grupo"], r["conta"])
        q, v = resumo.get(chave, (0, Decimal("0")))
        resumo[chave] = (q + 1, v + r["valor"])
    for (g, c), (q, v) in sorted(resumo.items(), key=lambda x: (x[0][0] or 99, x[0][1] or 99)):
        rot = f"{g}/{c} {nomes_conta.get((g, c)) or ''}" if g else "(sem classificação)"
        print(f"  {rot:45} {q:4}  {v:>12,.2f}")

    print("\nSem classificação:")
    for r in registros:
        if r["grupo"] is None:
            print(f"  {r['conta_banco']:11} {r['data']:%d/%m} {r['valor']:>10,.2f}  {r['historico']}")

    if not aplicar:
        print("\nSimulação — nada gravado. Rode com --aplicar para gravar.")
        return

    # Apaga o que uma rodada anterior gravou (resumo e detalhe) nas mesmas
    # contas e meses — rodar de novo não duplica.
    for conta_banco, ano, mes in sorted({(r["conta_banco"], r["data"].year, r["data"].month)
                                         for r in registros}):
        cur.execute("""
            DELETE FROM importacoes
            WHERE cod_empresa = %s AND conta_banco = %s AND ano = %s AND mes = %s
              AND complemento LIKE 'EXTRATO %%'
        """, (COD_EMPRESA, conta_banco, ano, mes))
        cur.execute("""
            DELETE FROM importacoes_detalhamento
            WHERE cod_empresa = %s AND codigo_conta LIKE %s AND ano = %s AND mes = %s
              AND complemento LIKE 'EXTRATO %%'
        """, (COD_EMPRESA, conta_banco.upper() + " - %", ano, mes))

    # Resumo em `importacoes` (é o que a tela classifica e o matricial soma) e
    # cada lançamento do extrato em `importacoes_detalhamento` (o que abre no
    # clique da célula). Os dois se ligam pelo histórico do resumo:
    #   - classificado: um resumo por conta bancária x conta gerencial no mês
    #     ("SICREDI CC - PESSOAL"), com todos os lançamentos dela por baixo;
    #   - sem classificação: um resumo por lançamento, com data no texto, para
    #     cada um poder ser classificado sozinho na tela.
    resumos = {}
    for r in registros:
        if r["grupo"]:
            nome = nomes_conta.get((r["grupo"], r["conta"])) or f"{r['grupo']}.{r['conta']}"
            historico = f"{r['conta_banco']} - {nome}".upper()
        else:
            base = f"{r['conta_banco']} - {r['data']:%d/%m} {r['historico']}".upper()
            historico, n = base, 2
            while (r["data"].year, r["data"].month, historico) in resumos:
                historico, n = f"{base} ({n})", n + 1
        chave = (r["data"].year, r["data"].month, historico)
        res = resumos.setdefault(chave, {"conta_banco": r["conta_banco"], "historico": historico,
                                         "grupo": r["grupo"], "conta": r["conta"], "data": r["data"],
                                         "valor": Decimal("0")})
        res["valor"] += r["valor"]
        res["data"] = max(res["data"], r["data"])
        r["historico_resumo"] = historico

    execute_values(cur, """
        INSERT INTO importacoes (cod_empresa, cod_filial, nome_filial, conta_banco, data, ano, mes,
                                 historico, valor, grupo, conta, descricao_conta, complemento)
        VALUES %s
    """, [(COD_EMPRESA, COD_FILIAL, nome_filial, x["conta_banco"], x["data"], ano, mes,
           x["historico"], x["valor"], x["grupo"], x["conta"],
           nomes_conta.get((x["grupo"], x["conta"])) if x["grupo"] else None,
           f"EXTRATO {x['conta_banco']}") for (ano, mes, _), x in resumos.items()])

    execute_values(cur, """
        INSERT INTO importacoes_detalhamento (cod_empresa, cod_filial, nome_filial, ano, mes, data,
                                              codigo_conta, historico_conta, descricao, valor,
                                              grupo, conta, descricao_conta, complemento)
        VALUES %s
    """, [(COD_EMPRESA, COD_FILIAL, nome_filial, r["data"].year, r["data"].month, r["data"],
           r["historico_resumo"], r["historico_resumo"], r["historico"], r["valor"],
           str(r["grupo"]) if r["grupo"] else None, str(r["conta"]) if r["grupo"] else None,
           nomes_conta.get((r["grupo"], r["conta"])) if r["grupo"] else None,
           f"EXTRATO {r['conta_banco']} {r['id']}") for r in registros])
    conn.commit()

    from importa_web_postos import (classificar_lancamentos_importados,
                                    propagar_classificacao_detalhamento)
    extra = classificar_lancamentos_importados(COD_EMPRESA, conn)
    propagar_classificacao_detalhamento(conn, COD_EMPRESA)
    conn.commit()
    print(f"\nGravado: {len(resumos)} linhas de resumo e {len(registros)} lançamentos no "
          f"detalhamento. Classificação automática pegou mais {extra}.")
    conn.close()


if __name__ == "__main__":
    main()
