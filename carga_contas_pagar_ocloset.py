"""
Carga do Contas a Pagar d'O Closet (EMP013) — implantação.

    python3 carga_contas_pagar_ocloset.py ~/Downloads            # simula
    python3 carga_contas_pagar_ocloset.py ~/Downloads --aplicar  # grava

Lê os arquivos "contas-a-pagar-*.csv" exportados do sistema de origem e joga
cada linha em `pdv_titulos_pagar` como título manual (origem IMPORTADO).

Três decisões que valem registro:

1. **Arquivos repetidos são ignorados.** A exportação veio com o mesmo mês
   baixado duas vezes; a carga compara o conteúdo e lê cada mês uma vez só.

2. **A classificação sai do CONTATO, não da coluna "Categoria" do arquivo.**
   No arquivo a mesma despesa recorrente muda de categoria conforme o mês —
   o cartão do Sam's Club aparece como "Despesas administrativas" em
   setembro, "Compra de insumos" em outubro e "Marketing" em dezembro; a
   contabilidade da SECRAN aparece com três nomes diferentes. Classificar por
   ali levaria essa instabilidade para dentro do Matrix. O contato é estável:
   quem recebe diz o que é a despesa. A categoria do arquivo é só desempate
   quando o contato não é conhecido.

3. **Cada linha é um título de uma parcela só.** O arquivo já vem parcela a
   parcela ("(3/12)", "parcela 2"), com o vencimento de cada uma — reagrupar
   em um título parcelado seria reconstruir informação que já chegou pronta.

`chave_origem` (vencimento + contato + descrição + valor) faz a carga ser
repetível: rodar de novo não duplica.
"""

import csv
import glob
import hashlib
import os
import sys
import unicodedata
from collections import Counter
from datetime import datetime

from db import _nova_conexao
from psycopg2.extras import RealDictCursor

EMPRESA = "EMP013"


def _norm(texto):
    texto = unicodedata.normalize("NFKD", texto or "").encode("ascii", "ignore").decode()
    return " ".join(texto.upper().split())


# ─── OS TIPOS DE DESPESA E A CLASSIFICAÇÃO GERENCIAL ─────────────────────────
# (grupo, conta) de `contas_gerenciais` da EMP013 — a mesma classificação do
# Fluxo de Caixa do Matrix.

TIPOS = {
    "Compras de fornecedores":  (3, 1,  "Compras",     10),
    "Salários":                 (4, 2,  "Pessoal",     20),
    "Pró-labore (sócios)":      (4, 7,  "Pessoal",     20),
    "Impostos e Simples Nacional": (4, 1, "Impostos",   20),
    "Aluguel da loja":          (4, 9,  "Ocupação",    30),
    "Software e assinaturas":   (4, 4,  "Administrativas", 30),
    "Internet e telefonia":     (4, 4,  "Administrativas", 30),
    "Serviços contábeis":       (4, 4,  "Administrativas", 30),
    "Segurança e vigilância":   (4, 4,  "Administrativas", 30),
    "Outras despesas administrativas": (4, 4, "Administrativas", 30),
    "Marketing e divulgação":   (4, 13, "Comercial",   40),
    "Cartão de crédito":        (5, 11, "Financeiras", 50),
    "Empréstimos e parcelamentos": (5, 7, "Financeiras", 50),
    "Devolução de aporte (sócios)": (5, 7, "Financeiras", 50),
}

# Contato (normalizado) → tipo de despesa. É a classificação estável.
POR_CONTATO = {
    "ALTO GIRO": "Compras de fornecedores",
    "LIVE": "Compras de fornecedores",
    "LIVE ROUPAS ESPORTIVAS LTDA": "Compras de fornecedores",
    "NS VESTUARIO LTDA": "Compras de fornecedores",
    "RECCO RECCO & CIA LTDA": "Compras de fornecedores",
    "AUTHEN COMERCIO DE ROUPAS E ARTIGOS ESPORTIVOS LTDA": "Compras de fornecedores",

    "ANNE BEATRIZ": "Salários",
    "EDUARDA": "Salários",
    "HEVILYN": "Salários",
    "HEVYLIN": "Salários",
    "LETICIA PEDRA": "Salários",
    "LIVIA MARIA": "Salários",
    "MARIA CLARA": "Salários",
    "MARIA EDUARDA": "Salários",
    "MARIA EDUARDA DA SILVA REBOUCAS": "Salários",
    "MARIA TASSILA": "Salários",
    "TASSILA MARIA": "Salários",

    "LUCCA": "Pró-labore (sócios)",

    "SIMPLES NACIONAL": "Impostos e Simples Nacional",
    "SIMPLES NACIONAL (PARCELAMENTO)": "Impostos e Simples Nacional",
    "G P R LOCACOES DE IMOVEIS": "Aluguel da loja",
    "FACE PONTO": "Software e assinaturas",
    "SCALEX CHAT": "Software e assinaturas",
    "SISTEMA BLING": "Software e assinaturas",
    "CLARO": "Internet e telefonia",
    "SECRAN (CONTABILIDADE)": "Serviços contábeis",
    "VIGIA": "Segurança e vigilância",
    "DIEGO TRAFEGO": "Marketing e divulgação",
    "TATI - PROVADOR": "Marketing e divulgação",

    # Faturas de cartão da empresa: o contato é o emissor, não o fornecedor.
    "SAM S CLUB": "Cartão de crédito",
    "SICREDI": "Cartão de crédito",
    "BANCO DO BRASIL": "Cartão de crédito",
    "CARTAO": "Cartão de crédito",

    "BANCO ITAU": "Empréstimos e parcelamentos",
    "ITAU": "Empréstimos e parcelamentos",
    "EMPRESA": "Empréstimos e parcelamentos",
    "EMPRESTIMO BB": "Empréstimos e parcelamentos",
    "PORCINO E FILHOS": "Empréstimos e parcelamentos",

    # Sócios: mercadoria da LIVE paga no cartão pessoal e devolvida depois.
    "MICHELLE BARBOSA NASSER": "Devolução de aporte (sócios)",
    "LUCCA CAMINHA": "Devolução de aporte (sócios)",
    "ROSANNE BASTOS CAMINHA BARBOSA": "Devolução de aporte (sócios)",
    "PORCINO FERNANDES DA COSTA JUNIOR": "Devolução de aporte (sócios)",
    "ROSANNE CAMINHA": "Devolução de aporte (sócios)",
}

# ─── QUEM É FORNECEDOR DE VERDADE ────────────────────────────────────────────
# No arquivo, "Contato" é quem recebe — e ali há de tudo: fornecedor (LIVE,
# RECCO), funcionária ("MARIA CLARA"), sócio e rótulo genérico ("CARTÃO",
# "EMPRESA", "SIMPLES NACIONAL"). Só os desta lista viram cadastro de
# fornecedor; nos demais o título fica **sem fornecedor** e o contato entra na
# descrição, para a informação não se perder.

FORNECEDORES = {
    "ALTO GIRO", "AUTHEN COMERCIO DE ROUPAS E ARTIGOS ESPORTIVOS LTDA",
    "LIVE", "LIVE ROUPAS ESPORTIVAS LTDA", "NS VESTUARIO LTDA",
    "RECCO RECCO & CIA LTDA", "SAM S CLUB", "SICREDI", "BANCO DO BRASIL",
    "BANCO ITAU", "ITAU", "PORCINO E FILHOS", "CLARO", "FACE PONTO",
    "SCALEX CHAT", "SISTEMA BLING", "SECRAN (CONTABILIDADE)",
    "G P R LOCACOES DE IMOVEIS", "DIEGO TRAFEGO", "TATI - PROVADOR", "VIGIA",
}


def separar(contato, descricao):
    """
    Devolve (fornecedor, descrição). Contato que não é fornecedor não vira
    cadastro: ele passa a fazer parte da descrição.
    """
    if _norm(contato) in FORNECEDORES:
        return contato, descricao
    if contato and _norm(contato) not in _norm(descricao):
        return None, f"{contato} — {descricao}" if descricao else contato
    return None, descricao or contato


# Quando o contato não basta: Rosanne e Lucca recebem pró-labore E devolução de
# aporte. Quem separa é a descrição.
POR_DESCRICAO = (("PAGAMENTO MENSAL", "Pró-labore (sócios)"),)

# Última rede: a categoria do arquivo, para contato que ainda não conhecemos.
POR_CATEGORIA = {
    "Compras de fornecedores": "Compras de fornecedores",
    "Compra de insumos e matéria prima": "Outras despesas administrativas",
    "Salários": "Salários",
    "Pró-labore (sócios)": "Pró-labore (sócios)",
    "DAS / Simples Nacional": "Impostos e Simples Nacional",
    "Aluguel da loja": "Aluguel da loja",
    "Software e assinaturas": "Software e assinaturas",
    "Internet": "Internet e telefonia",
    "Serviços contábeis": "Serviços contábeis",
    "Honorários contábeis": "Serviços contábeis",
    "Despesas administrativas": "Outras despesas administrativas",
    "Marketing e divulgação": "Marketing e divulgação",
    "EMPRESTIMOS": "Empréstimos e parcelamentos",
    "Devolução de aporte": "Devolução de aporte (sócios)",
    "Devoluções de vendas": "Devolução de aporte (sócios)",
}


def classificar(contato, descricao, categoria):
    """Devolve (tipo, como_foi_decidido)."""
    contato = _norm(contato)
    for marca, tipo in POR_DESCRICAO:
        if marca in _norm(descricao) and contato in POR_CONTATO:
            return tipo, "descrição"
    if contato in POR_CONTATO:
        return POR_CONTATO[contato], "contato"
    if categoria in POR_CATEGORIA:
        return POR_CATEGORIA[categoria], "categoria"
    return "Outras despesas administrativas", "sem regra"


# ─── LEITURA ─────────────────────────────────────────────────────────────────

def _valor(texto):
    return round(float((texto or "0").replace(".", "").replace(",", ".")), 2)


def _data(texto):
    return datetime.strptime(texto.strip(), "%d/%m/%Y").date()


def ler_arquivos(pasta):
    """
    Lê os CSVs da pasta, ignorando arquivos repetidos (a exportação veio com o
    mesmo mês baixado duas vezes).
    """
    linhas, vistos, lidos, repetidos = [], set(), [], []
    for caminho in sorted(glob.glob(os.path.join(pasta, "contas-a-pagar-*.csv"))):
        with open(caminho, encoding="utf-8-sig") as arquivo:
            dados = [l for l in csv.DictReader(arquivo, delimiter=";")
                     if l.get("Vencimento") and l["Vencimento"] != "TOTAL"]
        assinatura = hashlib.md5(str(dados).encode()).hexdigest()
        if assinatura in vistos:
            repetidos.append(os.path.basename(caminho))
            continue
        vistos.add(assinatura)
        lidos.append((os.path.basename(caminho), len(dados)))
        for l in dados:
            vencimento = _data(l["Vencimento"])
            valor = _valor(l["Valor"])
            descricao = (l["Descrição"] or "").strip()
            contato = (l["Contato"] or "").strip()
            tipo, como = classificar(contato, descricao, (l["Categoria"] or "").strip())
            fornecedor, descricao_final = separar(contato, descricao)
            linhas.append({
                "vencimento": vencimento,
                "emissao": _data(l["Emissão"]) if l.get("Emissão") else None,
                "descricao": descricao,
                "contato": contato,
                "fornecedor": fornecedor,
                "descricao_final": descricao_final,
                "valor": valor,
                "categoria_arquivo": (l["Categoria"] or "").strip(),
                "tipo": tipo,
                "como": como,
                "chave": hashlib.md5(
                    f"{vencimento}|{_norm(contato)}|{_norm(descricao)}|{valor:.2f}".encode()
                ).hexdigest(),
            })
    return linhas, lidos, repetidos


# ─── GRAVAÇÃO ────────────────────────────────────────────────────────────────

def garantir_tipos(cur):
    """Cria os tipos de despesa já classificados no plano gerencial."""
    ids = {}
    for nome, (grupo, conta, agrupamento, ordem) in TIPOS.items():
        cur.execute("""
            SELECT id_pdv_despesa_tipo FROM pdv_despesas_tipos
            WHERE cod_empresa = %s AND lower(nome) = lower(%s)
        """, (EMPRESA, nome))
        linha = cur.fetchone()
        if linha:
            cur.execute("""
                UPDATE pdv_despesas_tipos
                   SET cod_grupo = %s, cod_conta = %s, grupo = %s, atualizado_em = now()
                 WHERE id_pdv_despesa_tipo = %s
            """, (grupo, conta, agrupamento, linha["id_pdv_despesa_tipo"]))
            ids[nome] = linha["id_pdv_despesa_tipo"]
            continue
        cur.execute("""
            INSERT INTO pdv_despesas_tipos
                (cod_empresa, nome, grupo, cod_grupo, cod_conta, dia_vencimento, ordem, ativo)
            VALUES (%s, %s, %s, %s, %s, NULL, %s, TRUE)
            RETURNING id_pdv_despesa_tipo
        """, (EMPRESA, nome, agrupamento, grupo, conta, ordem))
        ids[nome] = cur.fetchone()["id_pdv_despesa_tipo"]
    return ids


def garantir_fornecedores(cur, nomes):
    """Cada contato do arquivo vira fornecedor — é a quem se paga."""
    ids = {}
    for nome in sorted(nomes):
        if not nome:
            continue
        cur.execute("""
            SELECT id_pdv_fornecedor FROM pdv_fornecedores
            WHERE cod_empresa = %s AND lower(nome) = lower(%s)
        """, (EMPRESA, nome))
        linha = cur.fetchone()
        if linha:
            ids[nome] = linha["id_pdv_fornecedor"]
            continue
        cur.execute("""
            INSERT INTO pdv_fornecedores (cod_empresa, nome, ordem, ativo)
            VALUES (%s, %s, 10, TRUE) RETURNING id_pdv_fornecedor
        """, (EMPRESA, nome))
        ids[nome] = cur.fetchone()["id_pdv_fornecedor"]
    return ids


def gravar(cur, linhas):
    tipos = garantir_tipos(cur)
    fornecedores = garantir_fornecedores(cur, {l["fornecedor"] for l in linhas if l["fornecedor"]})

    gravados = repetidos = 0
    for l in linhas:
        cur.execute("""
            INSERT INTO pdv_titulos_pagar
                (cod_empresa, origem, id_pdv_despesa_tipo, id_pdv_fornecedor,
                 nome_fornecedor, numero_parcela, total_parcelas, valor,
                 data_vencimento, competencia, documento, descricao,
                 chave_origem, situacao)
            VALUES (%s, 'IMPORTADO', %s, %s, %s, 1, 1, %s, %s, %s, NULL, %s, %s, 'ABERTO')
            ON CONFLICT (cod_empresa, chave_origem) WHERE chave_origem IS NOT NULL DO NOTHING
            RETURNING id_pdv_titulo_pagar
        """, (EMPRESA, tipos[l["tipo"]], fornecedores.get(l["fornecedor"]),
              l["fornecedor"], l["valor"], l["vencimento"],
              l["vencimento"].replace(day=1), l["descricao_final"], l["chave"]))
        if cur.fetchone():
            gravados += 1
        else:
            repetidos += 1
    return gravados, repetidos


def main():
    pasta = os.path.expanduser(sys.argv[1] if len(sys.argv) > 1 else "~/Downloads")
    aplicar = "--aplicar" in sys.argv

    linhas, lidos, ignorados = ler_arquivos(pasta)
    if not linhas:
        print(f"Nenhum arquivo 'contas-a-pagar-*.csv' em {pasta}.")
        return

    print("Arquivos lidos:")
    for nome, quantidade in lidos:
        print(f"  {nome}: {quantidade} linhas")
    for nome in ignorados:
        print(f"  {nome}: IGNORADO (conteúdo idêntico a outro arquivo)")

    meses = Counter(l["vencimento"].strftime("%m/%Y") for l in linhas)
    print("\nPor mês de vencimento:")
    for mes in sorted(meses, key=lambda m: (m[3:], m[:2])):
        total = sum(l["valor"] for l in linhas if l["vencimento"].strftime("%m/%Y") == mes)
        print(f"  {mes}: {meses[mes]:>3} títulos   R$ {total:>12,.2f}")

    print("\nClassificação:")
    for tipo, quantidade in Counter(l["tipo"] for l in linhas).most_common():
        grupo, conta, _, _ = TIPOS[tipo]
        total = sum(l["valor"] for l in linhas if l["tipo"] == tipo)
        print(f"  {grupo}.{conta:<3} {tipo:<34} {quantidade:>3} títulos   R$ {total:>12,.2f}")

    com_fornecedor = sum(1 for l in linhas if l["fornecedor"])
    print(f"\nFornecedor identificado em {com_fornecedor} de {len(linhas)} títulos "
          f"({len({l['fornecedor'] for l in linhas if l['fornecedor']})} fornecedores); "
          "nos demais o contato foi para a descrição.")
    print("\nComo cada linha foi classificada:", dict(Counter(l["como"] for l in linhas)))
    print(f"TOTAL: {len(linhas)} títulos, R$ {sum(l['valor'] for l in linhas):,.2f}")

    if not aplicar:
        print("\n(simulação — rode com --aplicar para gravar)")
        return

    conexao = _nova_conexao()
    cur = conexao.cursor(cursor_factory=RealDictCursor)
    try:
        gravados, repetidos = gravar(cur, linhas)
        conexao.commit()
        print(f"\nGravados {gravados} títulos. {repetidos} já existiam (não duplicados).")
    except Exception:
        conexao.rollback()
        raise
    finally:
        cur.close()
        conexao.close()


if __name__ == "__main__":
    main()
