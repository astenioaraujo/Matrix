"""Reclassificação de lançamentos do Fluxo de Caixa (após a importação analítica).

A classificação automática decide pela CONTA do WebPostos. A reclassificação
desce um nível: procura um texto na descrição do lançamento detalhado e tira
aquele lançamento da conta, sem mexer no resto dela.

Para cada lançamento que casa com uma regra, em `importacoes` (e no
detalhamento, para a conferência continuar fechando):

    <conta de origem> - ESTORNO P/ <DESTINO>   valor com o sinal invertido
    RECLASSIFICADO - <DESTINO>                 valor original

O lançamento original continua no detalhamento da conta, ao lado do estorno:
quem abrir a célula vê o que saiu e para onde foi. O lançamento novo é
classificado pela classificação automática (de-para) como qualquer outro —
sem de-para, ele aparece como não classificado na tela de importações.

Tudo o que é gerado leva `complemento = COMPLEMENTO_RECLASSIFICACAO`, e
aplicar de novo apaga o que foi gerado antes: rodar duas vezes não duplica.
"""

from collections import OrderedDict
from datetime import date
from decimal import Decimal

from psycopg2.extras import execute_values


COMPLEMENTO_RECLASSIFICACAO = "Reclassificação de lançamento"
PREFIXO_DESTINO = "RECLASSIFICADO - "
CODIGO_CONTA_DESTINO = "RECLASSIFICADO"


def _normalizar(texto):
    # Mesma normalização da classificação automática.
    from importa_web_postos import normalizar_texto
    return normalizar_texto(texto)


def historico_destino(destino):
    return f"{PREFIXO_DESTINO}{(destino or '').strip().upper()}"


def historico_estorno(historico_conta, destino):
    return f"{historico_conta} - ESTORNO P/ {(destino or '').strip().upper()}"


def carregar_regras(cur, cod_empresa, somente_ativas=True):
    cur.execute(f"""
        SELECT id_reclassificacao, texto, COALESCE(codigo_conta, ''), destino, ativo
        FROM reclassificacoes_lancamentos
        WHERE cod_empresa = %s
          AND TRIM(COALESCE(texto, '')) <> ''
          AND TRIM(COALESCE(destino, '')) <> ''
          {"AND ativo" if somente_ativas else ""}
        ORDER BY LENGTH(TRIM(texto)) DESC, LOWER(texto), id_reclassificacao
    """, (cod_empresa,))

    regras = []
    for id_regra, texto, codigo_conta, destino, ativo in cur.fetchall() or []:
        texto_norm = _normalizar(texto)
        if texto_norm:
            regras.append({
                "id": id_regra,
                "texto": texto.strip(),
                "texto_norm": texto_norm,
                "codigo_conta": codigo_conta.strip(),
                "destino": destino.strip().upper(),
                "ativo": ativo,
            })
    return regras


def _regra_do_lancamento(regras, codigo_conta, descricao_norm):
    # A primeira que casa vence; as regras vêm do texto mais longo (mais
    # específico) para o mais curto.
    for r in regras:
        if r["codigo_conta"] and r["codigo_conta"] != (codigo_conta or "").strip():
            continue
        if r["texto_norm"] in descricao_norm:
            return r
    return None


def desfazer_reclassificacoes(cur, cod_empresa):
    cur.execute("""
        DELETE FROM importacoes_detalhamento
        WHERE cod_empresa = %s AND complemento = %s
    """, (cod_empresa, COMPLEMENTO_RECLASSIFICACAO))
    cur.execute("""
        DELETE FROM importacoes
        WHERE cod_empresa = %s AND complemento = %s
    """, (cod_empresa, COMPLEMENTO_RECLASSIFICACAO))


def aplicar_reclassificacoes(conn, cod_empresa):
    """Aplica as regras ativas sobre o que está em `importacoes` (pendente).

    Não faz commit: roda na transação de quem chamou. Devolve o resumo por
    regra e a lista dos lançamentos reclassificados, para a tela mostrar.
    Depois dela, quem chama precisa rodar a classificação automática (para o
    lançamento novo) e propagar a classificação para o detalhamento.
    """
    cod_empresa = str(cod_empresa).strip()
    cur = conn.cursor()

    try:
        desfazer_reclassificacoes(cur, cod_empresa)

        regras = carregar_regras(cur, cod_empresa)
        resumo = {"regras": [], "lancamentos": [], "total_lancamentos": 0,
                  "total_valor": Decimal("0")}
        if not regras:
            return resumo

        cur.execute("""
            SELECT cod_filial, nome_filial, ano, mes, data, codigo_conta,
                   historico_conta, descricao, valor
            FROM importacoes_detalhamento
            WHERE cod_empresa = %s
            ORDER BY cod_filial, ano, mes, codigo_conta, data, descricao
        """, (cod_empresa,))
        detalhes = cur.fetchall() or []

        # Classificação já decidida da conta de origem: o estorno vai para a
        # mesma conta gerencial, senão não a zeraria.
        cur.execute("""
            SELECT cod_filial, ano, mes, historico, grupo, conta, descricao_conta
            FROM importacoes
            WHERE cod_empresa = %s
        """, (cod_empresa,))
        classif_origem = {
            (f, int(a), int(m), h): (g, c, d)
            for f, a, m, h, g, c, d in cur.fetchall() or []
        }

        estornos = OrderedDict()   # (filial, ano, mes, historico_conta, destino) -> dados
        destinos = OrderedDict()   # (filial, ano, mes, destino) -> dados
        por_regra = OrderedDict((r["id"], {"texto": r["texto"],
                                           "codigo_conta": r["codigo_conta"],
                                           "destino": r["destino"],
                                           "qtd": 0, "valor": Decimal("0")})
                                for r in regras)

        for (cod_filial, nome_filial, ano, mes, data_det, codigo_conta,
             historico_conta, descricao, valor) in detalhes:
            regra = _regra_do_lancamento(regras, codigo_conta, _normalizar(descricao))
            if not regra:
                continue

            ano, mes = int(ano), int(mes)
            valor = Decimal(str(valor or 0))
            destino = regra["destino"]
            item = {
                "cod_filial": cod_filial, "nome_filial": nome_filial,
                "ano": ano, "mes": mes, "data": data_det,
                "codigo_conta": codigo_conta, "historico_conta": historico_conta,
                "descricao": descricao, "valor": valor, "destino": destino,
            }

            chave_e = (cod_filial, ano, mes, historico_conta, destino)
            estornos.setdefault(chave_e, {"item": item, "linhas": []})["linhas"].append(item)

            chave_d = (cod_filial, ano, mes, destino)
            destinos.setdefault(chave_d, {"item": item, "linhas": []})["linhas"].append(item)

            por_regra[regra["id"]]["qtd"] += 1
            por_regra[regra["id"]]["valor"] += valor
            resumo["lancamentos"].append(item)

        if not resumo["lancamentos"]:
            resumo["regras"] = list(por_regra.values())
            return resumo

        contas = []
        linhas_det = []

        for (cod_filial, ano, mes, historico_conta, destino), dados in estornos.items():
            base = dados["item"]
            soma = sum((l["valor"] for l in dados["linhas"]), Decimal("0"))
            hist = historico_estorno(historico_conta, destino)
            g, c, d = classif_origem.get((cod_filial, ano, mes, historico_conta),
                                         (None, None, None))
            contas.append((cod_empresa, cod_filial, base["nome_filial"],
                           date(ano, mes, 1), ano, mes, hist, -soma, g, c, d,
                           COMPLEMENTO_RECLASSIFICACAO))
            for l in dados["linhas"]:
                linhas_det.append((cod_empresa, cod_filial, l["nome_filial"], ano, mes,
                                   l["data"], l["codigo_conta"], hist,
                                   f"ESTORNO P/ {destino}: {l['descricao']}",
                                   -l["valor"], COMPLEMENTO_RECLASSIFICACAO))

        for (cod_filial, ano, mes, destino), dados in destinos.items():
            base = dados["item"]
            soma = sum((l["valor"] for l in dados["linhas"]), Decimal("0"))
            hist = historico_destino(destino)
            # Sem grupo/conta: quem decide é a classificação automática (de-para).
            contas.append((cod_empresa, cod_filial, base["nome_filial"],
                           date(ano, mes, 1), ano, mes, hist, soma, None, None, None,
                           COMPLEMENTO_RECLASSIFICACAO))
            for l in dados["linhas"]:
                linhas_det.append((cod_empresa, cod_filial, l["nome_filial"], ano, mes,
                                   l["data"], CODIGO_CONTA_DESTINO, hist,
                                   f"{l['descricao']} (de {l['codigo_conta']})",
                                   l["valor"], COMPLEMENTO_RECLASSIFICACAO))

        execute_values(cur, """
            INSERT INTO importacoes (
                cod_empresa, cod_filial, nome_filial, data, ano, mes,
                historico, valor, grupo, conta, descricao_conta, complemento
            ) VALUES %s
        """, contas)

        execute_values(cur, """
            INSERT INTO importacoes_detalhamento (
                cod_empresa, cod_filial, nome_filial, ano, mes, data,
                codigo_conta, historico_conta, descricao, valor, complemento
            ) VALUES %s
        """, linhas_det)

        resumo["regras"] = list(por_regra.values())
        resumo["total_lancamentos"] = len(resumo["lancamentos"])
        resumo["total_valor"] = sum((l["valor"] for l in resumo["lancamentos"]),
                                    Decimal("0"))
        return resumo

    finally:
        cur.close()
