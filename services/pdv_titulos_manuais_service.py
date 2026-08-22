"""
Títulos manuais e Orçamento de Despesas — PDV Matrix.

Conta de luz, água, telefone, aluguel: a despesa que não passa por nota de
entrada. Ela gera a MESMA obrigação que a compra gera, então mora na MESMA
tabela (`pdv_titulos_pagar`), distinguida pela coluna `origem` — ver o
cabeçalho de `migrations/criar_pdv_titulos_manuais.sql`.

O orçamento, esse sim, é outra tabela: previsão não é obrigação. O título só
nasce quando alguém confirma o mês.
"""

from datetime import date, timedelta

from services.pdv_entrada_service import parcelar

ORIGEM_MANUAL = "MANUAL"
ORIGEM_ORCAMENTO = "ORCAMENTO"

# As origens que a tela de títulos manuais lista e deixa excluir. A de nota de
# entrada fica de fora de propósito: aquele título pertence ao documento que o
# gerou, e apagá-lo aqui deixaria a nota sem a obrigação que ela criou.
ORIGEM_IMPORTADO = "IMPORTADO"   # carga de implantação (ver carga_contas_pagar_ocloset.py)
ORIGENS_MANUAIS = (ORIGEM_MANUAL, ORIGEM_ORCAMENTO, ORIGEM_IMPORTADO)

MESES = ("Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
         "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro")


def _primeiro_dia(ano, mes):
    return date(int(ano), int(mes), 1)


def competencia_de(ano, mes):
    """O mês a que a despesa se refere, guardado como o dia 1."""
    return _primeiro_dia(ano, mes)


def vencimento_sugerido(ano, mes, dia):
    """
    Dia de vencimento do tipo aplicado ao mês, sem estourar mês curto
    (dia 31 em fevereiro cai no último dia).
    """
    dia = int(dia or 10)
    primeiro = _primeiro_dia(ano, mes)
    proximo = _primeiro_dia(ano + (mes == 12), 1 if mes == 12 else mes + 1)
    ultimo = (proximo - timedelta(days=1)).day
    return primeiro.replace(day=min(max(dia, 1), ultimo))


# ─── TIPOS DE DESPESA ────────────────────────────────────────────────────────

def tipos_despesa(cur, cod_empresa, apenas_ativos=True):
    filtro = " AND t.ativo" if apenas_ativos else ""
    cur.execute(f"""
        SELECT t.*, f.nome AS nome_fornecedor
        FROM pdv_despesas_tipos t
        LEFT JOIN pdv_fornecedores f ON f.id_pdv_fornecedor = t.id_pdv_fornecedor
        WHERE t.cod_empresa = %s{filtro}
        ORDER BY t.ordem, t.nome
    """, (cod_empresa,))
    return [dict(r) for r in cur.fetchall()]


# ─── TÍTULOS MANUAIS ─────────────────────────────────────────────────────────

def listar_titulos(cur, cod_empresa, ano=None, mes=None, situacao="TODOS"):
    """
    Os títulos manuais de uma competência. O filtro é pela competência (o mês
    a que a despesa se refere), não pelo vencimento: a luz de agosto vence em
    setembro e continua sendo despesa de agosto.
    """
    condicoes = ["t.cod_empresa = %s", "t.origem = ANY(%s)"]
    parametros = [cod_empresa, list(ORIGENS_MANUAIS)]

    if ano and mes:
        condicoes.append("t.competencia = %s")
        parametros.append(competencia_de(ano, mes))
    elif ano:
        condicoes.append("EXTRACT(YEAR FROM t.competencia) = %s")
        parametros.append(int(ano))

    if situacao and situacao != "TODOS":
        condicoes.append("t.situacao = %s")
        parametros.append(situacao)

    cur.execute(f"""
        SELECT t.*, d.nome AS nome_tipo, d.grupo,
               d.cod_grupo, d.cod_conta, cg.descricao AS nome_conta
        FROM pdv_titulos_pagar t
        LEFT JOIN pdv_despesas_tipos d ON d.id_pdv_despesa_tipo = t.id_pdv_despesa_tipo
        LEFT JOIN contas_gerenciais cg ON cg.cod_empresa = t.cod_empresa
                                      AND cg.cod_grupo = d.cod_grupo
                                      AND cg.cod_conta = d.cod_conta
        WHERE {' AND '.join(condicoes)}
        ORDER BY t.data_vencimento, t.id_pdv_titulo_pagar
    """, parametros)
    return [dict(r) for r in cur.fetchall()]


def incluir_titulo(cur, cod_empresa, dados):
    """
    Inclui um título manual — em uma ou várias parcelas.

    `dados`: id_pdv_despesa_tipo, descricao, id_pdv_fornecedor, documento,
    valor, data_vencimento, qtd_parcelas, ano, mes, observacao.

    Fornecedor e descrição são campos separados: nem toda obrigação tem
    fornecedor (folha, imposto, empréstimo), e o que se paga precisa estar
    escrito de qualquer jeito.

    Devolve a quantidade de títulos gravados. Roda na transação de quem chamou.
    """
    id_tipo = dados.get("id_pdv_despesa_tipo") or None
    valor = round(float(dados.get("valor") or 0), 2)
    if valor <= 0:
        raise ValueError("Informe o valor do título.")

    vencimento = dados.get("data_vencimento")
    if not vencimento:
        raise ValueError("Informe a data de vencimento.")
    if isinstance(vencimento, str):
        vencimento = date.fromisoformat(vencimento.strip())

    descricao = (dados.get("descricao") or "").strip()
    if not id_tipo and not descricao:
        raise ValueError("Escolha o tipo de despesa ou descreva o título.")

    ano = int(dados.get("ano") or vencimento.year)
    mes = int(dados.get("mes") or vencimento.month)
    quantidade = max(int(dados.get("qtd_parcelas") or 1), 1)

    if id_tipo:
        cur.execute("""
            SELECT nome, id_pdv_fornecedor FROM pdv_despesas_tipos
            WHERE id_pdv_despesa_tipo = %s AND cod_empresa = %s
        """, (id_tipo, cod_empresa))
        tipo = cur.fetchone()
        if not tipo:
            raise ValueError("Tipo de despesa não encontrado.")
        descricao = descricao or tipo["nome"]

    id_fornecedor = dados.get("id_pdv_fornecedor") or None
    nome_fornecedor = None
    if id_fornecedor:
        cur.execute("""
            SELECT nome FROM pdv_fornecedores
            WHERE id_pdv_fornecedor = %s AND cod_empresa = %s
        """, (id_fornecedor, cod_empresa))
        fornecedor = cur.fetchone()
        if not fornecedor:
            raise ValueError("Fornecedor não encontrado.")
        nome_fornecedor = fornecedor["nome"]

    parcelas = parcelar(valor, quantidade, vencimento)
    for parcela in parcelas:
        cur.execute("""
            INSERT INTO pdv_titulos_pagar
                (cod_empresa, origem, id_pdv_despesa_tipo, id_pdv_fornecedor,
                 nome_fornecedor, numero_parcela, total_parcelas, valor,
                 data_vencimento, documento, competencia, descricao, situacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'ABERTO')
        """, (cod_empresa, dados.get("origem") or ORIGEM_MANUAL, id_tipo,
              id_fornecedor, nome_fornecedor,
              parcela["numero"], quantidade, parcela["valor"],
              parcela["data_vencimento"], (dados.get("documento") or "").strip() or None,
              competencia_de(ano, mes), descricao))

    return len(parcelas)


def excluir_titulo(cur, cod_empresa, id_titulo):
    """
    Só o título manual e só enquanto nada foi pago nele: título com baixa já
    virou saída de caixa, e apagá-lo deixaria o lançamento órfão.
    """
    cur.execute("""
        SELECT origem, valor_baixado, situacao FROM pdv_titulos_pagar
        WHERE id_pdv_titulo_pagar = %s AND cod_empresa = %s
    """, (id_titulo, cod_empresa))
    titulo = cur.fetchone()
    if not titulo:
        raise ValueError("Título não encontrado.")
    if titulo["origem"] not in ORIGENS_MANUAIS:
        raise ValueError("Este título veio de uma nota de entrada. "
                         "Ele pertence àquele documento e não se exclui por aqui.")
    if float(titulo["valor_baixado"] or 0) > 0 or titulo["situacao"] == "BAIXADO":
        raise ValueError("Este título já tem pagamento lançado e não pode ser excluído.")

    cur.execute("""
        DELETE FROM pdv_titulos_pagar
        WHERE id_pdv_titulo_pagar = %s AND cod_empresa = %s
    """, (id_titulo, cod_empresa))


# ─── ORÇAMENTO ───────────────────────────────────────────────────────────────

def orcamento_do_ano(cur, cod_empresa, ano):
    """
    Grade tipo × 12 meses: previsto, o que já virou título e o que já foi pago.

    Nada aqui é somado e gravado — previsto e realizado são recalculados a
    cada abertura, como qualquer total do sistema.
    """
    tipos = tipos_despesa(cur, cod_empresa)

    cur.execute("""
        SELECT id_pdv_orcamento, id_pdv_despesa_tipo, mes, valor_previsto
        FROM pdv_orcamento_despesas
        WHERE cod_empresa = %s AND ano = %s
    """, (cod_empresa, int(ano)))
    previsto = {(r["id_pdv_despesa_tipo"], r["mes"]): dict(r) for r in cur.fetchall()}

    cur.execute("""
        SELECT id_pdv_despesa_tipo,
               EXTRACT(MONTH FROM competencia)::int AS mes,
               COUNT(*) AS qtd,
               SUM(valor) AS valor,
               SUM(valor_baixado) AS pago
        FROM pdv_titulos_pagar
        WHERE cod_empresa = %s
          AND competencia IS NOT NULL
          AND EXTRACT(YEAR FROM competencia) = %s
          AND id_pdv_despesa_tipo IS NOT NULL
        GROUP BY id_pdv_despesa_tipo, EXTRACT(MONTH FROM competencia)
    """, (cod_empresa, int(ano)))
    realizado = {(r["id_pdv_despesa_tipo"], r["mes"]): dict(r) for r in cur.fetchall()}

    linhas = []
    for tipo in tipos:
        meses = []
        for mes in range(1, 13):
            chave = (tipo["id_pdv_despesa_tipo"], mes)
            p = previsto.get(chave)
            r = realizado.get(chave)
            meses.append({
                "mes": mes,
                "id_pdv_orcamento": p["id_pdv_orcamento"] if p else None,
                "previsto": float(p["valor_previsto"]) if p else 0.0,
                "titulos": int(r["qtd"]) if r else 0,
                "lancado": float(r["valor"] or 0) if r else 0.0,
                "pago": float(r["pago"] or 0) if r else 0.0,
            })
        linhas.append({"tipo": tipo, "meses": meses,
                       "total_previsto": sum(m["previsto"] for m in meses),
                       "total_lancado": sum(m["lancado"] for m in meses)})

    totais = [{"mes": mes,
               "previsto": sum(l["meses"][mes - 1]["previsto"] for l in linhas),
               "lancado": sum(l["meses"][mes - 1]["lancado"] for l in linhas)}
              for mes in range(1, 13)]

    return {"linhas": linhas, "totais": totais,
            "total_previsto": sum(t["previsto"] for t in totais),
            "total_lancado": sum(t["lancado"] for t in totais)}


def salvar_previsao(cur, cod_empresa, id_tipo, ano, mes, valor):
    """Grava (ou apaga, quando zerado) a previsão de um tipo num mês."""
    valor = round(float(valor or 0), 2)
    if valor <= 0:
        cur.execute("""
            DELETE FROM pdv_orcamento_despesas
            WHERE cod_empresa = %s AND id_pdv_despesa_tipo = %s AND ano = %s AND mes = %s
              AND NOT EXISTS (SELECT 1 FROM pdv_titulos_pagar t
                              WHERE t.id_pdv_orcamento = pdv_orcamento_despesas.id_pdv_orcamento)
        """, (cod_empresa, id_tipo, int(ano), int(mes)))
        return

    cur.execute("""
        INSERT INTO pdv_orcamento_despesas
            (cod_empresa, id_pdv_despesa_tipo, ano, mes, valor_previsto)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (cod_empresa, id_pdv_despesa_tipo, ano, mes)
        DO UPDATE SET valor_previsto = EXCLUDED.valor_previsto, atualizado_em = now()
    """, (cod_empresa, id_tipo, int(ano), int(mes), valor))


def replicar_previsao(cur, cod_empresa, id_tipo, ano, mes_inicial, valor):
    """
    "De agosto em diante": repete o valor do mês escolhido até dezembro. É o
    caso comum — a conta de luz prevista uma vez vale para o resto do ano.
    """
    for mes in range(int(mes_inicial), 13):
        salvar_previsao(cur, cod_empresa, id_tipo, ano, mes, valor)
    return 13 - int(mes_inicial)


def gerar_titulos_do_mes(cur, cod_empresa, ano, mes):
    """
    Transforma a previsão do mês em obrigação de verdade.

    Só o que ainda não gerou título entra — rodar de novo não duplica. O
    valor previsto é ponto de partida: a conta chega com outro valor e o
    título se corrige na tela de títulos manuais.
    """
    cur.execute("""
        SELECT o.id_pdv_orcamento, o.id_pdv_despesa_tipo, o.valor_previsto,
               d.nome, d.dia_vencimento, d.id_pdv_fornecedor,
               f.nome AS nome_fornecedor
        FROM pdv_orcamento_despesas o
        JOIN pdv_despesas_tipos d ON d.id_pdv_despesa_tipo = o.id_pdv_despesa_tipo
        LEFT JOIN pdv_fornecedores f ON f.id_pdv_fornecedor = d.id_pdv_fornecedor
        WHERE o.cod_empresa = %s AND o.ano = %s AND o.mes = %s
          AND o.valor_previsto > 0 AND d.ativo
          AND NOT EXISTS (SELECT 1 FROM pdv_titulos_pagar t
                          WHERE t.id_pdv_orcamento = o.id_pdv_orcamento)
        ORDER BY d.ordem, d.nome
    """, (cod_empresa, int(ano), int(mes)))
    previsoes = [dict(r) for r in cur.fetchall()]

    for p in previsoes:
        cur.execute("""
            INSERT INTO pdv_titulos_pagar
                (cod_empresa, origem, id_pdv_despesa_tipo, id_pdv_orcamento,
                 id_pdv_fornecedor, nome_fornecedor, numero_parcela, total_parcelas,
                 valor, data_vencimento, competencia, descricao, situacao)
            VALUES (%s, %s, %s, %s, %s, %s, 1, 1, %s, %s, %s, %s, 'ABERTO')
        """, (cod_empresa, ORIGEM_ORCAMENTO, p["id_pdv_despesa_tipo"],
              p["id_pdv_orcamento"], p["id_pdv_fornecedor"], p["nome_fornecedor"],
              p["valor_previsto"],
              vencimento_sugerido(ano, mes, p["dia_vencimento"]),
              competencia_de(ano, mes), p["nome"]))

    return len(previsoes)


# ─── FLUXO DE CAIXA DAS OBRIGAÇÕES ───────────────────────────────────────────

import re

# O que faz duas linhas serem "o mesmo pagamento": o sufixo de parcela e os
# números de nota mudam de mês para mês, o compromisso não. "FOLHA (2/12)" e
# "FOLHA (3/12)" são a mesma folha; "Compra OC 9 — parcela 2" e "parcela 5",
# a mesma compra.
_RE_PARCELA = re.compile(r"\(\s*\d+\s*/\s*\d+\s*\)|—?\s*parcela\s+\d+", re.IGNORECASE)
_RE_NUMEROS = re.compile(r"\d[\d.,]{3,}")


def _identidade(texto):
    texto = _RE_PARCELA.sub("", texto or "")
    texto = _RE_NUMEROS.sub("", texto)
    return " ".join(texto.replace("-", " ").split()).upper()[:80] or "(sem descrição)"


def fluxo_caixa_pagar(cur, cod_empresa, ano, meses_visiveis=12, mes_inicial=1):
    """
    O que a loja tem a pagar, mês a mês: uma linha por compromisso, uma coluna
    por mês de vencimento.

    Os títulos são agrupados pelo compromisso (fornecedor + descrição sem o
    sufixo de parcela) — é o que transforma doze parcelas de "FOLHA" numa
    linha só, que é como se lê um fluxo de caixa. **Nada é gravado**: a grade
    é recalculada a cada abertura, a partir dos títulos.
    """
    cur.execute("""
        SELECT t.id_pdv_titulo_pagar, t.nome_fornecedor, t.descricao, t.valor,
               t.valor_baixado, t.situacao,
               EXTRACT(MONTH FROM t.data_vencimento)::int AS mes,
               d.nome AS nome_tipo, d.cod_grupo, d.cod_conta,
               cg.descricao AS nome_conta, g.abreviatura AS nome_grupo
        FROM pdv_titulos_pagar t
        LEFT JOIN pdv_despesas_tipos d ON d.id_pdv_despesa_tipo = t.id_pdv_despesa_tipo
        LEFT JOIN contas_gerenciais cg ON cg.cod_empresa = t.cod_empresa
                                      AND cg.cod_grupo = d.cod_grupo
                                      AND cg.cod_conta = d.cod_conta
        LEFT JOIN grupos_gerenciais g ON g.cod_grupo = d.cod_grupo
        WHERE t.cod_empresa = %s
          AND EXTRACT(YEAR FROM t.data_vencimento) = %s
        ORDER BY t.data_vencimento
    """, (cod_empresa, int(ano)))
    titulos = [dict(r) for r in cur.fetchall()]

    meses = list(range(int(mes_inicial), min(int(mes_inicial) + int(meses_visiveis), 13)))

    linhas = {}
    for t in titulos:
        if t["mes"] not in meses:
            continue
        chave = (t["cod_grupo"], t["cod_conta"], t["nome_fornecedor"] or "",
                 _identidade(t["descricao"]))
        linha = linhas.setdefault(chave, {
            "cod_grupo": t["cod_grupo"],
            "cod_conta": t["cod_conta"],
            "nome_grupo": t["nome_grupo"],
            "nome_conta": t["nome_conta"],
            "nome_tipo": t["nome_tipo"],
            "fornecedor": t["nome_fornecedor"],
            "descricao": _identidade(t["descricao"]),
            "valores": {m: 0.0 for m in meses},
            "titulos": {m: 0 for m in meses},
            "total": 0.0,
        })
        linha["valores"][t["mes"]] += float(t["valor"] or 0)
        linha["titulos"][t["mes"]] += 1
        linha["total"] += float(t["valor"] or 0)

    ordenadas = sorted(
        linhas.values(),
        key=lambda l: (l["cod_grupo"] or 99, l["cod_conta"] or 99, -l["total"]),
    )

    # Um bloco por conta gerencial: é a leitura que o Fluxo de Caixa do Matrix
    # já usa, e é o que permite comparar as duas telas.
    blocos = []
    for linha in ordenadas:
        chave = (linha["cod_grupo"], linha["cod_conta"])
        if not blocos or blocos[-1]["chave"] != chave:
            blocos.append({
                "chave": chave,
                "nome_grupo": linha["nome_grupo"] or "Sem classificação",
                "nome_conta": linha["nome_conta"] or (linha["nome_tipo"] or "—"),
                "linhas": [],
                "valores": {m: 0.0 for m in meses},
                "total": 0.0,
            })
        bloco = blocos[-1]
        bloco["linhas"].append(linha)
        for m in meses:
            bloco["valores"][m] += linha["valores"][m]
        bloco["total"] += linha["total"]

    totais = {m: sum(b["valores"][m] for b in blocos) for m in meses}
    return {"meses": meses, "blocos": blocos, "totais": totais,
            "total": sum(totais.values())}


def totais_por_grupo_conta(cur, cod_empresa, ano, mes="TODOS", situacao="TODOS"):
    """
    Quanto se deve em cada conta gerencial — só os totais, sem a lista de
    títulos. É a leitura de cima do Contas a Pagar: onde o dinheiro vai.

    Uma conta por linha, um subtotal por grupo, e o total geral. Tudo somado
    na consulta; nada disso é coluna em lugar nenhum. Título sem tipo de
    despesa (o que vem de nota de entrada) cai em "Sem classificação" — some
    do total seria pior do que aparecer sem nome.
    """
    condicoes = ["t.cod_empresa = %s", "EXTRACT(YEAR FROM t.data_vencimento) = %s"]
    parametros = [cod_empresa, int(ano)]
    if str(mes).upper() != "TODOS":
        condicoes.append("EXTRACT(MONTH FROM t.data_vencimento) = %s")
        parametros.append(int(mes))
    if situacao and situacao != "TODOS":
        condicoes.append("t.situacao = %s")
        parametros.append(situacao)

    cur.execute(f"""
        SELECT d.cod_grupo, d.cod_conta,
               g.abreviatura AS nome_grupo, g.descricao AS descricao_grupo,
               cg.descricao AS nome_conta,
               min(d.nome) AS nome_tipo,
               count(*) AS titulos,
               sum(t.valor) AS valor,
               sum(t.valor_baixado) AS baixado
        FROM pdv_titulos_pagar t
        LEFT JOIN pdv_despesas_tipos d ON d.id_pdv_despesa_tipo = t.id_pdv_despesa_tipo
        LEFT JOIN contas_gerenciais cg ON cg.cod_empresa = t.cod_empresa
                                      AND cg.cod_grupo = d.cod_grupo
                                      AND cg.cod_conta = d.cod_conta
        LEFT JOIN grupos_gerenciais g ON g.cod_grupo = d.cod_grupo
        WHERE {' AND '.join(condicoes)}
        GROUP BY d.cod_grupo, d.cod_conta, g.abreviatura, g.descricao, cg.descricao
        ORDER BY d.cod_grupo NULLS LAST, d.cod_conta NULLS LAST
    """, parametros)

    grupos = []
    for r in cur.fetchall():
        valor = float(r["valor"] or 0)
        baixado = float(r["baixado"] or 0)
        conta = {
            "cod_conta": r["cod_conta"],
            "nome": r["nome_conta"] or r["nome_tipo"] or "Sem classificação",
            "titulos": int(r["titulos"]),
            "valor": valor,
            "baixado": baixado,
            "aberto": valor - baixado,
        }
        if not grupos or grupos[-1]["cod_grupo"] != r["cod_grupo"]:
            grupos.append({
                "cod_grupo": r["cod_grupo"],
                "abreviatura": r["nome_grupo"] or "—",
                "descricao": r["descricao_grupo"] or "SEM CLASSIFICAÇÃO",
                "contas": [], "titulos": 0, "valor": 0.0, "baixado": 0.0, "aberto": 0.0,
            })
        grupo = grupos[-1]
        grupo["contas"].append(conta)
        for campo in ("titulos", "valor", "baixado", "aberto"):
            grupo[campo] += conta[campo]

    return {
        "grupos": grupos,
        "titulos": sum(g["titulos"] for g in grupos),
        "valor": sum(g["valor"] for g in grupos),
        "baixado": sum(g["baixado"] for g in grupos),
        "aberto": sum(g["aberto"] for g in grupos),
    }
