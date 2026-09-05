"""Margem bruta lida das vendas diárias.

Antes o Financeiro lia a MB de `vendas_mb_sintetico`, que só existe porque
alguém importava o painel sintético. As vendas diárias contêm a mesma
informação em detalhe — a conferência mês a mês na tela "Consultar Vendas
Sintéticas" fechou em todos os 21 meses da EMP010 —, então a fonte passou a
ser uma só e a importação sintética deixou de ser necessária para o resultado
financeiro.

Onde as diárias ainda não cobrem o mês inteiro — empresas que começaram a
importar depois, ou um mês com dias faltando — a leitura cai no sintético
antigo, que continua sendo a única memória daquele período. Trocar a fonte sem
essa ressalva apagaria da tela o histórico da EMP011, EMP012 e EMP013.

ARLA fica de fora, como no painel: não é combustível, e o Financeiro passou a
ler a mesma base que a tela de Vendas Sintéticas mostra.

A regra de projeção é a mesma do painel: o último mês, enquanto incompleto, é
reprojetado pelo acumulado ÷ dias com venda × dias do mês. É o que a
importação sintética gravava, e é o que mantém as telas do Financeiro com o
mesmo número de antes.
"""
import calendar

from psycopg2.extras import RealDictCursor

from db import get_connection
from services.vendas_produtos import FILTRO_SQL_COMBUSTIVEL


def _cursor():
    return get_connection().cursor(cursor_factory=RealDictCursor)


def fator_projecao(cur, cod_empresa):
    """(ano, mes, fator) do último mês com venda. Fator 1.0 se o mês fechou."""
    cur.execute("""
        SELECT
            EXTRACT(YEAR FROM MAX(data))::int  AS ano,
            EXTRACT(MONTH FROM MAX(data))::int AS mes
        FROM vendas_diarias
        WHERE cod_empresa = %s
    """, (cod_empresa,))
    row = cur.fetchone()

    if not row or not row["ano"]:
        return None, None, 1.0

    ano = int(row["ano"])
    mes = int(row["mes"])

    cur.execute("""
        SELECT COUNT(DISTINCT data) AS dias
        FROM vendas_diarias
        WHERE cod_empresa = %s
          AND EXTRACT(YEAR FROM data)::int = %s
          AND EXTRACT(MONTH FROM data)::int = %s
    """, (cod_empresa, ano, mes))
    dia_base = int(cur.fetchone()["dias"] or 0)
    dias_mes = calendar.monthrange(ano, mes)[1]

    fator = (dias_mes / dia_base) if dia_base and dia_base < dias_mes else 1.0
    return ano, mes, fator


def meses_cobertos(cur, cod_empresa, ano):
    """Meses do ano em que as diárias têm o calendário completo.

    O último mês com venda entra mesmo incompleto: ele é o mês corrente, e é
    justamente o que a projeção resolve.
    """
    ano_proj, mes_proj, _ = fator_projecao(cur, cod_empresa)

    cur.execute("""
        SELECT
            EXTRACT(MONTH FROM data)::int AS mes,
            COUNT(DISTINCT data) AS dias
        FROM vendas_diarias
        WHERE cod_empresa = %s
          AND EXTRACT(YEAR FROM data)::int = %s
        GROUP BY 1
    """, (cod_empresa, int(ano)))

    cobertos = set()

    for r in cur.fetchall():
        mes = int(r["mes"])
        completo = int(r["dias"] or 0) >= calendar.monthrange(int(ano), mes)[1]

        if completo or (int(ano) == ano_proj and mes == mes_proj):
            cobertos.add(mes)

    return cobertos


def _sintetico(cur, cod_empresa, ano, meses, por, cod_filial=None, mes=None):
    """Fallback: MB do painel sintético importado, só dos meses pedidos."""
    if not meses:
        return []

    params = [cod_empresa, int(ano), list(meses)]
    filtro = ""

    if cod_filial is not None:
        filtro += " AND cod_filial = %s"
        params.append(int(cod_filial))

    if mes:
        filtro += " AND mes = %s"
        params.append(int(mes))

    campo = "cod_filial" if por == "filial" else "mes"

    cur.execute(f"""
        SELECT {campo} AS chave, COALESCE(SUM(margem_bruta), 0) AS mb
        FROM vendas_mb_sintetico
        WHERE cod_empresa = %s
          AND ano = %s
          AND mes = ANY(%s)
          {filtro}
        GROUP BY 1
    """, params)

    return cur.fetchall()


def mb_por_filial(cod_empresa, ano, mes=None, projetar=True):
    """{cod_filial: margem_bruta} do ano (ou do mês, se informado)."""
    cur = _cursor()

    try:
        ano_proj, mes_proj, fator = fator_projecao(cur, cod_empresa)

        params = [cod_empresa, int(ano)]
        where_mes = ""
        if mes:
            where_mes = "AND EXTRACT(MONTH FROM data)::int = %s"
            params.append(int(mes))

        cobertos = meses_cobertos(cur, cod_empresa, ano)
        pedidos = {int(mes)} if mes else set(range(1, 13))
        faltantes = sorted(pedidos - cobertos)

        cur.execute(f"""
            SELECT
                cod_filial,
                EXTRACT(MONTH FROM data)::int AS mes,
                COALESCE(SUM(margem_bruta), 0) AS mb
            FROM vendas_diarias
            WHERE cod_empresa = %s
              AND EXTRACT(YEAR FROM data)::int = %s
              {where_mes}
              {FILTRO_SQL_COMBUSTIVEL}
            GROUP BY cod_filial, 2
        """, params)
        linhas = cur.fetchall()

        antigas = _sintetico(cur, cod_empresa, ano, faltantes, "filial", mes=mes)
    finally:
        cur.close()

    resultado = {}

    for r in linhas:
        if int(r["mes"]) not in cobertos:
            continue

        v = float(r["mb"] or 0)
        if projetar and int(ano) == ano_proj and int(r["mes"]) == mes_proj:
            v *= fator
        resultado[int(r["cod_filial"])] = resultado.get(int(r["cod_filial"]), 0.0) + v

    for r in antigas:
        chave = int(r["chave"])
        resultado[chave] = resultado.get(chave, 0.0) + float(r["mb"] or 0)

    return resultado


def mb_por_mes(cod_empresa, ano, cod_filial=None, projetar=True):
    """{mes: margem_bruta} do ano. Sem filial, soma todos os postos."""
    cur = _cursor()

    try:
        ano_proj, mes_proj, fator = fator_projecao(cur, cod_empresa)

        params = [cod_empresa, int(ano)]
        where_filial = ""
        if cod_filial is not None:
            where_filial = "AND cod_filial = %s"
            params.append(int(cod_filial))

        cur.execute(f"""
            SELECT
                EXTRACT(MONTH FROM data)::int AS mes,
                COALESCE(SUM(margem_bruta), 0) AS mb
            FROM vendas_diarias
            WHERE cod_empresa = %s
              AND EXTRACT(YEAR FROM data)::int = %s
              {where_filial}
              {FILTRO_SQL_COMBUSTIVEL}
            GROUP BY 1
        """, params)
        linhas = cur.fetchall()

        cobertos = meses_cobertos(cur, cod_empresa, ano)
        faltantes = sorted(set(range(1, 13)) - cobertos)
        antigas = _sintetico(cur, cod_empresa, ano, faltantes, "mes",
                             cod_filial=cod_filial)
    finally:
        cur.close()

    resultado = {}

    for r in linhas:
        m = int(r["mes"])

        if m not in cobertos:
            continue

        v = float(r["mb"] or 0)
        if projetar and int(ano) == ano_proj and m == mes_proj:
            v *= fator
        resultado[m] = v

    for r in antigas:
        resultado[int(r["chave"])] = float(r["mb"] or 0)

    return resultado
