"""Ciclos de aferição e de drenagem: do dia parametrizado até a véspera do próximo ciclo."""
from calendar import monthrange
from datetime import date, timedelta

DIAS_SEMANA_AFERICAO = [
    (0, "Segunda-feira"),
    (1, "Terça-feira"),
    (2, "Quarta-feira"),
    (3, "Quinta-feira"),
    (4, "Sexta-feira"),
]

FREQUENCIAS_AFERICAO = [
    ("SEMANAL", "Toda semana"),
    ("DUAS_SEMANAS", "A cada 2 semanas"),
    ("TRES_SEMANAS", "A cada 3 semanas"),
    ("MENSAL", "Todo mês"),
]

SEMANAS_POR_FREQUENCIA = {"SEMANAL": 1, "DUAS_SEMANAS": 2, "TRES_SEMANAS": 3}


def _primeiro_dia_semana_do_mes(ano, mes, dia_semana):
    primeiro = date(ano, mes, 1)
    return primeiro + timedelta(days=(dia_semana - primeiro.weekday()) % 7)


def ciclos_que_comecam_no_mes(ano, mes, dia_semana, frequencia, data_referencia=None):
    """Lista de (inicio, fim) dos ciclos que começam no mês; o fim é a véspera do próximo.

    O ciclo pertence ao mês em que começa, mesmo terminando no mês seguinte.
    """
    if frequencia == "MENSAL":
        inicio = _primeiro_dia_semana_do_mes(ano, mes, dia_semana)
        ano_seg, mes_seg = (ano + 1, 1) if mes == 12 else (ano, mes + 1)
        proximo = _primeiro_dia_semana_do_mes(ano_seg, mes_seg, dia_semana)
        return [(inicio, proximo - timedelta(days=1))]

    semanas = SEMANAS_POR_FREQUENCIA[frequencia]
    passo = semanas * 7
    inicio = _primeiro_dia_semana_do_mes(ano, mes, dia_semana)

    # Em 2 ou 3 semanas, a referência é que diz qual semana é a do ciclo.
    if semanas > 1:
        inicio += timedelta(days=(data_referencia - inicio).days % passo)

    ultimo_dia = date(ano, mes, monthrange(ano, mes)[1])
    ciclos = []
    while inicio <= ultimo_dia:
        ciclos.append((inicio, inicio + timedelta(days=passo - 1)))
        inicio += timedelta(days=passo)
    return ciclos


# O mesmo controle por ciclo serve a mais de um evento (10/11/2026: Drenagem
# de Tanques ganhou o aviso das Aferições). Cada tipo diz de onde vêm os
# parâmetros, os eventos e quais postos são cobrados. Os nomes de tabela e
# coluna entram no SQL por f-string — vêm só daqui, nunca da requisição.
TIPOS_CICLO = {
    "afericao": {
        "tabela_parametros": "afericoes_parametros",
        "tabela": "afericoes",
        "coluna_data": "data_afericao",
        # Mesmo universo do aviso de Medições: filial ativa e com tanque.
        "filtro_tanque": "",
    },
    "drenagem": {
        "tabela_parametros": "drenagens_parametros",
        "tabela": "drenagens",
        "coluna_data": "data_drenagem",
        # Só posto com tanque de diesel drena.
        "filtro_tanque": """
              AND EXISTS (
                SELECT 1 FROM combustiveis c
                WHERE c.cod_empresa = ct.cod_empresa
                  AND c.cod_produto = ct.cod_produto
                  AND (c.descricao ILIKE '%%diesel%%' OR c.descricao ILIKE '%%s10%%'
                       OR c.descricao ILIKE '%%s500%%')
              )""",
    },
}


def parametros_ciclo(cur, cod_empresa, tipo):
    """Parâmetros da empresa para o tipo, ou None. O cursor precisa ser RealDictCursor."""
    cfg = TIPOS_CICLO[tipo]
    cur.execute(
        f"""
        SELECT dia_semana, frequencia, data_referencia
        FROM {cfg["tabela_parametros"]}
        WHERE cod_empresa = %s
        """,
        (cod_empresa,),
    )
    return cur.fetchone()


def parametros_afericao(cur, cod_empresa):
    return parametros_ciclo(cur, cod_empresa, "afericao")


def ciclo_de(data, parametros):
    """(inicio, fim) do ciclo que contém a data."""
    for ano, mes in ((data.year, data.month), ((data.year - 1, 12) if data.month == 1 else (data.year, data.month - 1))):
        for inicio, fim in ciclos_que_comecam_no_mes(
            ano, mes, parametros["dia_semana"], parametros["frequencia"], parametros["data_referencia"],
        ):
            if inicio <= data <= fim:
                return inicio, fim
    return None


def faltas_por_ciclo(cur, cod_empresa, ano, mes, filiais_consulta, parametros, hoje, tipo="afericao"):
    """Filiais sem o evento em cada ciclo já começado do mês, mais recente primeiro.

    Só cobra a partir do primeiro evento da empresa: antes dele não havia
    o processo, e todo o histórico viraria falta.
    """
    cfg = TIPOS_CICLO[tipo]
    tabela, coluna = cfg["tabela"], cfg["coluna_data"]

    cur.execute(
        f"SELECT MIN({coluna}) AS primeira FROM {tabela} WHERE cod_empresa = %s",
        (cod_empresa,),
    )
    primeira = cur.fetchone()["primeira"]
    if not primeira:
        return []

    ciclos = [
        (inicio, fim)
        for inicio, fim in ciclos_que_comecam_no_mes(
            ano, mes, parametros["dia_semana"], parametros["frequencia"],
            parametros["data_referencia"],
        )
        if inicio <= hoje and fim >= primeira
    ]
    if not ciclos:
        return []

    cur.execute(
        f"""
        SELECT f.cod_filial, f.nome_filial
        FROM filiais f
        WHERE f.cod_empresa = %s
          AND f.cod_filial = ANY(%s)
          AND f.ativo = TRUE
          AND EXISTS (
            SELECT 1
            FROM capacidade_tanques ct
            WHERE ct.cod_empresa = f.cod_empresa
              AND ct.cod_filial = f.cod_filial
              AND COALESCE(ct.capacidade_tanque, 0) > 0
              {cfg["filtro_tanque"]}
          )
        ORDER BY f.cod_filial
        """,
        (cod_empresa, filiais_consulta),
    )
    filiais = cur.fetchall() or []

    cur.execute(
        f"""
        SELECT DISTINCT cod_filial, {coluna} AS data_evento
        FROM {tabela}
        WHERE cod_empresa = %s
          AND {coluna} BETWEEN %s AND %s
        """,
        (cod_empresa, ciclos[0][0], ciclos[-1][1]),
    )
    eventos = cur.fetchall() or []

    faltas = []
    for inicio, fim in reversed(ciclos):
        feitas = {e["cod_filial"] for e in eventos if inicio <= e["data_evento"] <= fim}
        pendentes = [f for f in filiais if f["cod_filial"] not in feitas]
        if pendentes:
            faltas.append({"inicio": inicio, "fim": fim, "filiais": pendentes})
    return faltas
