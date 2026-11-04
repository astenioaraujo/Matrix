"""Ciclos de aferição: do dia parametrizado até a véspera do próximo ciclo."""
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


def parametros_afericao(cur, cod_empresa):
    """Parâmetros da empresa, ou None. O cursor precisa ser RealDictCursor."""
    cur.execute(
        """
        SELECT dia_semana, frequencia, data_referencia
        FROM afericoes_parametros
        WHERE cod_empresa = %s
        """,
        (cod_empresa,),
    )
    return cur.fetchone()


def faltas_por_ciclo(cur, cod_empresa, ano, mes, filiais_consulta, parametros, hoje):
    """Filiais sem aferição em cada ciclo já começado do mês, mais recente primeiro.

    Só cobra a partir da primeira aferição da empresa: antes dela não havia
    processo de aferição, e todo o histórico viraria falta.
    """
    cur.execute(
        "SELECT MIN(data_afericao) AS primeira FROM afericoes WHERE cod_empresa = %s",
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

    # Mesmo universo do aviso de Medições: filial ativa e com tanque.
    cur.execute(
        """
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
          )
        ORDER BY f.cod_filial
        """,
        (cod_empresa, filiais_consulta),
    )
    filiais = cur.fetchall() or []

    cur.execute(
        """
        SELECT DISTINCT cod_filial, data_afericao
        FROM afericoes
        WHERE cod_empresa = %s
          AND data_afericao BETWEEN %s AND %s
        """,
        (cod_empresa, ciclos[0][0], ciclos[-1][1]),
    )
    afericoes = cur.fetchall() or []

    faltas = []
    for inicio, fim in reversed(ciclos):
        feitas = {a["cod_filial"] for a in afericoes if inicio <= a["data_afericao"] <= fim}
        pendentes = [f for f in filiais if f["cod_filial"] not in feitas]
        if pendentes:
            faltas.append({"inicio": inicio, "fim": fim, "filiais": pendentes})
    return faltas
