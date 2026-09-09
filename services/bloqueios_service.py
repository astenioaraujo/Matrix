"""Bloqueio de movimentações de Operações — regras compartilhadas.

Duas regras, e as duas valem tanto em Operações quanto na importação de
estoque em Financeiro → Saldos:

1. **Bloqueio automático**: data mais antiga que `DIAS_LIMITE_BLOQUEIO` dias
   conta como bloqueada, esteja ou não na tabela, e não pode ser
   desbloqueada. Movimento velho não se mexe mais.

2. **Bloqueio corrido**: para importar valores de estoque de uma data não
   basta ela estar bloqueada — tudo até ela precisa estar, senão um dia
   aberto lá atrás ainda muda o número importado (as compras e o trânsito
   da Consulta de Estoques vêm do dia anterior e de compras em aberto de
   qualquer dia passado). A varredura para em `DIAS_LIMITE_BLOQUEIO` dias,
   que é justamente onde o bloqueio automático começa — não precisa
   percorrer o histórico inteiro.
"""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# Quantos dias para trás o bloqueio ainda é manual. Antes disso, bloqueado
# automaticamente e sem desbloqueio.
DIAS_LIMITE_BLOQUEIO = 10


def hoje_br():
    return datetime.now(ZoneInfo("America/Sao_Paulo")).date()


def data_limite_bloqueio(hoje=None):
    """A partir desta data o bloqueio é manual; antes dela, automático."""
    return (hoje or hoje_br()) - timedelta(days=DIAS_LIMITE_BLOQUEIO)


def _normalizar(data_ref):
    if isinstance(data_ref, str):
        try:
            return date.fromisoformat(data_ref.strip())
        except ValueError:
            return None
    return data_ref


def datas_bloqueadas(cur, cod_empresa, datas):
    """Subconjunto de `datas` que está bloqueado (na tabela ou por antiguidade)."""
    datas = [d for d in (_normalizar(d) for d in datas) if d]
    if not datas:
        return set()

    cur.execute("""
        SELECT data
        FROM operacoes_bloqueios
        WHERE cod_empresa = %s
          AND data = ANY(%s)
    """, (cod_empresa, datas))

    # cursor pode ser comum ou RealDictCursor
    marcadas = {(linha["data"] if isinstance(linha, dict) else linha[0]) for linha in cur.fetchall()}
    limite = data_limite_bloqueio()

    return {d for d in datas if d in marcadas or d < limite}


def data_bloqueada(cur, cod_empresa, data_ref):
    """Data fechada em Operações → Bloquear Movimentações.

    Bloqueio é por empresa e por data: nenhuma filial grava medição, preço
    de compra, compra ou descarrego daquele dia. As telas continuam abrindo
    (só para consulta) — a trava de verdade fica nos pontos de gravação.
    """
    data_ref = _normalizar(data_ref)
    if not data_ref:
        return False
    return bool(datas_bloqueadas(cur, cod_empresa, [data_ref]))


def datas_bloqueio_pendentes(cur, cod_empresa, data_ref):
    """Datas ainda ABERTAS de `data_ref` para trás, dentro da janela manual.

    Vazio significa "tudo bloqueado até esta data" — a condição para importar
    os valores de estoque em Saldos.
    """
    data_ref = _normalizar(data_ref)
    if not data_ref:
        return []

    limite = data_limite_bloqueio()
    janela = []
    dia = data_ref
    while dia >= limite:
        janela.append(dia)
        dia -= timedelta(days=1)

    if not janela:
        return []

    bloqueadas = datas_bloqueadas(cur, cod_empresa, janela)
    return sorted(d for d in janela if d not in bloqueadas)


def msg_data_bloqueada(data_ref):
    data_ref = _normalizar(data_ref)
    if not data_ref:
        return "Data bloqueada para movimentações."

    if data_ref < data_limite_bloqueio():
        return (
            f"Movimentações de {data_ref.strftime('%d/%m/%Y')} estão bloqueadas: "
            f"passaram de {DIAS_LIMITE_BLOQUEIO} dias e não podem mais ser reabertas."
        )

    return f"Movimentações de {data_ref.strftime('%d/%m/%Y')} estão bloqueadas."


# ---------------------------------------------------------------------
# Liberação temporária de data para Informar Medições
# ---------------------------------------------------------------------
# Quantas horas uma liberação vale depois de ativada. Quem expira é o
# relógio, na leitura — não há rotina em background.
HORAS_LIBERACAO_MEDICOES = 4


def _ativado_em_local(ativado_em):
    """Converte o `ativado_em` da liberação para o horário de Brasília.

    A coluna é `timestamp without time zone` e é preenchida pelo NOW() do
    banco, que roda em UTC. Comparar esse valor direto com datetime.now(),
    que é local, daria três horas de folga: a liberação anunciada como de
    4 horas duraria 7, e a tela mostraria a expiração em UTC.
    """
    if ativado_em is None:
        return None
    return (ativado_em.replace(tzinfo=timezone.utc)
            .astimezone(ZoneInfo("America/Sao_Paulo")))


def liberacoes_medicao_vigentes(cur, cod_empresa):
    """Liberações ainda dentro do prazo, mais recentes primeiro.

    São **acumulativas**: cada linha abre uma data, e todas as que não
    expiraram valem ao mesmo tempo. Ler só a última faria uma liberação
    nova cancelar a anterior sem avisar.
    """
    cur.execute("""
        SELECT l.id_liberacao, l.data_liberada, l.ativado_em,
               u.nome AS ativado_por
        FROM operacoes_liberacao_temporaria l
        LEFT JOIN usuarios u ON u.id_usuario = l.id_usuario_ativou
        WHERE l.cod_empresa = %s
          AND l.revogado_em IS NULL
          AND l.ativado_em >= (NOW() AT TIME ZONE 'UTC') - %s::interval
        ORDER BY l.ativado_em DESC
    """, (cod_empresa, f"{HORAS_LIBERACAO_MEDICOES} hours"))

    agora = datetime.now(ZoneInfo("America/Sao_Paulo"))
    vigentes = []
    for linha in cur.fetchall():
        lib = linha if isinstance(linha, dict) else {
            "id_liberacao": linha[0],
            "data_liberada": linha[1],
            "ativado_em": linha[2],
            "ativado_por": linha[3],
        }
        expira_em = (_ativado_em_local(lib["ativado_em"])
                     + timedelta(hours=HORAS_LIBERACAO_MEDICOES))
        if agora >= expira_em:
            continue
        lib["expira_em"] = expira_em
        vigentes.append(lib)
    return vigentes


def datas_medicao_liberadas(cur, cod_empresa):
    """Conjunto de datas avulsas abertas por liberação temporária vigente."""
    return {lib["data_liberada"] for lib in liberacoes_medicao_vigentes(cur, cod_empresa)}
