"""Empresas arquivadas.

Arquivar é feito direto no banco (`empresas.arquivada`, ver
`migrations/arquivar_empresas.sql`). Aqui fica só a leitura: quem está
arquivado, para o login não oferecer e para a sessão já aberta perder o acesso.
"""
import time

from db import get_connection

# A lista é consultada em toda requisição (before_request em app.py), e
# arquivar é raro e feito à mão no banco: um minuto de atraso não importa,
# uma consulta por requisição sim.
_TTL_SEGUNDOS = 60
_cache = {"em": 0.0, "codigos": frozenset()}


def codigos_arquivados():
    agora = time.monotonic()
    if agora - _cache["em"] < _TTL_SEGUNDOS:
        return _cache["codigos"]

    cur = get_connection().cursor()
    try:
        cur.execute("SELECT cod_empresa FROM empresas WHERE arquivada")
        codigos = frozenset(str(r[0]).strip() for r in cur.fetchall())
    finally:
        cur.close()

    _cache["em"] = agora
    _cache["codigos"] = codigos
    return codigos


def empresas_arquivadas():
    """Lista para a tela do superusuário, mais recente primeiro."""
    cur = get_connection().cursor()
    try:
        cur.execute("""
            SELECT e.cod_empresa, e.nome_fantasia, e.arquivada_em,
                   (SELECT COUNT(*) FROM filiais f
                     WHERE f.cod_empresa = e.cod_empresa) AS qtd_filiais
            FROM empresas e
            WHERE e.arquivada
            ORDER BY e.arquivada_em DESC NULLS LAST, e.cod_empresa
        """)
        return [
            {
                "cod_empresa": str(r[0]).strip(),
                "nome_fantasia": r[1],
                "arquivada_em": r[2],
                "qtd_filiais": r[3],
            }
            for r in cur.fetchall()
        ]
    finally:
        cur.close()
