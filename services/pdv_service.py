"""
Parâmetros do PDV Matrix.

O PDV é um módulo novo, que mexe em venda e em caixa. A trava de proteção é
por empresa: quem não estiver marcado em `pdv_parametros.opera_pdv` não
enxerga o módulo — **nem o superusuário** — e as rotas do PDV recusam o
acesso, não só o menu.

Empresa sem linha na tabela não opera com PDV. É o padrão: para ligar, é
preciso ir em Configurações → Configuração de PDV e marcar.
"""

from flask import g

from db import get_connection


def empresa_opera_pdv(cod_empresa):
    """
    A empresa está liberada para o PDV?

    Cacheado em `g` porque o menu principal e o `_checar_acesso` de cada tela
    do PDV perguntam isto na mesma requisição.
    """
    cod_empresa = str(cod_empresa or "").strip()
    if not cod_empresa:
        return False

    cache = getattr(g, "_pdv_opera", None)
    if isinstance(cache, dict) and cod_empresa in cache:
        return cache[cod_empresa]

    opera = False
    try:
        cur = get_connection().cursor()
        try:
            cur.execute(
                "SELECT opera_pdv FROM pdv_parametros WHERE cod_empresa = %s",
                (cod_empresa,),
            )
            linha = cur.fetchone()
            opera = bool(linha[0]) if linha else False
        finally:
            cur.close()
    except Exception:
        # tabela ainda não criada (migration não rodada): o seguro é não
        # mostrar o módulo
        opera = False

    if not isinstance(cache, dict):
        cache = {}
        g._pdv_opera = cache
    cache[cod_empresa] = opera
    return opera


def definir_opera_pdv(cod_empresa, opera):
    """Liga/desliga o PDV para uma empresa. Uma linha por empresa."""
    cod_empresa = str(cod_empresa or "").strip()
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO pdv_parametros (cod_empresa, opera_pdv)
            VALUES (%s, %s)
            ON CONFLICT (cod_empresa)
            DO UPDATE SET opera_pdv = EXCLUDED.opera_pdv, atualizado_em = now()
        """, (cod_empresa, bool(opera)))
        conn.commit()
    finally:
        cur.close()

    cache = getattr(g, "_pdv_opera", None)
    if isinstance(cache, dict):
        cache.pop(cod_empresa, None)
