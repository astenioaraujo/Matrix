"""Venda extra como ponta de descarrego (Operações → Informar Descarregos).

A venda extra (vendas_diarias com venda_extra = true) é combustível vendido
direto da carga: sai de uma compra sem passar pelo tanque. O descarrego que a
atende tem `id_venda_extra` preenchido — terceira ponta, ao lado da filial e da
coligada (migrations/descarrego_para_venda_extra.sql). Não é empréstimo.

No estoque, esse descarrego conta como entrada no posto DA VENDA, na data dela
(que é obrigatoriamente a data do descarrego): a venda extra já sai do tanque
desse posto como qualquer venda, e sem a entrada correspondente o dia fecharia
com uma sobra do tamanho da carga. Entra e sai no mesmo dia, efeito zero.
"""

from services.vendas_produtos import cod_produto_das_vendas


def id_venda_extra_do_form(form, campo="parte_descarga"):
    """A tela manda a ponta 'V:<id>' quando o destino é uma venda extra."""
    texto = (form.get(campo) or "").strip()
    if texto.startswith("V:") and texto[2:].isdigit():
        return int(texto[2:])
    return None


def _completar(linhas):
    for v in linhas:
        v["cod_produto"] = cod_produto_das_vendas(v["descricao"]) or ""
    return linhas


def vendas_extras_sem_descarrego(cur, cod_empresa, codigos_filiais=None):
    """Vendas extras ainda não ligadas a nenhum descarrego.

    `codigos_filiais` recorta pelos postos do usuário (None = todos)."""
    filtro = "AND v.cod_filial = ANY(%s)" if codigos_filiais is not None else ""
    params = [cod_empresa] + ([list(codigos_filiais)] if codigos_filiais is not None else [])
    cur.execute(f"""
        SELECT v.id, v.data, v.cod_filial, f.nome_filial, v.descricao,
               v.quantidade, v.observacao
        FROM vendas_diarias v
        LEFT JOIN filiais f
          ON f.cod_empresa = v.cod_empresa AND f.cod_filial = v.cod_filial
        WHERE v.cod_empresa = %s
          AND v.venda_extra
          {filtro}
          AND NOT EXISTS (
              SELECT 1 FROM descarregos_combustiveis d
              WHERE d.id_venda_extra = v.id
          )
        ORDER BY v.data DESC, v.cod_filial, v.id DESC
    """, params)
    return _completar(cur.fetchall() or [])


def venda_extra_travada(cur, cod_empresa, id_venda):
    """A venda extra, com FOR UPDATE (até o commit do descarrego), e o
    descarrego que já a atende, se houver. None se não for venda extra."""
    cur.execute("""
        SELECT v.id, v.data, v.cod_filial, v.descricao, v.quantidade
        FROM vendas_diarias v
        WHERE v.id = %s AND v.cod_empresa = %s AND v.venda_extra
        FOR UPDATE
    """, (id_venda, cod_empresa))
    venda = cur.fetchone()
    if not venda:
        return None
    cur.execute("""
        SELECT id_descarrego FROM descarregos_combustiveis
        WHERE id_venda_extra = %s
    """, (id_venda,))
    ligado = cur.fetchone()
    venda["id_descarrego"] = ligado["id_descarrego"] if ligado else None
    venda["cod_produto"] = cod_produto_das_vendas(venda["descricao"]) or ""
    return venda


def descarregos_das_vendas_extras(cur, ids_vendas):
    """{id_venda: {id_descarrego, data_descarrego, id_compra}} — a tela de
    Vendas Extras mostra se a venda já baixou uma compra."""
    if not ids_vendas:
        return {}
    cur.execute("""
        SELECT id_venda_extra, id_descarrego, data_descarrego, id_compra
        FROM descarregos_combustiveis
        WHERE id_venda_extra = ANY(%s)
    """, (list(ids_vendas),))
    return {r["id_venda_extra"]: r for r in cur.fetchall() or []}
