"""Coligadas — empresas da família/grupo econômico que não são do grupo.

Elas compram e descarregam combustível junto com a gente, mas o estoque,
a perda e a sobra delas não são nossos. Por isso a coligada é uma das
pontas possíveis de uma compra ou de um descarrego, e nunca uma filial.

Não existe parâmetro "trabalha com coligadas": empresa com coligada ativa
cadastrada trabalha com coligadas. Um flag à parte seria a mesma verdade
em dois lugares, e divergiria na primeira manutenção.
"""

from flask import g, has_request_context


def coligadas_ativas(cur, cod_empresa):
    cur.execute("""
        SELECT id_coligada, nome, nome_fantasia
        FROM operacoes_coligadas
        WHERE cod_empresa = %s
          AND ativo = TRUE
        ORDER BY ordem, nome
    """, (str(cod_empresa).strip(),))
    return cur.fetchall() or []


def empresa_tem_coligadas(cur, cod_empresa):
    """A empresa trabalha com coligadas? Cacheado na requisição — o menu e
    cada tela de lançamento perguntam a mesma coisa."""
    cod_empresa = str(cod_empresa).strip()
    chave = f"_tem_coligadas_{cod_empresa}"

    if has_request_context() and hasattr(g, chave):
        return getattr(g, chave)

    cur.execute("""
        SELECT 1
        FROM operacoes_coligadas
        WHERE cod_empresa = %s
          AND ativo = TRUE
        LIMIT 1
    """, (cod_empresa,))
    resposta = cur.fetchone() is not None

    if has_request_context():
        setattr(g, chave, resposta)

    return resposta


def parte_do_form(form, campo="parte"):
    """A tela manda a ponta como 'F:<cod_filial>' ou 'C:<id_coligada>'.

    Devolve (cod_filial, id_coligada) com exatamente um dos dois preenchido,
    ou (None, None) quando o texto não vale — quem chama decide a mensagem.
    """
    texto = (form.get(campo) or "").strip()

    if texto.startswith("F:") and texto[2:].isdigit():
        return int(texto[2:]), None

    if texto.startswith("C:") and texto[2:].isdigit():
        return None, int(texto[2:])

    # compatibilidade: tela antiga manda só o número da filial
    if texto.isdigit():
        return int(texto), None

    return None, None


def coligada_valida(cur, cod_empresa, id_coligada):
    cur.execute("""
        SELECT 1
        FROM operacoes_coligadas
        WHERE id_coligada = %s
          AND cod_empresa = %s
          AND ativo = TRUE
    """, (id_coligada, str(cod_empresa).strip()))
    return cur.fetchone() is not None
