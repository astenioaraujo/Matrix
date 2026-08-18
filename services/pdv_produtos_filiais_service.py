"""
O produto na loja.

    pdv_produtos          → cadastro central da empresa (SKU, descrição, preço)
    pdv_produtos_filiais  → quais peças a loja trabalha e quanto ela tem

O catálogo é único porque a peça é a mesma em qualquer loja — mesmo SKU, mesma
descrição, mesmo preço. Mas cada loja tem a **sua** lista: sem isso, uma filial
pequena carregaria itens que nunca vendeu e que já saíram de linha.

O item entra na loja sozinho, quando aparece movimento dele ali, ou pelo SKU
digitado. E **nunca é apagado** — venda e movimento apontam para ele. Quando
para de fazer sentido na loja, fica `OCULTO`: some das telas, continua no
histórico.
"""

from datetime import date, timedelta


# Zerar não é sair de linha: é precisar repor. Só depois deste tempo parado é
# que o item vira candidato a sumir da lista da loja.
MESES_SEM_MOVIMENTO = 6
DIAS_SEM_MOVIMENTO = MESES_SEM_MOVIMENTO * 30

SITUACOES_PRODUTO_FILIAL = {
    "ATIVO":  "Ativo na loja",
    "OCULTO": "Oculto (fora de linha)",
}


def garantir_produto_na_filial(cur, cod_empresa, cod_filial, id_produto,
                               origem="AUTOMATICA", data_movimento=None):
    """
    Garante que o produto exista na loja e devolve o id da linha.

    É chamado por quem movimenta estoque: receber a mercadoria já inclui o
    item na loja, sem ninguém precisar cadastrá-lo ali antes. Se o item
    estava oculto e voltou a se movimentar, ele **volta a aparecer** — o
    movimento é a prova de que a loja trabalha aquilo de novo.
    """
    cur.execute("""
        INSERT INTO pdv_produtos_filiais
            (cod_empresa, cod_filial, id_pdv_produto, origem_inclusao,
             ultimo_movimento_em)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (cod_empresa, cod_filial, id_pdv_produto)
        DO UPDATE SET
            ultimo_movimento_em = COALESCE(EXCLUDED.ultimo_movimento_em,
                                           pdv_produtos_filiais.ultimo_movimento_em),
            situacao = 'ATIVO',
            ocultado_em = NULL,
            atualizado_em = now()
        RETURNING id_pdv_produto_filial
    """, (cod_empresa, cod_filial, id_produto, origem, data_movimento))
    linha = cur.fetchone()
    return linha[0] if isinstance(linha, tuple) else linha["id_pdv_produto_filial"]


def incluir_por_sku(cur, cod_empresa, cod_filial, sku):
    """
    Inclusão manual: digita-se o SKU e o item passa a existir na loja.

    A descrição, o preço e o resto continuam vindo do cadastro central — aqui
    só se diz "esta loja trabalha esta peça". Serve para preparar a loja para
    uma mercadoria que ainda vai chegar.
    """
    sku = (sku or "").strip()
    if not sku:
        raise ValueError("Informe o SKU.")

    cur.execute("""
        SELECT id_pdv_produto, sku, descricao FROM pdv_produtos
        WHERE cod_empresa = %s AND upper(sku) = upper(%s) AND ativo
    """, (cod_empresa, sku))
    produto = cur.fetchone()
    if not produto:
        raise ValueError(f"SKU {sku} não existe no cadastro da empresa.")

    cur.execute("""
        SELECT situacao FROM pdv_produtos_filiais
        WHERE cod_empresa = %s AND cod_filial = %s AND id_pdv_produto = %s
    """, (cod_empresa, cod_filial, produto["id_pdv_produto"]))
    ja = cur.fetchone()

    garantir_produto_na_filial(cur, cod_empresa, cod_filial,
                               produto["id_pdv_produto"], origem="MANUAL")

    return {
        "id_pdv_produto": produto["id_pdv_produto"],
        "sku": produto["sku"],
        "descricao": produto["descricao"],
        # já estava lá: ou continuava ativo, ou acabou de voltar do oculto
        "ja_existia": bool(ja),
        "reativado": bool(ja and ja["situacao"] == "OCULTO"),
    }


def ocultar(cur, cod_empresa, cod_filial, id_produto, ocultar_=True):
    """
    Tira o item das telas da loja — ou o traz de volta.

    Não apaga nada: o item continua ligado às vendas e aos movimentos, e
    qualquer relatório de vendas passadas continua contando com ele.
    """
    cur.execute("""
        SELECT quantidade_atual FROM pdv_produtos_filiais
        WHERE cod_empresa = %s AND cod_filial = %s AND id_pdv_produto = %s
    """, (cod_empresa, cod_filial, id_produto))
    linha = cur.fetchone()
    if not linha:
        raise ValueError("Este produto não está nesta loja.")

    if ocultar_ and float(linha["quantidade_atual"] or 0) != 0:
        raise ValueError(
            "Este produto ainda tem saldo nesta loja. Zere o estoque antes de ocultar."
        )

    cur.execute("""
        UPDATE pdv_produtos_filiais
           SET situacao = %s,
               ocultado_em = %s,
               atualizado_em = now()
         WHERE cod_empresa = %s AND cod_filial = %s AND id_pdv_produto = %s
    """, ("OCULTO" if ocultar_ else "ATIVO",
          date.today() if ocultar_ else None,
          cod_empresa, cod_filial, id_produto))


def candidatos_a_ocultar(cur, cod_empresa, cod_filial, dias=DIAS_SEM_MOVIMENTO):
    """
    Itens que a loja pode deixar de exibir: zerados e parados há tempo demais.

    É uma **sugestão**, não uma faxina automática: quem decide o que sai da
    lista da loja é quem cuida dela. Sumir sozinho com um item que a compradora
    esperava repor seria pior do que a lista comprida.
    """
    limite = date.today() - timedelta(days=dias)
    cur.execute("""
        SELECT pf.id_pdv_produto, p.sku, p.descricao, p.marca,
               pf.ultimo_movimento_em, pf.incluido_em
        FROM pdv_produtos_filiais pf
        JOIN pdv_produtos p ON p.id_pdv_produto = pf.id_pdv_produto
        WHERE pf.cod_empresa = %s AND pf.cod_filial = %s
          AND pf.situacao = 'ATIVO'
          AND pf.quantidade_atual = 0
          AND COALESCE(pf.ultimo_movimento_em, pf.incluido_em) < %s
        ORDER BY COALESCE(pf.ultimo_movimento_em, pf.incluido_em), p.descricao
    """, (cod_empresa, cod_filial, limite))
    return [dict(r) for r in cur.fetchall()]
