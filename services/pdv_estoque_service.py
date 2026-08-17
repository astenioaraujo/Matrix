"""
Estoque do PDV Matrix.

O saldo de um produto (`pdv_produtos.quantidade_atual`) é o consolidado da
movimentação (`pdv_estoque_movimentos`), que funciona como o extrato de uma
conta: entrada positiva, saída negativa, e cada linha diz de onde veio.

    Saldo inicial + Entradas − Saídas = Saldo final

Por isso **ninguém escreve `quantidade_atual` por fora daqui**: quem movimenta
estoque chama `movimentar()`, que grava o movimento e ajusta o saldo na mesma
transação. Saldo sem movimento é saldo sem lastro — não dá para auditar nem
para descobrir depois de onde veio a diferença.
"""

# Tipos de movimento. O sinal da quantidade é que diz se entra ou sai; o tipo
# é a natureza do fato.
TIPOS_MOVIMENTO = {
    "VENDA":         "Venda",
    "ENTRADA":       "Entrada de mercadoria",
    "AJUSTE":        "Ajuste de estoque",
    "DEVOLUCAO":     "Devolução",
    "PERDA":         "Perda / quebra",
    "TRANSFERENCIA": "Transferência",
}


def movimentar(cur, cod_empresa, cod_filial, id_produto, data_movimento, tipo,
               quantidade, custo_unitario=0, tipo_origem="AJUSTE", id_origem=None,
               historico=None, id_usuario=None):
    """
    Registra um movimento e reflete no saldo do produto.

    `quantidade` positiva entra, negativa sai. Recebe o cursor de quem chamou
    para rodar dentro da transação do documento que provocou o movimento (a
    venda, a nota de entrada): ou tudo grava, ou nada grava.

    Devolve o id do movimento.
    """
    if not quantidade:
        return None

    cur.execute("""
        INSERT INTO pdv_estoque_movimentos
            (cod_empresa, cod_filial, id_pdv_produto, data_movimento, tipo,
             quantidade, custo_unitario, tipo_origem, id_origem, historico, id_usuario)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id_pdv_estoque_movimento
    """, (cod_empresa, cod_filial, id_produto, data_movimento, tipo,
          quantidade, custo_unitario or 0, tipo_origem, id_origem,
          historico, id_usuario))
    linha = cur.fetchone()
    id_movimento = linha[0] if isinstance(linha, tuple) else linha["id_pdv_estoque_movimento"]

    cur.execute("""
        UPDATE pdv_produtos
           SET quantidade_atual = quantidade_atual + %s,
               atualizado_em = now()
         WHERE id_pdv_produto = %s AND cod_empresa = %s
    """, (quantidade, id_produto, cod_empresa))

    return id_movimento


def baixar_itens_da_venda(cur, cod_empresa, cod_filial, id_venda, data_venda,
                          itens, id_usuario=None):
    """
    Saída de estoque dos itens de uma venda concluída.

    `itens` são as linhas já gravadas em pdv_vendas_itens (dicionários com
    id_pdv_produto, quantidade, custo_unitario, descricao_produto). Item sem
    produto cadastrado não movimenta nada.
    """
    for item in itens:
        id_produto = item.get("id_pdv_produto")
        if not id_produto:
            continue
        movimentar(
            cur, cod_empresa, cod_filial, id_produto, data_venda,
            tipo="VENDA",
            quantidade=-abs(float(item.get("quantidade") or 0)),
            custo_unitario=item.get("custo_unitario") or 0,
            tipo_origem="VENDA",
            id_origem=id_venda,
            historico=f"Venda — {item.get('descricao_produto') or ''}".strip(" —"),
            id_usuario=id_usuario,
        )


def extrato_produto(cur, cod_empresa, id_produto, data_de, data_ate):
    """
    Extrato do produto no período: o saldo em que ele começou, os movimentos e
    o saldo em que terminou.

    O saldo inicial é reconstruído somando tudo o que veio antes do período —
    não existe coluna de "saldo do dia" em lugar nenhum, pelo mesmo motivo de
    sempre: total não se persiste, se calcula.
    """
    cur.execute("""
        SELECT COALESCE(SUM(quantidade), 0) AS saldo
        FROM pdv_estoque_movimentos
        WHERE cod_empresa = %s AND id_pdv_produto = %s AND data_movimento < %s
    """, (cod_empresa, id_produto, data_de))
    saldo_inicial = float(cur.fetchone()["saldo"] or 0)

    cur.execute("""
        SELECT id_pdv_estoque_movimento, data_movimento, tipo, quantidade,
               custo_unitario, tipo_origem, id_origem, historico
        FROM pdv_estoque_movimentos
        WHERE cod_empresa = %s AND id_pdv_produto = %s
          AND data_movimento BETWEEN %s AND %s
        ORDER BY data_movimento, id_pdv_estoque_movimento
    """, (cod_empresa, id_produto, data_de, data_ate))
    movimentos = [dict(r) for r in cur.fetchall()]

    # o saldo corrente é calculado aqui, linha a linha, para a tela mostrar
    # como o produto chegou ao saldo final
    saldo = saldo_inicial
    entradas = 0.0
    saidas = 0.0
    for m in movimentos:
        quantidade = float(m["quantidade"] or 0)
        saldo += quantidade
        m["saldo"] = saldo
        if quantidade >= 0:
            entradas += quantidade
        else:
            saidas += -quantidade

    return {
        "saldo_inicial": saldo_inicial,
        "movimentos": movimentos,
        "entradas": entradas,
        "saidas": saidas,
        "saldo_final": saldo,
    }
