"""
Devolução e cancelamento de venda no PDV Matrix.

Segue o princípio do documento: a venda registra e encerra a operação
comercial. Devolver é uma **operação nova**, com documento próprio, que aponta
para a venda de origem e produz os efeitos contrários:

    Devolução → Itens devolvidos      → voltam ao Estoque
              → destino do valor      → Caixa / Nota a Prazo / Estorno de cartão

A venda original não é tocada: itens, valores e formas de recebimento
continuam como estavam. Cancelamento é a devolução total — mesmo caminho.
"""

from services.pdv_estoque_service import movimentar
from services.pdv_financeiro_service import lancar


DESTINOS_VALOR = {
    "DINHEIRO": "Devolver em dinheiro (sai do caixa)",
    "ABATIMENTO_NOTA_PRAZO": "Abater da nota a prazo em aberto",
    "ESTORNO_CARTAO": "Estorno pela operadora do cartão",
}


def itens_disponiveis(cur, cod_empresa, id_venda):
    """
    Itens da venda com o quanto ainda pode ser devolvido.

    `disponivel` = vendido − já devolvido em devoluções anteriores. É isto que
    impede devolver duas vezes a mesma peça.
    """
    cur.execute("""
        SELECT i.id_pdv_venda_item, i.id_pdv_produto, i.descricao_produto, i.unidade,
               i.quantidade, i.preco_unitario, i.valor_total, i.custo_unitario,
               COALESCE((
                   SELECT SUM(d.quantidade)
                   FROM pdv_devolucoes_itens d
                   WHERE d.id_pdv_venda_item = i.id_pdv_venda_item
               ), 0) AS devolvido
        FROM pdv_vendas_itens i
        WHERE i.id_pdv_venda = %s AND i.cod_empresa = %s
        ORDER BY i.sequencia
    """, (id_venda, cod_empresa))

    itens = []
    for linha in cur.fetchall():
        item = dict(linha)
        vendida = float(item["quantidade"] or 0)
        devolvida = float(item["devolvido"] or 0)
        item["quantidade"] = vendida
        item["devolvido"] = devolvida
        item["disponivel"] = round(vendida - devolvida, 3)
        # o preço unitário efetivo já considera o desconto dado no item
        item["preco_efetivo"] = (round(float(item["valor_total"] or 0) / vendida, 4)
                                 if vendida else 0.0)
        itens.append(item)
    return itens


def registrar_devolucao(cur, cod_empresa, cod_filial, id_venda, data_devolucao,
                        itens_devolvidos, destino_valor, id_conta=None, motivo=None,
                        id_usuario=None, nome_usuario=None):
    """
    Grava a devolução e produz os efeitos. Roda tudo na transação de quem
    chamou.

    `itens_devolvidos` é [{id_pdv_venda_item, quantidade}]. Devolve o
    dicionário da devolução criada.
    """
    cur.execute("""
        SELECT id_pdv_venda, situacao, valor_total, numero_venda
        FROM pdv_vendas WHERE id_pdv_venda = %s AND cod_empresa = %s
    """, (id_venda, cod_empresa))
    venda = cur.fetchone()
    if not venda:
        raise ValueError("Venda não encontrada.")
    if venda["situacao"] != "CONCLUIDA":
        raise ValueError("Esta venda já está cancelada.")

    if destino_valor not in DESTINOS_VALOR:
        raise ValueError("Destino do valor inválido.")
    if destino_valor == "DINHEIRO" and not id_conta:
        raise ValueError("Informe de qual conta o dinheiro vai sair.")

    disponiveis = {i["id_pdv_venda_item"]: i
                   for i in itens_disponiveis(cur, cod_empresa, id_venda)}

    linhas = []
    valor_total = 0.0
    for pedido in itens_devolvidos:
        id_item = pedido.get("id_pdv_venda_item")
        quantidade = round(float(pedido.get("quantidade") or 0), 3)
        if quantidade <= 0:
            continue
        item = disponiveis.get(id_item)
        if not item:
            raise ValueError("Item não pertence a esta venda.")
        if quantidade > item["disponivel"] + 0.0001:
            raise ValueError(
                f"{item['descricao_produto']}: só há {item['disponivel']:g} "
                f"disponível(is) para devolução."
            )
        valor = round(quantidade * item["preco_efetivo"], 2)
        valor_total += valor
        linhas.append({**item, "_quantidade": quantidade, "_valor": valor})

    if not linhas:
        raise ValueError("Nenhum item informado para devolução.")

    valor_total = round(valor_total, 2)

    # devolução total = cancelamento: todo o disponível de todos os itens veio
    total = all(
        any(l["id_pdv_venda_item"] == item["id_pdv_venda_item"]
            and abs(l["_quantidade"] - item["disponivel"]) < 0.0001
            for l in linhas)
        for item in disponiveis.values() if item["disponivel"] > 0
    )
    tipo = "TOTAL" if total else "PARCIAL"

    cur.execute("""
        SELECT COALESCE(MAX(numero_devolucao), 0) + 1 AS proximo
        FROM pdv_devolucoes WHERE cod_empresa = %s AND cod_filial = %s
    """, (cod_empresa, cod_filial))
    numero = cur.fetchone()["proximo"]

    cur.execute("""
        INSERT INTO pdv_devolucoes
            (cod_empresa, cod_filial, numero_devolucao, id_pdv_venda, data_devolucao,
             tipo, destino_valor, id_pdv_conta_financeira, valor_total, motivo,
             id_usuario, nome_usuario)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id_pdv_devolucao
    """, (cod_empresa, cod_filial, numero, id_venda, data_devolucao, tipo,
          destino_valor, id_conta, valor_total, motivo, id_usuario, nome_usuario))
    id_devolucao = cur.fetchone()["id_pdv_devolucao"]

    for linha in linhas:
        cur.execute("""
            INSERT INTO pdv_devolucoes_itens
                (cod_empresa, id_pdv_devolucao, id_pdv_venda_item, id_pdv_produto,
                 descricao_produto, unidade, quantidade, preco_unitario, valor_total,
                 custo_unitario)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (cod_empresa, id_devolucao, linha["id_pdv_venda_item"],
              linha["id_pdv_produto"], linha["descricao_produto"], linha["unidade"],
              linha["_quantidade"], linha["preco_efetivo"], linha["_valor"],
              linha["custo_unitario"]))

        # a mercadoria volta para a prateleira, pelo custo com que saiu
        if linha["id_pdv_produto"]:
            movimentar(
                cur, cod_empresa, cod_filial, linha["id_pdv_produto"], data_devolucao,
                tipo="DEVOLUCAO", quantidade=linha["_quantidade"],
                custo_unitario=linha["custo_unitario"],
                tipo_origem="DEVOLUCAO", id_origem=id_devolucao,
                historico=f"Devolução da venda nº {venda['numero_venda']}",
                id_usuario=id_usuario,
            )

    _devolver_valor(cur, cod_empresa, id_venda, id_devolucao, venda["numero_venda"],
                    destino_valor, id_conta, valor_total, data_devolucao, id_usuario)

    # Marcador de estado, não alteração de valores: os itens, os totais e as
    # formas de recebimento da venda continuam exatamente como estavam.
    if tipo == "TOTAL":
        cur.execute("""
            UPDATE pdv_vendas SET situacao = 'CANCELADA', atualizado_em = now()
            WHERE id_pdv_venda = %s AND cod_empresa = %s
        """, (id_venda, cod_empresa))

    return {"id_pdv_devolucao": id_devolucao, "numero_devolucao": numero,
            "tipo": tipo, "valor_total": valor_total}


def _devolver_valor(cur, cod_empresa, id_venda, id_devolucao, numero_venda,
                    destino_valor, id_conta, valor, data_devolucao, id_usuario):
    """
    O caminho de volta do dinheiro. Cada destino tem tratamento próprio — e
    dois deles **não** movimentam caixa, pelo mesmo princípio de sempre: só
    lança quem efetivamente mexeu em dinheiro.
    """
    if valor <= 0:
        return

    if destino_valor == "DINHEIRO":
        # saiu dinheiro do caixa agora
        lancar(cur, cod_empresa, id_conta, data_devolucao, -abs(valor),
               f"Devolução da venda nº {numero_venda}",
               tipo_origem="DEVOLUCAO", id_origem=id_devolucao, id_usuario=id_usuario)
        return

    if destino_valor == "ABATIMENTO_NOTA_PRAZO":
        # o cliente devia e devolveu a mercadoria: a dívida diminui, mas
        # dinheiro nenhum se moveu — logo, nenhum lançamento
        cur.execute("""
            SELECT id_pdv_nota_prazo, valor, valor_baixado
            FROM pdv_notas_prazo
            WHERE id_pdv_venda = %s AND cod_empresa = %s AND situacao = 'ABERTA'
            ORDER BY id_pdv_nota_prazo
        """, (id_venda, cod_empresa))
        nota = cur.fetchone()
        if not nota:
            raise ValueError(
                "Esta venda não tem nota a prazo em aberto para abater. "
                "Escolha outro destino para o valor."
            )

        saldo = round(float(nota["valor"] or 0) - float(nota["valor_baixado"] or 0), 2)
        if valor > saldo + 0.001:
            raise ValueError(
                f"O valor devolvido (R$ {valor:.2f}) é maior que o saldo em aberto "
                f"da nota a prazo (R$ {saldo:.2f})."
            )

        novo_valor = round(float(nota["valor"] or 0) - valor, 2)
        quitada = novo_valor <= round(float(nota["valor_baixado"] or 0), 2)
        cur.execute("""
            UPDATE pdv_notas_prazo
               SET valor = %s,
                   situacao = %s,
                   atualizado_em = now()
             WHERE id_pdv_nota_prazo = %s
        """, (novo_valor, "BAIXADA" if quitada else "ABERTA",
              nota["id_pdv_nota_prazo"]))
        return

    if destino_valor == "ESTORNO_CARTAO":
        # o estorno é feito na maquineta e chega pelo acerto da operadora: não
        # passa pelo caixa da loja. As parcelas a receber é que deixam de vir.
        cur.execute("""
            UPDATE pdv_cartoes_parcelas p
               SET situacao = 'ESTORNADO', atualizado_em = now()
              FROM pdv_cartoes_recebimentos cr
             WHERE cr.id_pdv_cartao_recebimento = p.id_pdv_cartao_recebimento
               AND cr.id_pdv_venda = %s AND cr.cod_empresa = %s
               AND p.situacao = 'A_RECEBER'
        """, (id_venda, cod_empresa))
        cur.execute("""
            UPDATE pdv_cartoes_recebimentos
               SET situacao = 'ESTORNADO', atualizado_em = now()
             WHERE id_pdv_venda = %s AND cod_empresa = %s
        """, (id_venda, cod_empresa))
        return
