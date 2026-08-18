"""
Canais de venda do PDV Matrix.

Um canal é a porta por onde a venda saiu (balcão, e-commerce, outlet) — não é
uma filial. A diferença importa no estoque:

    estoque_por_canal = FALSE (padrão)
        Os canais usam o estoque da filial. Existe um saldo só; o canal serve
        para saber por onde a venda saiu. Não há transferência a fazer.

    estoque_por_canal = TRUE
        Cada canal tem saldo próprio, calculado a partir dos movimentos que
        levam o canal. Mover peça de um canal para outro exige transferência —
        que é um par de movimentos e não muda o saldo total do produto.

O canal é gravado no movimento e na venda nos **dois** modos: mesmo com
estoque compartilhado, dá para responder quanto o e-commerce vendeu.
"""

from services.pdv_estoque_service import movimentar


def estoque_por_canal(cur, cod_empresa):
    """A empresa controla saldo separado por canal?"""
    cur.execute(
        "SELECT estoque_por_canal FROM pdv_parametros WHERE cod_empresa = %s",
        (cod_empresa,),
    )
    linha = cur.fetchone()
    return bool(linha["estoque_por_canal"]) if linha else False


def canais_da_filial(cur, cod_empresa, cod_filial, so_ativos=True):
    cur.execute(f"""
        SELECT id_pdv_canal, cod_filial, nome, padrao, ativo, ordem
        FROM pdv_canais_venda
        WHERE cod_empresa = %s AND cod_filial = %s {'AND ativo' if so_ativos else ''}
        ORDER BY ordem, nome
    """, (cod_empresa, cod_filial))
    return [dict(r) for r in cur.fetchall()]


def canal_padrao(cur, cod_empresa, cod_filial):
    """O canal que a tela de venda já vem marcando. Sem padrão, o primeiro."""
    canais = canais_da_filial(cur, cod_empresa, cod_filial)
    if not canais:
        return None
    return next((c for c in canais if c["padrao"]), canais[0])


def saldos_por_canal(cur, cod_empresa, id_produto):
    """
    Quanto do produto está em cada canal, somado dos movimentos.

    Só faz sentido com `estoque_por_canal = TRUE`; com estoque compartilhado o
    saldo do produto é um só e este detalhamento não representa separação
    física nenhuma.
    """
    cur.execute("""
        SELECT c.id_pdv_canal, c.nome,
               COALESCE(SUM(m.quantidade), 0) AS saldo
        FROM pdv_canais_venda c
        LEFT JOIN pdv_estoque_movimentos m
               ON m.id_pdv_canal = c.id_pdv_canal
              AND m.id_pdv_produto = %s
        WHERE c.cod_empresa = %s AND c.ativo
        GROUP BY c.id_pdv_canal, c.nome, c.ordem
        ORDER BY c.ordem, c.nome
    """, (id_produto, cod_empresa))
    saldos = [dict(r) for r in cur.fetchall()]
    for s in saldos:
        s["saldo"] = float(s["saldo"] or 0)

    # o que entrou sem canal definido (carga antiga, ajuste sem canal)
    cur.execute("""
        SELECT COALESCE(SUM(quantidade), 0) AS saldo
        FROM pdv_estoque_movimentos
        WHERE cod_empresa = %s AND id_pdv_produto = %s AND id_pdv_canal IS NULL
    """, (cod_empresa, id_produto))
    sem_canal = float(cur.fetchone()["saldo"] or 0)

    return {"canais": saldos, "sem_canal": sem_canal}


def transferir_estoque(cur, cod_empresa, cod_filial, id_produto, id_canal_origem,
                       id_canal_destino, quantidade, data_movimento,
                       observacao=None, id_usuario=None):
    """
    Move peças de um canal para outro: um par de movimentos, saída num canal e
    entrada no outro.

    O saldo TOTAL do produto não muda — a peça continua na loja, só passou a
    ser oferecida por outro canal. Por isso os dois movimentos se anulam em
    `pdv_produtos_filiais.quantidade_atual`, que é o saldo da loja.
    """
    if id_canal_origem == id_canal_destino:
        raise ValueError("O canal de origem e o de destino têm que ser diferentes.")
    quantidade = round(float(quantidade or 0), 3)
    if quantidade <= 0:
        raise ValueError("Informe uma quantidade maior que zero.")

    saldos = saldos_por_canal(cur, cod_empresa, id_produto)
    origem = next((c for c in saldos["canais"]
                   if c["id_pdv_canal"] == id_canal_origem), None)
    destino = next((c for c in saldos["canais"]
                    if c["id_pdv_canal"] == id_canal_destino), None)
    if not origem or not destino:
        raise ValueError("Canal não encontrado.")
    if quantidade > origem["saldo"] + 0.0001:
        raise ValueError(
            f"{origem['nome']} tem apenas {origem['saldo']:g} em estoque."
        )

    texto = observacao or f"Transferência {origem['nome']} → {destino['nome']}"

    cur.execute("""
        SELECT custo_atual FROM pdv_produtos
        WHERE id_pdv_produto = %s AND cod_empresa = %s
    """, (id_produto, cod_empresa))
    produto = cur.fetchone()
    custo = float(produto["custo_atual"] or 0) if produto else 0

    movimentar(cur, cod_empresa, cod_filial, id_produto, data_movimento,
               tipo="TRANSFERENCIA", quantidade=-quantidade, custo_unitario=custo,
               tipo_origem="TRANSFERENCIA_CANAL", historico=texto,
               id_usuario=id_usuario, id_canal=id_canal_origem)

    movimentar(cur, cod_empresa, cod_filial, id_produto, data_movimento,
               tipo="TRANSFERENCIA", quantidade=quantidade, custo_unitario=custo,
               tipo_origem="TRANSFERENCIA_CANAL", historico=texto,
               id_usuario=id_usuario, id_canal=id_canal_destino)
