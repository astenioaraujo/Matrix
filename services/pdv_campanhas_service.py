"""
Campanhas promocionais do PDV Matrix.

    Campanha (nome + período + desconto)
      → Itens da Campanha (carregados por filtro: marca, categoria ou todos)
      → Venda dentro do período usa o preço promocional

O que fica gravado por item é o **percentual de desconto** — a decisão que foi
tomada. O preço promocional é calculado dele:

    preco_promocional = preco_venda × (1 − desconto/100)

Valor derivado não vira coluna, pela regra do projeto. E o preço que
efetivamente valeu numa venda continua gravado no item da venda, que é onde o
histórico precisa estar.

Fora do período a campanha simplesmente deixa de valer — nada precisa ser
desfeito quando ela termina.

Situação da campanha:

    ATIVA     → vale enquanto o dia estiver dentro do período
    PAUSADA   → suspensa; volta a valer ao ser retomada
    ENCERRADA → interrompida antes do previsto (data_fim vira o dia em que se
                encerrou, para o período gravado refletir o que valeu)
"""

SITUACOES_CAMPANHA = {
    "ATIVA":     "Ativa",
    "PAUSADA":   "Pausada",
    "ENCERRADA": "Encerrada",
}

from datetime import date


def _arredondar(valor):
    return round(float(valor or 0) + 1e-9, 2)


def preco_promocional(preco_venda, percentual):
    """Preço com o desconto aplicado, em reais."""
    preco = float(preco_venda or 0)
    desconto = float(percentual or 0)
    if preco <= 0 or desconto <= 0:
        return _arredondar(preco)
    return _arredondar(preco * (1 - desconto / 100.0))


def campanhas_vigentes(cur, cod_empresa, dia=None):
    """Campanhas ativas cujo período contém o dia (hoje, por padrão)."""
    dia = dia or date.today()
    cur.execute("""
        SELECT id_pdv_campanha, nome, data_inicio, data_fim, percentual_desconto,
               situacao
        FROM pdv_campanhas
        WHERE cod_empresa = %s AND situacao = 'ATIVA'
          AND %s BETWEEN data_inicio AND data_fim
        ORDER BY data_inicio, id_pdv_campanha
    """, (cod_empresa, dia))
    return [dict(r) for r in cur.fetchall()]


def promocoes_do_dia(cur, cod_empresa, dia=None):
    """
    Preço promocional de cada produto em campanha hoje, pronto para a tela de
    venda: `{id_pdv_produto: {...}}`.

    Um produto pode estar em mais de uma campanha ao mesmo tempo (Dia das Mães
    e queima de estoque, por exemplo). Nesse caso vale o **maior desconto** —
    é o preço mais barato, que é o que o cliente veria anunciado e o que a
    loja não pode deixar de honrar. O critério é fixo para não depender da
    ordem em que as campanhas foram cadastradas.
    """
    dia = dia or date.today()
    cur.execute("""
        SELECT DISTINCT ON (ci.id_pdv_produto)
               ci.id_pdv_produto, ci.percentual_desconto,
               c.id_pdv_campanha, c.nome, c.data_fim,
               p.preco_venda
        FROM pdv_campanhas_itens ci
        JOIN pdv_campanhas c ON c.id_pdv_campanha = ci.id_pdv_campanha
        JOIN pdv_produtos p ON p.id_pdv_produto = ci.id_pdv_produto
        WHERE ci.cod_empresa = %s AND c.situacao = 'ATIVA'
          AND %s BETWEEN c.data_inicio AND c.data_fim
        ORDER BY ci.id_pdv_produto, ci.percentual_desconto DESC, c.id_pdv_campanha
    """, (cod_empresa, dia))

    promocoes = {}
    for linha in cur.fetchall():
        r = dict(linha)
        promocoes[r["id_pdv_produto"]] = {
            "id_pdv_campanha": r["id_pdv_campanha"],
            "campanha": r["nome"],
            "percentual_desconto": float(r["percentual_desconto"] or 0),
            "preco_promocional": preco_promocional(r["preco_venda"],
                                                   r["percentual_desconto"]),
            "data_fim": r["data_fim"].isoformat() if r["data_fim"] else None,
        }
    return promocoes


def produtos_do_filtro(cur, cod_empresa, marca=None, categoria=None):
    """
    Produtos que o filtro alcança. Sem marca nem categoria = todos os ativos.

    A campanha é da **empresa**, não de uma loja: o preço promocional vale em
    qualquer filial que trabalhe a peça. Por isso o filtro varre o cadastro
    central, e não a lista de uma loja.
    """
    condicoes = ["cod_empresa = %s", "ativo"]
    parametros = [cod_empresa]
    if marca:
        condicoes.append("marca = %s")
        parametros.append(marca)
    if categoria:
        condicoes.append("categoria = %s")
        parametros.append(categoria)

    cur.execute(f"""
        SELECT id_pdv_produto, sku, descricao, marca, categoria, cor, tamanho,
               preco_venda
        FROM pdv_produtos
        WHERE {' AND '.join(condicoes)}
        ORDER BY descricao
    """, parametros)
    return [dict(r) for r in cur.fetchall()]


def carregar_itens(cur, cod_empresa, id_campanha, percentual, marca=None,
                   categoria=None):
    """
    Põe na campanha todos os produtos que o filtro alcança, com o percentual
    informado.

    Produto que já estava na campanha tem o desconto **atualizado** — rodar o
    filtro de novo com outro percentual corrige, em vez de duplicar.

    Devolve (incluídos, atualizados).
    """
    percentual = float(percentual or 0)
    if percentual <= 0 or percentual >= 100:
        raise ValueError("O desconto tem que estar entre 0 e 100.")

    produtos = produtos_do_filtro(cur, cod_empresa, marca, categoria)
    if not produtos:
        raise ValueError("Nenhum produto encontrado para este filtro.")

    from psycopg2.extras import execute_values

    # O filtro pode alcançar milhares de peças (a loja tem ~2.200): grava em
    # lote, como a importação do estoque.
    resultado = execute_values(cur, """
        INSERT INTO pdv_campanhas_itens
            (cod_empresa, id_pdv_campanha, id_pdv_produto, percentual_desconto)
        VALUES %s
        ON CONFLICT (id_pdv_campanha, id_pdv_produto)
        DO UPDATE SET percentual_desconto = EXCLUDED.percentual_desconto
        RETURNING (xmax = 0) AS incluido
    """, [(cod_empresa, id_campanha, p["id_pdv_produto"], percentual)
          for p in produtos],
        template="(%s,%s,%s,%s::numeric)", fetch=True)

    incluidos = sum(1 for r in resultado if r["incluido"])
    return incluidos, len(resultado) - incluidos


def carregar_itens_por_percentual(cur, cod_empresa, id_campanha, itens):
    """
    Põe itens na campanha **com o percentual de cada um**, em vez de um único
    desconto para todos.

    É o caminho da carga de implantação: o arquivo de estoque da loja traz o
    desconto peça a peça, sem seguir marca nem categoria, então o filtro de
    `carregar_itens()` não daria conta.

    `itens` é [{"id_pdv_produto": int, "percentual": float}]. Devolve
    (incluídos, atualizados).
    """
    if not itens:
        return 0, 0

    from psycopg2.extras import execute_values

    resultado = execute_values(cur, """
        INSERT INTO pdv_campanhas_itens
            (cod_empresa, id_pdv_campanha, id_pdv_produto, percentual_desconto)
        VALUES %s
        ON CONFLICT (id_pdv_campanha, id_pdv_produto)
        DO UPDATE SET percentual_desconto = EXCLUDED.percentual_desconto
        RETURNING (xmax = 0) AS incluido
    """, [(cod_empresa, id_campanha, i["id_pdv_produto"], i["percentual"])
          for i in itens],
        template="(%s,%s,%s,%s::numeric)", fetch=True)

    incluidos = sum(1 for r in resultado if r["incluido"])
    return incluidos, len(resultado) - incluidos
