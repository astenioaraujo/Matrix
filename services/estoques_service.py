from datetime import timedelta

def linhas_estoque(cur, cod_empresa, data_base, codigos_filiais=None):
    """Linhas da Consulta de Estoques (Operações) para uma data.

    Uma linha por filial × produto, com os valores em R$ que a tela mostra:
    `estoque_atual_rs` (medição de `data_base`), `compras_rs` e `transito_rs`
    (do dia anterior — é assim que a tela sempre exibiu). Extraído da rota
    para que a importação em Financeiro → Saldos puxe exatamente os mesmos
    números, sem uma segunda consulta que possa divergir com o tempo.

    `codigos_filiais=None` significa todas as filiais ativas da empresa.
    O cursor precisa ser RealDictCursor — as linhas voltam como dicionário.
    """
    data_anterior = data_base - timedelta(days=1)
    data_sel = data_base.isoformat()

    if codigos_filiais is None:
        filtro_filiais_sql = ""
        params_filiais = []
    else:
        filtro_filiais_sql = "AND f.cod_filial = ANY(%s)"
        params_filiais = [list(codigos_filiais)]

    sql = f"""
        WITH base AS (
            SELECT
                f.cod_filial,
                f.nome_filial,
                c.cod_produto,
                c.descricao AS produto,
                COALESCE(ct.capacidade_tanque, 0) AS capacidade_tanque
            FROM filiais f
            JOIN capacidade_tanques ct
              ON ct.cod_empresa = f.cod_empresa
             AND ct.cod_filial = f.cod_filial
            JOIN combustiveis c
              ON c.cod_empresa = ct.cod_empresa
             AND c.cod_produto = ct.cod_produto
            WHERE f.cod_empresa = %s
              AND f.ativo = TRUE
              AND COALESCE(ct.capacidade_tanque, 0) > 0
              {filtro_filiais_sql}
        ),

        medicao_anterior AS (
            SELECT
                cod_filial,
                cod_produto,
                SUM(COALESCE(quantidade_medida, 0)) AS medicao_anterior
            FROM medicoes
            WHERE cod_empresa = %s
              AND data_medicao = %s
            GROUP BY cod_filial, cod_produto
        ),

        vendas AS (
            SELECT
                cod_filial,
                CASE
                    WHEN POSITION('S10' IN txt) > 0 THEN 'C5'
                    WHEN POSITION('S500' IN txt) > 0 THEN 'C4'
                    WHEN POSITION('ADIT' IN txt) > 0 THEN 'C2'
                    WHEN POSITION('ETAN' IN txt) > 0 THEN 'C3'
                    WHEN POSITION('PODIUM' IN txt) > 0 THEN 'C6'
                    WHEN POSITION('GASOL' IN txt) > 0 THEN 'C1'
                    ELSE NULL
                END AS cod_produto,
                SUM(COALESCE(quantidade, 0)) AS vendas
            FROM (
                SELECT
                    cod_filial,
                    quantidade,
                    REGEXP_REPLACE(
                        UPPER(COALESCE(descricao, '')),
                        '[^A-Z0-9]',
                        '',
                        'g'
                    ) AS txt
                FROM vendas_diarias
                WHERE cod_empresa = %s
                AND data = %s
            ) vd
            GROUP BY
                cod_filial,
                CASE
                    WHEN POSITION('S10' IN txt) > 0 THEN 'C5'
                    WHEN POSITION('S500' IN txt) > 0 THEN 'C4'
                    WHEN POSITION('ADIT' IN txt) > 0 THEN 'C2'
                    WHEN POSITION('ETAN' IN txt) > 0 THEN 'C3'
                    WHEN POSITION('PODIUM' IN txt) > 0 THEN 'C6'
                    WHEN POSITION('GASOL' IN txt) > 0 THEN 'C1'
                    ELSE NULL
                END
        ),

        media_vendas AS (
            SELECT
                cod_filial,
                CASE
                    WHEN POSITION('S10' IN txt) > 0 THEN 'C5'
                    WHEN POSITION('S500' IN txt) > 0 THEN 'C4'
                    WHEN POSITION('ADIT' IN txt) > 0 THEN 'C2'
                    WHEN POSITION('ETAN' IN txt) > 0 THEN 'C3'
                    WHEN POSITION('PODIUM' IN txt) > 0 THEN 'C6'
                    WHEN POSITION('GASOL' IN txt) > 0 THEN 'C1'
                    ELSE NULL
                END AS cod_produto,

                SUM(COALESCE(quantidade, 0))
                / NULLIF(COUNT(DISTINCT data), 0) AS media_vendas_dia

            FROM (
                SELECT
                    cod_filial,
                    quantidade,
                    data,
                    REGEXP_REPLACE(
                        UPPER(COALESCE(descricao, '')),
                        '[^A-Z0-9]',
                        '',
                        'g'
                    ) AS txt
                FROM vendas_diarias
                WHERE cod_empresa = %s
                AND data >= %s
                AND data <= %s
            ) vd

            GROUP BY
                cod_filial,
                CASE
                    WHEN POSITION('S10' IN txt) > 0 THEN 'C5'
                    WHEN POSITION('S500' IN txt) > 0 THEN 'C4'
                    WHEN POSITION('ADIT' IN txt) > 0 THEN 'C2'
                    WHEN POSITION('ETAN' IN txt) > 0 THEN 'C3'
                    WHEN POSITION('PODIUM' IN txt) > 0 THEN 'C6'
                    WHEN POSITION('GASOL' IN txt) > 0 THEN 'C1'
                    ELSE NULL
                END
        ),

        compras_dia AS (
            SELECT
                cod_filial,
                cod_produto,
                SUM(COALESCE(quantidade_comprada, 0)) AS compras,
                SUM(COALESCE(valor_comprado, 0)) AS compras_rs
            FROM compras_combustiveis
            WHERE cod_empresa = %s
              AND data_compra = %s
            GROUP BY cod_filial, cod_produto
        ),

        transito AS (
            SELECT
                cc.cod_filial,
                cc.cod_produto,
                SUM(
                    COALESCE(cc.quantidade_comprada, 0)
                    - COALESCE(d.total_descarregado, 0)
                ) AS estoque_transito
            FROM compras_combustiveis cc
            LEFT JOIN (
                SELECT
                    id_compra,
                    cod_empresa,
                    SUM(COALESCE(quantidade_descarregada, 0)) AS total_descarregado
                FROM descarregos_combustiveis
                WHERE cod_empresa = %s
                GROUP BY id_compra, cod_empresa
            ) d
              ON d.cod_empresa = cc.cod_empresa
             AND d.id_compra = cc.id_compra
            WHERE cc.cod_empresa = %s
              AND cc.data_compra < %s
              AND COALESCE(cc.status, 'ABERTA') = 'ABERTA'
            GROUP BY cc.cod_filial, cc.cod_produto
        ),

        descarregos AS (
            SELECT
                cod_filial_descarga AS cod_filial,
                cod_produto,
                SUM(COALESCE(quantidade_descarregada, 0)) AS descarregos
            FROM descarregos_combustiveis
            WHERE cod_empresa = %s
              AND data_descarrego = %s
            GROUP BY cod_filial_descarga, cod_produto
        ),

        medicao_atual AS (
            SELECT
                cod_filial,
                cod_produto,
                SUM(COALESCE(quantidade_medida, 0)) AS medicao_atual
            FROM medicoes
            WHERE cod_empresa = %s
              AND data_medicao = %s
            GROUP BY cod_filial, cod_produto
        ),

        ultima_compra AS (
            SELECT DISTINCT ON (cod_filial, cod_produto)
                cod_filial,
                cod_produto,
                COALESCE(preco_unitario, 0) AS preco_ultima_compra
            FROM compras_combustiveis
            WHERE cod_empresa = %s
              AND data_compra <= %s
            ORDER BY cod_filial, cod_produto, data_compra DESC, id_compra DESC
        ),

        preco_data AS (
            SELECT DISTINCT ON (cod_produto)
                cod_produto,
                COALESCE(preco_compra, 0) AS preco_tabela
            FROM precos_compra
            WHERE cod_empresa = %s
            AND data_preco <= %s
            ORDER BY cod_produto, data_preco DESC
        ),

        ultima_compra_empresa AS (
            SELECT DISTINCT ON (cod_produto)
                cod_produto,
                COALESCE(preco_unitario, 0) AS preco_empresa
            FROM compras_combustiveis
            WHERE cod_empresa = %s
              AND data_compra <= %s
              AND COALESCE(preco_unitario, 0) > 0
            ORDER BY cod_produto, data_compra DESC, id_compra DESC
        )

        SELECT
            b.cod_filial,
            b.nome_filial,
            b.cod_produto,
            b.produto,
            b.capacidade_tanque,

            COALESCE(ma.medicao_anterior, 0) AS medicao_anterior,
            COALESCE(v.vendas, 0) AS vendas,
            COALESCE(mv.media_vendas_dia, 0) AS media_vendas_dia,
            COALESCE(cd.compras, 0) AS compras,
            COALESCE(t.estoque_transito, 0) AS estoque_transito,
            COALESCE(ds.descarregos, 0) AS descarregos,

            (
                COALESCE(ma.medicao_anterior, 0)
                - COALESCE(v.vendas, 0)
                + COALESCE(cd.compras, 0)
                + COALESCE(t.estoque_transito, 0)
                + COALESCE(ds.descarregos, 0)
            ) AS estoque_calculado,

            COALESCE(mat.medicao_atual, 0) AS medicao_atual,

            COALESCE(NULLIF(uc.preco_ultima_compra, 0), pd.preco_tabela, uce.preco_empresa, 0) AS preco_ultima_compra,

            COALESCE(mat.medicao_atual, 0)
            * COALESCE(NULLIF(uc.preco_ultima_compra, 0), pd.preco_tabela, uce.preco_empresa, 0) AS estoque_atual_rs,
            (
                COALESCE(mat.medicao_atual, 0)
                - (
                    COALESCE(ma.medicao_anterior, 0)
                    + COALESCE(ds.descarregos, 0)
                    - COALESCE(v.vendas, 0)
                )
            )
            * COALESCE(NULLIF(uc.preco_ultima_compra, 0), pd.preco_tabela, uce.preco_empresa, 0) AS perda_sobra_rs,

            COALESCE(cd.compras_rs, 0) AS compras_rs,

            COALESCE(t.estoque_transito, 0)
            * COALESCE(NULLIF(uc.preco_ultima_compra, 0), pd.preco_tabela, uce.preco_empresa, 0) AS transito_rs

        FROM base b

        LEFT JOIN medicao_anterior ma
          ON ma.cod_filial = b.cod_filial
         AND ma.cod_produto = b.cod_produto

        LEFT JOIN vendas v
          ON v.cod_filial = b.cod_filial
         AND v.cod_produto = b.cod_produto

        LEFT JOIN media_vendas mv
          ON mv.cod_filial = b.cod_filial
         AND mv.cod_produto = b.cod_produto

        LEFT JOIN compras_dia cd
          ON cd.cod_filial = b.cod_filial
         AND cd.cod_produto = b.cod_produto

        LEFT JOIN transito t
          ON t.cod_filial = b.cod_filial
         AND t.cod_produto = b.cod_produto

        LEFT JOIN descarregos ds
          ON ds.cod_filial = b.cod_filial
         AND ds.cod_produto = b.cod_produto

        LEFT JOIN medicao_atual mat
          ON mat.cod_filial = b.cod_filial
         AND mat.cod_produto = b.cod_produto

        LEFT JOIN ultima_compra uc
          ON uc.cod_filial = b.cod_filial
         AND uc.cod_produto = b.cod_produto

        LEFT JOIN preco_data pd
          ON pd.cod_produto = b.cod_produto

        LEFT JOIN ultima_compra_empresa uce
          ON uce.cod_produto = b.cod_produto

        ORDER BY b.cod_filial, b.cod_produto
    """

    params = (
        [cod_empresa]
        + params_filiais
        + [
            cod_empresa, data_anterior,  # medicao_anterior

            cod_empresa, data_anterior,  # vendas

            cod_empresa,
            data_base - timedelta(days=7),
            data_anterior,               # media_vendas

            cod_empresa, data_anterior,  # compras_dia

            cod_empresa,                 # transito subquery
            cod_empresa, data_anterior,  # transito

            cod_empresa, data_anterior,  # descarregos

            cod_empresa, data_sel,       # medicao_atual

            cod_empresa, data_anterior,  # ultima_compra

            cod_empresa, data_sel,       # preco_data

            cod_empresa, data_anterior,  # ultima_compra_empresa
        ]
    )


    cur.execute(sql, params)
    return cur.fetchall() or []


# Cada indicador de Saldos marcado com `origem_estoque` puxa uma destas colunas.
COLUNA_POR_ORIGEM = {
    "COMPRA": "compras_rs",
    "TRANSITO": "transito_rs",
    "ESTOQUE": "estoque_atual_rs",
    # Perdas e Sobras vai para o bloco Valores Informados, não para um
    # indicador de recebível — mas sai da mesma Consulta de Estoques.
    "PERDA_SOBRA": "perda_sobra_rs",
}


def totais_estoque_rs(cur, cod_empresa, data_base, codigos_filiais):
    """Total em R$ por filial, por origem (COMPRA/TRANSITO/ESTOQUE)."""
    totais = {int(f): {origem: 0.0 for origem in COLUNA_POR_ORIGEM} for f in codigos_filiais}

    for linha in linhas_estoque(cur, cod_empresa, data_base, codigos_filiais):
        filial = totais.get(int(linha["cod_filial"]))
        if filial is None:
            continue
        for origem, coluna in COLUNA_POR_ORIGEM.items():
            filial[origem] += float(linha.get(coluna) or 0)

    return totais
