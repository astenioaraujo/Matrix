from db import get_connection
from utils.helpers import conta_para_ordenacao

def obter_dados_matricial(cod_empresa, ano_sel="", mes_sel="", filial_sel=""):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT DISTINCT ano
            FROM lancamentos
            WHERE cod_empresa = %s
              AND ano IS NOT NULL
            ORDER BY ano
        """, (cod_empresa,))
        anos = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT mes
            FROM lancamentos
            WHERE cod_empresa = %s
              AND mes IS NOT NULL
            ORDER BY mes
        """, (cod_empresa,))
        meses = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = true
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()

        where_extra = []
        params = [cod_empresa]

        if ano_sel:
            where_extra.append("l.ano = %s")
            params.append(int(ano_sel))

        if mes_sel:
            where_extra.append("l.mes = %s")
            params.append(int(mes_sel))

        if filial_sel:
            where_extra.append("l.cod_filial = %s")
            params.append(int(filial_sel))

        where_sql = ""
        if where_extra:
            where_sql = " AND " + " AND ".join(where_extra)

        cur.execute(f"""
            SELECT DISTINCT
                l.cod_filial,
                COALESCE(f.nome_filial, CAST(l.cod_filial AS TEXT)) AS nome_filial
            FROM lancamentos l
            LEFT JOIN filiais f
                   ON f.cod_empresa = l.cod_empresa
                  AND f.cod_filial = l.cod_filial
            WHERE l.cod_empresa = %s
              {where_sql}
            ORDER BY l.cod_filial
        """, params)
        filiais_colunas = cur.fetchall()

        cur.execute(f"""
            SELECT
                l.grupo,
                l.conta,
                COALESCE(l.descricao_conta, '') AS descricao,
                l.cod_filial,
                COALESCE(SUM(l.valor), 0) AS total_valor
            FROM lancamentos l
            WHERE l.cod_empresa = %s
              {where_sql}
            GROUP BY l.grupo, l.conta, l.descricao_conta, l.cod_filial
            ORDER BY l.grupo, l.conta, l.cod_filial
        """, params)
        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    codigos_filiais = [f[0] for f in filiais_colunas]
    grupos_dict = {}

    for grupo, conta, descricao, cod_filial, total_valor in rows:
        if grupo not in grupos_dict:
            grupos_dict[grupo] = {}

        chave_conta = (conta, descricao)

        if chave_conta not in grupos_dict[grupo]:
            grupos_dict[grupo][chave_conta] = {
                "grupo": grupo,
                "conta": conta,
                "descricao": descricao,
                "total_geral": 0.0,
                "por_filial": {f: 0.0 for f in codigos_filiais},
                "tipo": "conta",
            }

        valor_float = float(total_valor or 0)
        grupos_dict[grupo][chave_conta]["total_geral"] += valor_float

        if cod_filial in grupos_dict[grupo][chave_conta]["por_filial"]:
            grupos_dict[grupo][chave_conta]["por_filial"][cod_filial] += valor_float

    linhas_matriciais = []

    for grupo in sorted(grupos_dict.keys(), key=lambda x: (x is None, str(x))):
        linhas = list(grupos_dict[grupo].values())
        linhas.sort(key=lambda x: (x["conta"] is None, conta_para_ordenacao(x["conta"])))

        total_grupo = {
            "grupo": grupo,
            "conta": "",
            "descricao": f"Total do Grupo {grupo}",
            "total_geral": 0.0,
            "por_filial": {f: 0.0 for f in codigos_filiais},
            "tipo": "total_grupo",
        }

        for linha in linhas:
            linhas_matriciais.append(linha)
            total_grupo["total_geral"] += float(linha["total_geral"] or 0)

            for filial in codigos_filiais:
                total_grupo["por_filial"][filial] += float(
                    linha["por_filial"].get(filial, 0) or 0
                )

        linhas_matriciais.append(total_grupo)

    return anos, meses, filiais, filiais_colunas, linhas_matriciais