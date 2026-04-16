from decimal import Decimal
from db import get_connection
from utils.helpers import cor
from utils.formatters import MESES

def montar_dashboard(cod_empresa):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT ano, mes
        FROM lancamentos
        WHERE cod_empresa = %s
          AND ano IS NOT NULL
          AND mes IS NOT NULL
        ORDER BY ano DESC, mes DESC
        LIMIT 1
    """, (cod_empresa,))
    ultimo = cur.fetchone()

    if not ultimo:
        cur.close()
        conn.close()
        return [], {
            "lanc": 0,
            "op": 0,
            "enop": 0,
            "prod": 0,
            "desp": 0,
            "snop": 0,
            "divi": 0,
            "transfe": 0,
            "total": 0,
        }

    ano, mes = int(ultimo[0]), int(ultimo[1])

    janela = []
    for i in range(11, -1, -1):
        m = mes - i
        a = ano
        while m <= 0:
            m += 12
            a -= 1
        janela.append((a, m))

    cur.execute("""
        SELECT
            ano,
            mes,
            COUNT(*) AS lanc,
            COALESCE(SUM(CASE WHEN CAST(grupo AS text) = '1' THEN valor ELSE 0 END), 0) AS op,
            COALESCE(SUM(CASE WHEN CAST(grupo AS text) = '2' THEN valor ELSE 0 END), 0) AS enop,
            COALESCE(SUM(CASE WHEN CAST(grupo AS text) = '3' THEN valor ELSE 0 END), 0) AS prod,
            COALESCE(SUM(CASE WHEN CAST(grupo AS text) = '4' THEN valor ELSE 0 END), 0) AS desp,
            COALESCE(SUM(CASE WHEN CAST(grupo AS text) = '5' THEN valor ELSE 0 END), 0) AS snop,
            COALESCE(SUM(CASE WHEN CAST(grupo AS text) = '6' THEN valor ELSE 0 END), 0) AS divi,
            COALESCE(SUM(CASE WHEN CAST(grupo AS text) = '7' THEN valor ELSE 0 END), 0) AS transfe
        FROM lancamentos
        WHERE cod_empresa = %s
        GROUP BY ano, mes
    """, (cod_empresa,))

    dados = {}
    for r in cur.fetchall():
        dados[(int(r[0]), int(r[1]))] = r

    cur.close()
    conn.close()

    linhas = []
    desp_vals = []
    snop_vals = []

    for a, m in janela:
        r = dados.get((a, m))

        if r:
            _, _, lanc, op, enop, prod, desp, snop, divi, transfe = r
        else:
            lanc = 0
            op = enop = prod = desp = snop = divi = transfe = Decimal("0")

        desp_vals.append(float(desp or 0))
        snop_vals.append(float(snop or 0))

        total = op + enop + prod + desp + snop + divi + transfe

        linhas.append({
            "ano": a,
            "mes": MESES.get(m, str(m)),
            "lanc": lanc,
            "op": op,
            "enop": enop,
            "prod": prod,
            "desp": desp,
            "snop": snop,
            "divi": divi,
            "transfe": transfe,
            "total": total,
        })

    min_desp = min(desp_vals) if desp_vals else 0
    max_desp = max(desp_vals) if desp_vals else 0
    min_snop = min(snop_vals) if snop_vals else 0
    max_snop = max(snop_vals) if snop_vals else 0

    for l in linhas:
        l["estilo_desp"] = cor(l["desp"], min_desp, max_desp)
        l["estilo_snop"] = cor(l["snop"], min_snop, max_snop)

    totais = {
        "lanc": sum(int(l["lanc"] or 0) for l in linhas),
        "op": sum(float(l["op"] or 0) for l in linhas),
        "enop": sum(float(l["enop"] or 0) for l in linhas),
        "prod": sum(float(l["prod"] or 0) for l in linhas),
        "desp": sum(float(l["desp"] or 0) for l in linhas),
        "snop": sum(float(l["snop"] or 0) for l in linhas),
        "divi": sum(float(l["divi"] or 0) for l in linhas),
        "transfe": sum(float(l["transfe"] or 0) for l in linhas),
        "total": sum(float(l["total"] or 0) for l in linhas),
    }

    return linhas, totais