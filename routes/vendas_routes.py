from flask import Blueprint, render_template, session, redirect, url_for, request, flash
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from openpyxl import load_workbook
import tempfile
import os
import re
import uuid
import calendar

from collections import defaultdict
from datetime import date, timedelta, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from zoneinfo import ZoneInfo

vendas_bp = Blueprint("vendas", __name__, url_prefix="/vendas")


# =========================
# BANCO
# =========================
def get_connection():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres.uaafkuovkzkozmscyapw",
        password="DataMatrix@1962#",
        host="aws-1-us-east-1.pooler.supabase.com",
        port=6543,
        sslmode="require"
    )


# =========================
# EXTRAIR PERIODO DO ARQUIVO
# =========================



def extrair_periodo_do_arquivo_diario(ws):
    datas = []

    for row in ws.iter_rows(min_row=1, values_only=True):
        linha = list(row or [])

        while len(linha) < 2:
            linha.append(None)

        col_b = str(linha[1] or "").strip()

        # Exemplo: 01/04/2026 - Quarta
        m = re.match(r"^(\d{2}/\d{2}/\d{4})\s*-\s*.+$", col_b)
        if m:
            try:
                data_lida = datetime.strptime(m.group(1), "%d/%m/%Y").date()
                datas.append(data_lida)
            except Exception:
                pass

    if not datas:
        return None, None

    return min(datas), max(datas)
# =========================
# FUNÇÕES AUXILIARES
# =========================
def para_float(valor):
    if valor is None or valor == "":
        return 0.0

    if isinstance(valor, (int, float)):
        return float(valor)

    texto = str(valor).strip()

    try:
        return float(texto)
    except Exception:
        pass

    texto = texto.replace(".", "").replace(",", ".")
    try:
        return float(texto)
    except Exception:
        return 0.0


def para_decimal(valor):
    if valor is None or valor == "":
        return Decimal("0")

    if isinstance(valor, Decimal):
        return valor

    if isinstance(valor, (int, float)):
        return Decimal(str(valor))

    texto = str(valor).strip()

    try:
        return Decimal(texto)
    except Exception:
        pass

    texto = texto.replace(".", "").replace(",", ".")
    try:
        return Decimal(texto)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def para_data_excel(valor):
    if valor is None or valor == "":
        return None

    if isinstance(valor, datetime):
        return valor.date()

    if isinstance(valor, date):
        return valor

    texto = str(valor).strip()

    for fmt in ("%d/%m/%y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(texto, fmt).date()
        except Exception:
            pass

    return None


def formatar_numero_br(valor, casas=2):
    try:
        numero = float(valor or 0)
    except Exception:
        numero = 0.0

    texto = f"{numero:,.{casas}f}"
    return texto.replace(",", "X").replace(".", ",").replace("X", ".")


def formatar_data_brasil(dt):
    if not dt:
        return ""

    try:
        tz_brasil = ZoneInfo("America/Fortaleza")

        if hasattr(dt, "tzinfo") and dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))

        if hasattr(dt, "astimezone"):
            dt_local = dt.astimezone(tz_brasil)
            return dt_local.strftime("%d/%m/%y")

        return dt.strftime("%d/%m/%y")
    except Exception:
        return dt.strftime("%d/%m/%y")


def cor_excel_51(valor, minimo, maximo):
    try:
        v = float(valor)
        mn = float(minimo)
        mx = float(maximo)
    except Exception:
        return "#ffffff"

    if mx == mn:
        return "#f5da90"

    if mx < mn:
        return "#ffffff"

    ratio = (v - mn) / (mx - mn)
    ratio = max(0, min(1, ratio))

    faixa = int(round(ratio * 50))
    faixa = max(0, min(50, faixa))

    cores = [
        "#f8696b", "#f96d6c", "#f9716d", "#fa756e", "#fa796f",
        "#fb7d70", "#fb8171", "#fc8572", "#fc8973", "#fd8d74",
        "#fd9175", "#fe9576", "#fe9977", "#ef9e78", "#f0a37a",
        "#f0a87c", "#f1ad7e", "#f1b280", "#f2b782", "#f2bc84",
        "#f3c186", "#f3c688", "#f4cb8a", "#f4d08c", "#f5d58e",
        "#f5da90", "#efe08f", "#e9e58e", "#e3ea8d", "#dde68d",
        "#d7e28c", "#d1df8b", "#cbdb8a", "#c5d789", "#bfd489",
        "#b9d088", "#b3cc87", "#add986", "#a7c585", "#a1c184",
        "#9bbe84", "#95ba83", "#8fb682", "#89b281", "#83af80",
        "#7dab80", "#77a77f", "#71a37e", "#6ba07d", "#67c07b",
        "#63be7b"
    ]
    return cores[faixa]


def aplicar_heatmap_na_grade(grade):
    valores = []

    for linha in grade["linhas"]:
        for v in linha["valores"]:
            if v not in (None, 0, 0.0, ""):
                valores.append(float(v))

    if not valores:
        grade["cores_linhas"] = []
        return grade

    minimo = min(valores)
    maximo = max(valores)

    cores_linhas = []
    for linha in grade["linhas"]:
        cores = []
        for v in linha["valores"]:
            if v in (None, 0, 0.0, ""):
                cores.append("")
            else:
                cores.append(cor_excel_51(v, minimo, maximo))
        cores_linhas.append(cores)

    grade["cores_linhas"] = cores_linhas
    return grade


def aplicar_heatmap_na_coluna_total(grade):
    valores = []

    for linha in grade["linhas"]:
        v = linha.get("total")
        if v not in (None, 0, 0.0, ""):
            valores.append(float(v))

    if not valores:
        grade["cores_totais"] = []
        return grade

    minimo = min(valores)
    maximo = max(valores)

    cores_totais = []
    for linha in grade["linhas"]:
        v = linha.get("total")
        if v in (None, 0, 0.0, ""):
            cores_totais.append("")
        else:
            cores_totais.append(cor_excel_51(v, minimo, maximo))

    grade["cores_totais"] = cores_totais
    return grade


def aplicar_heatmap_variacoes(linhas, campos):
    if not linhas:
        return linhas

    for campo in campos:
        valores = []
        for linha in linhas:
            valor = linha.get(campo)
            if valor not in (None, 0, 0.0, ""):
                valores.append(float(valor))

        minimo = min(valores) if valores else None
        maximo = max(valores) if valores else None

        cor_key = f"{campo}_cor"

        for linha in linhas:
            valor = linha.get(campo)
            if minimo is None or valor in (None, 0, 0.0, ""):
                linha[cor_key] = ""
            else:
                linha[cor_key] = cor_excel_51(valor, minimo, maximo)

    return linhas


def aplicar_heatmap_consulta(linhas):
    if not linhas:
        return linhas
    return aplicar_heatmap_variacoes(linhas, ["quantidade", "valor", "mb", "mun"])


def localizar_total_filial(ws, nome_busca):
    nome_busca = (nome_busca or "").strip().upper()
    if not nome_busca:
        return None

    for linha in range(1, 2000):
        valor = ws.cell(row=linha, column=1).value
        texto = str(valor).strip().upper() if valor is not None else ""

        if nome_busca in texto:
            for linha2 in range(linha, min(linha + 100, 2000)):
                valor2 = ws.cell(row=linha2, column=1).value
                texto2 = str(valor2).strip().upper() if valor2 is not None else ""

                if texto2 == "TOTAL FILIAL:":
                    return linha2

    return None


def upsert(cur, tabela, campo, cod_empresa, cod_filial, ano, mes, valor):
    cur.execute(
        f"""
        INSERT INTO {tabela}
        (cod_empresa, cod_filial, ano, mes, data_importacao, {campo})
        VALUES (%s, %s, %s, %s, NOW(), %s)
        ON CONFLICT (cod_empresa, cod_filial, ano, mes)
        DO UPDATE SET
            {campo} = EXCLUDED.{campo},
            data_importacao = NOW()
        """,
        (cod_empresa, cod_filial, ano, mes, valor)
    )


def limpar_importacao_mes(cur, cod_empresa, ano, mes):
    cur.execute("""
        DELETE FROM vendas_unidades_sintetico
        WHERE cod_empresa = %s
          AND ano = %s
          AND mes = %s
    """, (cod_empresa, ano, mes))

    cur.execute("""
        DELETE FROM vendas_valores_sintetico
        WHERE cod_empresa = %s
          AND ano = %s
          AND mes = %s
    """, (cod_empresa, ano, mes))

    cur.execute("""
        DELETE FROM vendas_mb_sintetico
        WHERE cod_empresa = %s
          AND ano = %s
          AND mes = %s
    """, (cod_empresa, ano, mes))


def limpar_importacao_diaria_periodo(cur, conn, cod_empresa, data_ini, data_fim, lote=5000):
    while True:
        cur.execute("""
            DELETE FROM vendas_diarias
            WHERE ctid IN (
                SELECT ctid
                FROM vendas_diarias
                WHERE cod_empresa = %s
                  AND data BETWEEN %s AND %s
                LIMIT %s
            )
        """, (cod_empresa, data_ini, data_fim, lote))

        if cur.rowcount == 0:
            break

        conn.commit()




def inserir_importacao_painel(
    cur,
    cod_empresa,
    ano,
    mes,
    dia_base,
    dias_mes,
    quantidade_proj,
    valor_proj,
    mb_proj
):
    data_brasil = datetime.now(ZoneInfo("America/Sao_Paulo")).date()

    cur.execute("""
        INSERT INTO vendas_painel_importacoes
        (
            cod_empresa,
            ano,
            mes,
            dia_base,
            dias_mes,
            quantidade_proj,
            valor_proj,
            mb_proj,
            data_importacao,
            data_importacao_dia
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
        ON CONFLICT (cod_empresa, data_importacao_dia)
        DO UPDATE SET
            ano = EXCLUDED.ano,
            mes = EXCLUDED.mes,
            dia_base = EXCLUDED.dia_base,
            dias_mes = EXCLUDED.dias_mes,
            quantidade_proj = EXCLUDED.quantidade_proj,
            valor_proj = EXCLUDED.valor_proj,
            mb_proj = EXCLUDED.mb_proj,
            data_importacao = NOW()
    """, (
        cod_empresa,
        ano,
        mes,
        dia_base,
        dias_mes,
        float(quantidade_proj),
        float(valor_proj),
        float(mb_proj),
        data_brasil
    ))


def obter_nome_mes_abrev(mes):
    nomes = {
        1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr",
        5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago",
        9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
    }
    return nomes.get(int(mes), str(mes))


def obter_parametros_padrao_importacao():
    hoje = date.today()

    if hoje.day == 1:
        referencia = hoje - timedelta(days=1)
    else:
        referencia = hoje

    ontem = hoje - timedelta(days=1)

    ano = referencia.year
    mes = referencia.month

    if hoje.day == 1:
        dia_base = referencia.day
    else:
        dia_base = ontem.day

    dias_mes = calendar.monthrange(ano, mes)[1]

    return {
        "ano": ano,
        "mes": mes,
        "dia_base": dia_base,
        "dias_mes": dias_mes
    }


def obter_periodo_padrao_consulta():
    hoje = datetime.now(ZoneInfo("America/Fortaleza")).date()
    data_fim = hoje
    data_ini = hoje - timedelta(days=30)
    return data_ini, data_fim


def obter_filiais_ativas(cur, cod_empresa):
    cur.execute("""
        SELECT cod_filial, nome_filial
        FROM filiais
        WHERE cod_empresa = %s
          AND ativo = TRUE
        ORDER BY cod_filial
    """, (cod_empresa,))
    return cur.fetchall()


def montar_grade_sintetica(filiais, registros, campo_valor):
    meses_ordenados = sorted(
        {(int(r["ano"]), int(r["mes"])) for r in registros},
        key=lambda x: (x[0], x[1])
    )

    meses_ordenados = meses_ordenados[-24:]
    meses_validos = set(meses_ordenados)

    mapa = {}
    for r in registros:
        ano = int(r["ano"])
        mes = int(r["mes"])

        if (ano, mes) not in meses_validos:
            continue

        chave = (ano, mes, int(r["cod_filial"]))
        mapa[chave] = float(r[campo_valor] or 0)

    linhas = []
    totais_por_filial = defaultdict(float)
    serie_por_filial = defaultdict(list)

    for ano, mes in meses_ordenados:
        linha = {
            "periodo": f"{obter_nome_mes_abrev(mes)}/{str(ano)[-2:]}",
            "ano": ano,
            "mes": mes,
            "total": 0.0,
            "valores": []
        }

        for filial in filiais:
            cod_filial = int(filial["cod_filial"])
            valor = mapa.get((ano, mes, cod_filial))
            linha["valores"].append(valor)

            if valor not in (None, 0, 0.0, ""):
                linha["total"] += valor
                totais_por_filial[cod_filial] += valor
                serie_por_filial[cod_filial].append(valor)

        linhas.append(linha)

    total_geral = sum(l["total"] for l in linhas)

    linha_total = {
        "rotulo": "TOTAL",
        "valores": [totais_por_filial[int(f["cod_filial"])] for f in filiais],
        "total": total_geral
    }

    linha_med_12m = {
        "rotulo": "MED 12 M",
        "valores": [],
        "total": 0.0
    }

    for filial in filiais:
        cod_filial = int(filial["cod_filial"])
        serie = serie_por_filial[cod_filial][-12:]
        media = sum(serie) / len(serie) if serie else 0.0
        linha_med_12m["valores"].append(media)

    linha_med_12m["total"] = sum(linha_med_12m["valores"])

    linha_proj_12m = {
        "rotulo": "PROJ 12 M",
        "valores": [v * 12 for v in linha_med_12m["valores"]],
        "total": sum(v * 12 for v in linha_med_12m["valores"])
    }

    return {
        "linhas": linhas,
        "linha_total": linha_total,
        "linha_med_12m": linha_med_12m,
        "linha_proj_12m": linha_proj_12m
    }


def montar_grade_mun(filiais, grade_mb, grade_unidades):
    mapa_mb = {(linha["ano"], linha["mes"]): linha for linha in grade_mb["linhas"]}
    mapa_un = {(linha["ano"], linha["mes"]): linha for linha in grade_unidades["linhas"]}

    chaves = sorted(set(mapa_mb.keys()) | set(mapa_un.keys()))
    linhas = []

    totais_mun_filial = []
    med_mun_filial = []
    proj_mun_filial = []

    for i, _filial in enumerate(filiais):
        total_mb = grade_mb["linha_total"]["valores"][i]
        total_un = grade_unidades["linha_total"]["valores"][i]
        totais_mun_filial.append((total_mb / total_un) if total_un else 0.0)

        med_mb = grade_mb["linha_med_12m"]["valores"][i]
        med_un = grade_unidades["linha_med_12m"]["valores"][i]
        med_mun_filial.append((med_mb / med_un) if med_un else 0.0)

        proj_mb = grade_mb["linha_proj_12m"]["valores"][i]
        proj_un = grade_unidades["linha_proj_12m"]["valores"][i]
        proj_mun_filial.append((proj_mb / proj_un) if proj_un else 0.0)

    for ano, mes in chaves:
        linha_mb = mapa_mb.get((ano, mes))
        linha_un = mapa_un.get((ano, mes))

        valores = []
        total_mb_linha = linha_mb["total"] if linha_mb else None
        total_un_linha = linha_un["total"] if linha_un else None

        for i in range(len(filiais)):
            mb = linha_mb["valores"][i] if linha_mb else None
            un = linha_un["valores"][i] if linha_un else None

            if mb in (None, 0, 0.0, "") or un in (None, 0, 0.0, ""):
                valores.append(None)
            else:
                valores.append(mb / un)

        linhas.append({
            "periodo": f"{obter_nome_mes_abrev(mes)}/{str(ano)[-2:]}",
            "ano": ano,
            "mes": mes,
            "valores": valores,
            "total": (total_mb_linha / total_un_linha)
            if total_mb_linha not in (None, 0, 0.0, "") and total_un_linha not in (None, 0, 0.0, "")
            else None
        })

    return {
        "linhas": linhas,
        "linha_total": {
            "rotulo": "TOTAL",
            "valores": totais_mun_filial,
            "total": (
                grade_mb["linha_total"]["total"] / grade_unidades["linha_total"]["total"]
                if grade_unidades["linha_total"]["total"] else 0.0
            )
        },
        "linha_med_12m": {
            "rotulo": "MED 12 M",
            "valores": med_mun_filial,
            "total": (
                grade_mb["linha_med_12m"]["total"] / grade_unidades["linha_med_12m"]["total"]
                if grade_unidades["linha_med_12m"]["total"] else 0.0
            )
        },
        "linha_proj_12m": {
            "rotulo": "PROJ 12 M",
            "valores": proj_mun_filial,
            "total": (
                grade_mb["linha_proj_12m"]["total"] / grade_unidades["linha_proj_12m"]["total"]
                if grade_unidades["linha_proj_12m"]["total"] else 0.0
            )
        }
    }


def montar_resumo_projecao(grade_unidades, grade_valores, grade_mb, dias_mes=None):
    if not grade_unidades["linhas"]:
        return None

    ultima_qtd = grade_unidades["linhas"][-1]
    ultima_val = grade_valores["linhas"][-1] if grade_valores["linhas"] else None
    ultima_mb = grade_mb["linhas"][-1] if grade_mb["linhas"] else None

    quantidade = float(ultima_qtd.get("total") or 0)
    valor = float(ultima_val.get("total") or 0) if ultima_val else 0.0
    mb = float(ultima_mb.get("total") or 0) if ultima_mb else 0.0

    mun = (mb / quantidade) if quantidade else 0.0
    qtd_dia = (quantidade / dias_mes) if dias_mes else 0.0

    return {
        "titulo": f"PROJEÇÃO {ultima_qtd['periodo'].upper()}",
        "periodo": ultima_qtd["periodo"],
        "quantidade": quantidade,
        "valor": valor,
        "mb": mb,
        "mun": mun,
        "qtd_dia": qtd_dia,
        "dias_mes": dias_mes
    }


def montar_grade_variacoes_projecoes(registros):
    if not registros:
        return []

    linhas = []

    for r in registros:
        quantidade = float(r["quantidade_proj"]) if r.get("quantidade_proj") not in (None, "") else 0.0
        valor = float(r["valor_proj"]) if r.get("valor_proj") not in (None, "") else 0.0
        mb = float(r["mb_proj"]) if r.get("mb_proj") not in (None, "") else 0.0
        mun = (mb / quantidade) if quantidade else 0.0

        data_importacao = r.get("data_importacao")
        data_fmt = formatar_data_brasil(data_importacao)

        linhas.append({
            "data": data_fmt,
            "dia_base": int(r["dia_base"]) if r.get("dia_base") not in (None, "") else 0,
            "dias_mes": int(r["dias_mes"]) if r.get("dias_mes") not in (None, "") else 0,
            "quantidade": quantidade,
            "valor": valor,
            "mb": mb,
            "mun": mun
        })

    linhas = aplicar_heatmap_variacoes(
        linhas,
        ["quantidade", "valor", "mb", "mun"]
    )

    return linhas

def criar_job_importacao(cur, cod_empresa, tipo_importacao):
    job_id = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO importacoes_progresso (
            job_id,
            cod_empresa,
            tipo_importacao,
            status,
            etapa,
            mensagem,
            percentual,
            total_linhas,
            linhas_processadas
        )
        VALUES (%s, %s, %s, 'processando', '', '', 0, 0, 0)
    """, (job_id, cod_empresa, tipo_importacao))
    return job_id


def atualizar_job_importacao(
    cur,
    job_id,
    status=None,
    etapa=None,
    mensagem=None,
    percentual=None,
    total_linhas=None,
    linhas_processadas=None
):
    sets = ["atualizado_em = NOW()"]
    params = []

    if status is not None:
        sets.append("status = %s")
        params.append(status)

    if etapa is not None:
        sets.append("etapa = %s")
        params.append(etapa)

    if mensagem is not None:
        sets.append("mensagem = %s")
        params.append(mensagem)

    if percentual is not None:
        sets.append("percentual = %s")
        params.append(int(percentual))

    if total_linhas is not None:
        sets.append("total_linhas = %s")
        params.append(int(total_linhas))

    if linhas_processadas is not None:
        sets.append("linhas_processadas = %s")
        params.append(int(linhas_processadas))

    params.append(job_id)

    cur.execute(f"""
        UPDATE importacoes_progresso
           SET {", ".join(sets)}
         WHERE job_id = %s
    """, params)

# =========================
# PAINEL
# =========================
@vendas_bp.route("/painel")
def vendas_painel():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.escolher_empresa"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = TRUE
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()

        cur.execute("""
            SELECT cod_empresa, cod_filial, ano, mes, quantidade_vendida
            FROM vendas_unidades_sintetico
            WHERE cod_empresa = %s
            ORDER BY ano, mes, cod_filial
        """, (cod_empresa,))
        registros_unidades = cur.fetchall()

        cur.execute("""
            SELECT cod_empresa, cod_filial, ano, mes, valor_vendido
            FROM vendas_valores_sintetico
            WHERE cod_empresa = %s
            ORDER BY ano, mes, cod_filial
        """, (cod_empresa,))
        registros_valores = cur.fetchall()

        cur.execute("""
            SELECT cod_empresa, cod_filial, ano, mes, margem_bruta
            FROM vendas_mb_sintetico
            WHERE cod_empresa = %s
            ORDER BY ano, mes, cod_filial
        """, (cod_empresa,))
        registros_mb = cur.fetchall()

        cur.execute("""
            SELECT ano, mes, dia_base, dias_mes, data_importacao
            FROM vendas_painel_importacoes
            WHERE cod_empresa = %s
            ORDER BY data_importacao DESC
            LIMIT 1
        """, (cod_empresa,))
        meta_importacao = cur.fetchone()

        cur.execute("""
            SELECT
                ano,
                mes,
                dia_base,
                dias_mes,
                quantidade_proj,
                valor_proj,
                mb_proj,
                data_importacao
            FROM vendas_painel_importacoes
            WHERE cod_empresa = %s
            ORDER BY data_importacao DESC
            LIMIT 30
        """, (cod_empresa,))
        registros_variacoes = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    grade_unidades = montar_grade_sintetica(filiais, registros_unidades, "quantidade_vendida")
    grade_valores = montar_grade_sintetica(filiais, registros_valores, "valor_vendido")
    grade_mb = montar_grade_sintetica(filiais, registros_mb, "margem_bruta")
    grade_mun = montar_grade_mun(filiais, grade_mb, grade_unidades)

    grade_unidades = aplicar_heatmap_na_grade(grade_unidades)
    grade_valores = aplicar_heatmap_na_grade(grade_valores)
    grade_mb = aplicar_heatmap_na_grade(grade_mb)
    grade_mun = aplicar_heatmap_na_grade(grade_mun)

    grade_unidades = aplicar_heatmap_na_coluna_total(grade_unidades)
    grade_valores = aplicar_heatmap_na_coluna_total(grade_valores)
    grade_mb = aplicar_heatmap_na_coluna_total(grade_mb)
    grade_mun = aplicar_heatmap_na_coluna_total(grade_mun)

    dias_mes_resumo = meta_importacao["dias_mes"] if meta_importacao else None

    resumo_projecao = montar_resumo_projecao(
        grade_unidades,
        grade_valores,
        grade_mb,
        dias_mes=dias_mes_resumo
    )

    variacoes_projecoes = montar_grade_variacoes_projecoes(
        list(reversed(registros_variacoes))
    )

    return render_template(
        "vendas_painel.html",
        nome_empresa=nome_empresa,
        filiais=filiais,
        grade_unidades=grade_unidades,
        grade_valores=grade_valores,
        grade_mb=grade_mb,
        grade_mun=grade_mun,
        resumo_projecao=resumo_projecao,
        variacoes_projecoes=variacoes_projecoes,
        formatar_numero_br=formatar_numero_br,
        url_voltar=url_for("sistema.menu_vendas"),
        texto_voltar="← Voltar"
    )


# =========================
# IMPORTAÇÃO DO PAINEL
# =========================
@vendas_bp.route("/painel/importar", methods=["GET", "POST"])
def vendas_importar_painel():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")
    padrao = obter_parametros_padrao_importacao()

    return render_template(
        "vendas_importar_painel.html",
        nome_empresa=nome_empresa,
        ano_sugerido=padrao["ano"],
        mes_sugerido=padrao["mes"],
        dia_base_sugerido=padrao["dia_base"],
        dias_mes_sugerido=padrao["dias_mes"],

        # 👇 ADICIONAR ISSO
        url_voltar=url_for("vendas.vendas_painel"),
        texto_voltar="← Voltar para Painel"
    )
    
    arquivo = request.files.get("arquivo")
    ano_txt = (request.form.get("ano") or "").strip()
    mes_txt = (request.form.get("mes") or "").strip()
    dia_base_txt = (request.form.get("dia_base") or "").strip()
    dias_mes_txt = (request.form.get("dias_mes") or "").strip()

    if not arquivo or arquivo.filename == "":
        flash("Selecione um arquivo.", "error")
        return render_template(
            "vendas_importar_painel.html",
            nome_empresa=nome_empresa,
            ano_sugerido=padrao["ano"],
            mes_sugerido=padrao["mes"],
            dia_base_sugerido=padrao["dia_base"],
            dias_mes_sugerido=padrao["dias_mes"]
        )

    if not (ano_txt.isdigit() and mes_txt.isdigit() and dia_base_txt.isdigit() and dias_mes_txt.isdigit()):
        flash("Informe ano, mês, dia base e dias do mês válidos.", "error")
        return render_template(
            "vendas_importar_painel.html",
            nome_empresa=nome_empresa,
            ano_sugerido=padrao["ano"],
            mes_sugerido=padrao["mes"],
            dia_base_sugerido=padrao["dia_base"],
            dias_mes_sugerido=padrao["dias_mes"]
        )

    ano = int(ano_txt)
    mes = int(mes_txt)
    dia_base = int(dia_base_txt)
    dias_mes = int(dias_mes_txt)

    if mes < 1 or mes > 12:
        flash("O mês deve estar entre 1 e 12.", "error")
        return render_template(
            "vendas_importar_painel.html",
            nome_empresa=nome_empresa,
            ano_sugerido=padrao["ano"],
            mes_sugerido=padrao["mes"],
            dia_base_sugerido=padrao["dia_base"],
            dias_mes_sugerido=padrao["dias_mes"]
        )

    ultimo_dia_mes = calendar.monthrange(ano, mes)[1]

    if dia_base < 1 or dia_base > ultimo_dia_mes:
        flash(f"O dia base deve estar entre 1 e {ultimo_dia_mes}.", "error")
        return render_template(
            "vendas_importar_painel.html",
            nome_empresa=nome_empresa,
            ano_sugerido=ano,
            mes_sugerido=mes,
            dia_base_sugerido=dia_base,
            dias_mes_sugerido=dias_mes
        )

    if dias_mes < 1 or dias_mes > 31:
        flash("Dias do mês inválido.", "error")
        return render_template(
            "vendas_importar_painel.html",
            nome_empresa=nome_empresa,
            ano_sugerido=ano,
            mes_sugerido=mes,
            dia_base_sugerido=dia_base,
            dias_mes_sugerido=dias_mes
        )

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    tmp = None

    try:
        cur.execute("""
            SELECT cod_filial, nome_filial_importacao
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = TRUE
        """, (cod_empresa,))
        filiais = cur.fetchall()

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        arquivo.save(tmp.name)
        tmp.close()

        wb = load_workbook(tmp.name, data_only=True)
        ws = wb.worksheets[0]

        cur2 = conn.cursor()

        limpar_importacao_mes(cur2, cod_empresa, ano, mes)

        importadas = 0

        total_unidades_lidas = Decimal("0")
        total_valor_lido = Decimal("0")
        total_mb_lido = Decimal("0")

        total_unidades_proj = Decimal("0")
        total_valor_proj = Decimal("0")
        total_mb_proj = Decimal("0")

        for f in filiais:
            cod_filial = f["cod_filial"]
            nome_busca = f["nome_filial_importacao"]

            if not nome_busca:
                continue

            linha = localizar_total_filial(ws, nome_busca)
            if not linha:
                continue

            unidades_lidas = para_decimal(ws.cell(row=linha, column=7).value)
            valor_lido = para_decimal(ws.cell(row=linha, column=9).value)
            mb_lido = para_decimal(ws.cell(row=linha, column=11).value)

            fator_proj = (
                Decimal(str(dias_mes)) / Decimal(str(dia_base))
                if dia_base > 0 else Decimal("1")
            )

            unidades_proj = (unidades_lidas * fator_proj).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            valor_proj = (valor_lido * fator_proj).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            mb_proj = (mb_lido * fator_proj).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            total_unidades_lidas += unidades_lidas
            total_valor_lido += valor_lido
            total_mb_lido += mb_lido

            total_unidades_proj += unidades_proj
            total_valor_proj += valor_proj
            total_mb_proj += mb_proj

            upsert(
                cur2,
                "vendas_unidades_sintetico",
                "quantidade_vendida",
                cod_empresa,
                cod_filial,
                ano,
                mes,
                float(unidades_proj)
            )

            upsert(
                cur2,
                "vendas_valores_sintetico",
                "valor_vendido",
                cod_empresa,
                cod_filial,
                ano,
                mes,
                float(valor_proj)
            )

            upsert(
                cur2,
                "vendas_mb_sintetico",
                "margem_bruta",
                cod_empresa,
                cod_filial,
                ano,
                mes,
                float(mb_proj)
            )

            importadas += 1

        inserir_importacao_painel(
            cur2,
            cod_empresa,
            ano,
            mes,
            dia_base,
            dias_mes,
            total_unidades_proj,
            total_valor_proj,
            total_mb_proj
        )

        flash(
            "Conferência da importação — "
            f"Qtd lida: {formatar_numero_br(float(total_unidades_lidas))} | "
            f"Vlr lido: {formatar_numero_br(float(total_valor_lido))} | "
            f"MB lida: {formatar_numero_br(float(total_mb_lido))} | "
            f"Qtd proj: {formatar_numero_br(float(total_unidades_proj))} | "
            f"Vlr proj: {formatar_numero_br(float(total_valor_proj))} | "
            f"MB proj: {formatar_numero_br(float(total_mb_proj))}",
            "warning"
        )

        conn.commit()
        cur2.close()

        flash(
            f"{importadas} filiais importadas com sucesso. "
            f"Projeção aplicada: dia base {dia_base} / dias do mês {dias_mes}.",
            "success"
        )

    except Exception as e:
        conn.rollback()
        flash(f"Erro: {e}", "error")

    finally:
        cur.close()
        conn.close()
        if tmp is not None and os.path.exists(tmp.name):
            os.unlink(tmp.name)

    return render_template(
        "vendas_importar_painel.html",
        nome_empresa=nome_empresa,
        ano_sugerido=ano,
        mes_sugerido=mes,
        dia_base_sugerido=dia_base,
        dias_mes_sugerido=dias_mes
    )


# =========================
# VENDAS DIÁRIAS
# =========================
@vendas_bp.route("/diarias")
def vendas_diarias():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    data_ini_padrao, data_fim_padrao = obter_periodo_padrao_consulta()

    data_ini_txt = (request.args.get("data_ini") or "").strip()
    data_fim_txt = (request.args.get("data_fim") or "").strip()

    data_ini = para_data_excel(data_ini_txt) if data_ini_txt else data_ini_padrao
    data_fim = para_data_excel(data_fim_txt) if data_fim_txt else data_fim_padrao

    if not data_ini:
        data_ini = data_ini_padrao
    if not data_fim:
        data_fim = data_fim_padrao

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT
                data,
                dia_semana,
                SUM(quantidade) AS quantidade,
                SUM(valor) AS valor,
                SUM(margem_bruta) AS mb
            FROM vendas_diarias
            WHERE cod_empresa = %s
              AND data BETWEEN %s AND %s
            GROUP BY data, dia_semana
            ORDER BY data ASC
        """, (cod_empresa, data_ini, data_fim))
        linhas_totais = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    for linha in linhas_totais:
        quantidade = float(linha["quantidade"] or 0)
        valor = float(linha["valor"] or 0)
        mb = float(linha["mb"] or 0)

        linha["quantidade"] = quantidade
        linha["valor"] = valor
        linha["mb"] = mb
        linha["mun"] = (mb / quantidade) if quantidade else 0.0
        linha["data_fmt"] = formatar_data_brasil(linha["data"])

    linhas_totais = aplicar_heatmap_consulta(linhas_totais)

    return render_template(
        "vendas_diarias.html",
        nome_empresa=nome_empresa,
        data_ini=data_ini.strftime("%Y-%m-%d"),
        data_fim=data_fim.strftime("%Y-%m-%d"),
        linhas_totais=linhas_totais,
        formatar_numero_br=formatar_numero_br,
        url_voltar=url_for("sistema.menu_vendas"),
        texto_voltar="← Voltar"
    )
# =========================
# VENDAS DIÁRIAS IMPORTAR
# =========================
@vendas_bp.route("/diarias/importar", methods=["GET", "POST"])
def vendas_importar_diarias():

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    if request.method == "GET":
        return render_template(
            "vendas_importar_diarias.html",
            nome_empresa=nome_empresa,
            job_id=None
        )

    arquivo = request.files.get("arquivo")

    if not arquivo or arquivo.filename == "":
        flash("Selecione um arquivo.", "error")
        return render_template(
            "vendas_importar_diarias.html",
            nome_empresa=nome_empresa,
            job_id=None
        )

    conn = get_connection()
    cur = conn.cursor()
    tmp = None
    wb = None
    job_id = None

    try:
        job_id = criar_job_importacao(cur, cod_empresa, "vendas_diarias")
        conn.commit()

        atualizar_job_importacao(
            cur,
            job_id,
            etapa="upload",
            mensagem="Recebendo arquivo...",
            percentual=5
        )
        conn.commit()

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        arquivo.save(tmp.name)
        tmp.close()

        atualizar_job_importacao(
            cur,
            job_id,
            etapa="leitura",
            mensagem="Abrindo planilha...",
            percentual=15
        )
        conn.commit()

        wb = load_workbook(tmp.name, data_only=True, read_only=True)
        ws = wb.worksheets[0]
        ws.reset_dimensions()

        cur.execute("""
            SELECT cod_filial, nome_filial_importacao
            FROM filiais
            WHERE cod_empresa = %s
        """, (cod_empresa,))
        filiais_db = cur.fetchall()

        mapa_filiais = {}
        for cod_filial, nome_importacao in filiais_db:
            if nome_importacao:
                mapa_filiais[str(nome_importacao).strip().upper()] = int(cod_filial)

        dados = []
        linhas_ignoradas = 0
        filial_atual = None
        data_atual = None
        dia_semana_atual = ""
        data_ini = None
        data_fim = None
        linhas_lidas = 0

        for row in ws.iter_rows(min_row=1, values_only=True):
            linhas_lidas += 1

            if linhas_lidas % 500 == 0:
                percentual_leitura = min(55, 20 + (linhas_lidas // 500))
                atualizar_job_importacao(
                    cur,
                    job_id,
                    etapa="leitura",
                    mensagem=f"Lendo planilha... {linhas_lidas} linhas analisadas",
                    percentual=percentual_leitura,
                    linhas_processadas=linhas_lidas
                )
                conn.commit()

            linha = list(row or [])

            while len(linha) < 11:
                linha.append(None)

            col_a = str(linha[0] or "").strip()
            col_b = str(linha[1] or "").strip()
            col_d = str(linha[3] or "").strip()

            if col_a.upper().startswith("FILIAL:"):
                nome_filial_planilha = col_a.split(":", 1)[1].strip().upper()
                filial_atual = mapa_filiais.get(nome_filial_planilha)
                data_atual = None
                dia_semana_atual = ""
                continue

            texto_a_norm = re.sub(r"\s+", "", col_a.upper())

            if texto_a_norm in ("DATA", "DATA:"):
                texto_data = col_b
                m = re.search(r"(\d{2}/\d{2}/\d{4})", texto_data)

                if m:
                    try:
                        data_atual = datetime.strptime(m.group(1), "%d/%m/%Y").date()

                        parte_dia = texto_data.replace(m.group(1), "").strip()
                        parte_dia = parte_dia.lstrip("-").strip()
                        dia_semana_atual = parte_dia

                        if data_ini is None or data_atual < data_ini:
                            data_ini = data_atual

                        if data_fim is None or data_atual > data_fim:
                            data_fim = data_atual

                    except Exception:
                        data_atual = None
                        dia_semana_atual = ""
                else:
                    data_atual = None
                    dia_semana_atual = ""

                continue

            if filial_atual is None or data_atual is None:
                continue

            if col_d.upper().startswith("SUBTOTAL"):
                continue

            codigo = col_a
            descricao = col_b

            if not codigo or not descricao:
                continue

            if codigo.upper() in ("DATA:", "FILIAL:", "DATA", "FILIAL"):
                continue

            custo = para_float(linha[2])
            preco_venda = para_float(linha[4])
            quantidade = para_float(linha[7])
            valor = para_float(linha[9])
            margem_bruta = para_float(linha[10])

            if quantidade == 0 and valor == 0 and margem_bruta == 0:
                linhas_ignoradas += 1
                continue

            dados.append((
                cod_empresa,
                filial_atual,
                data_atual,
                dia_semana_atual,
                codigo,
                descricao,
                custo,
                preco_venda,
                quantidade,
                valor,
                margem_bruta
            ))

        if not data_ini or not data_fim:
            atualizar_job_importacao(
                cur,
                job_id,
                status="erro",
                etapa="erro",
                mensagem="Não foi possível identificar o período no arquivo.",
                percentual=100
            )
            conn.commit()

            flash("Não foi possível identificar o período no arquivo.", "error")
            return render_template(
                "vendas_importar_diarias.html",
                nome_empresa=nome_empresa,
                job_id=job_id
            )

        if not dados:
            atualizar_job_importacao(
                cur,
                job_id,
                status="erro",
                etapa="erro",
                mensagem="Nenhum dado válido encontrado no arquivo.",
                percentual=100
            )
            conn.commit()

            flash("Nenhum dado válido encontrado no arquivo.", "error")
            return render_template(
                "vendas_importar_diarias.html",
                nome_empresa=nome_empresa,
                job_id=job_id
            )

        atualizar_job_importacao(
            cur,
            job_id,
            etapa="limpeza",
            mensagem="Limpando dados antigos do período...",
            percentual=60,
            total_linhas=len(dados),
            linhas_processadas=0
        )
        conn.commit()

        limpar_importacao_diaria_periodo(cur, conn, cod_empresa, data_ini, data_fim, lote=5000)

        atualizar_job_importacao(
            cur,
            job_id,
            etapa="insercao",
            mensagem="Gravando novos dados...",
            percentual=70,
            total_linhas=len(dados),
            linhas_processadas=0
        )
        conn.commit()

        lote = 5000
        sql_insert = """
            INSERT INTO vendas_diarias (
                cod_empresa,
                cod_filial,
                data,
                dia_semana,
                codigo_produto,
                descricao,
                custo,
                preco_venda,
                quantidade,
                valor,
                margem_bruta
            )
            VALUES %s
        """

        for i in range(0, len(dados), lote):
            bloco = dados[i:i + lote]

            execute_values(
                cur,
                sql_insert,
                bloco,
                page_size=lote
            )

            processados = min(i + len(bloco), len(dados))
            percentual = 70 + int((processados / len(dados)) * 25)

            atualizar_job_importacao(
                cur,
                job_id,
                etapa="insercao",
                mensagem=f"Gravando novos dados... {processados} de {len(dados)}",
                percentual=min(percentual, 95),
                total_linhas=len(dados),
                linhas_processadas=processados
            )

            conn.commit()

        atualizar_job_importacao(
            cur,
            job_id,
            status="concluido",
            etapa="finalizado",
            mensagem="Importação concluída com sucesso.",
            percentual=100,
            total_linhas=len(dados),
            linhas_processadas=len(dados)
        )
        conn.commit()

        msg = (
            f"{len(dados)} registros importados com sucesso. "
            f"Período identificado no arquivo: {data_ini.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}."
        )
        if linhas_ignoradas > 0:
            msg += f" {linhas_ignoradas} linhas foram ignoradas."

        flash(msg, "success")

    except Exception as e:
        conn.rollback()

        try:
            if job_id:
                atualizar_job_importacao(
                    cur,
                    job_id,
                    status="erro",
                    etapa="erro",
                    mensagem=str(e),
                    percentual=100
                )
                conn.commit()
        except Exception:
            pass

        flash(f"Erro: {e}", "error")

    finally:
        if wb is not None:
            try:
                wb.close()
            except Exception:
                pass

        cur.close()
        conn.close()

        if tmp is not None and os.path.exists(tmp.name):
            os.unlink(tmp.name)

    return render_template(
        "vendas_importar_diarias.html",
        nome_empresa=nome_empresa,
        job_id=job_id
    )

# =========================
# CONSULTAS
# =========================
@vendas_bp.route("/consultas")
def vendas_consultas():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    data_ini_padrao, data_fim_padrao = obter_periodo_padrao_consulta()

    data_ini_txt = (request.args.get("data_ini") or "").strip()
    data_fim_txt = (request.args.get("data_fim") or "").strip()
    cod_filial_sel = (request.args.get("cod_filial") or "").strip()

    data_ini = para_data_excel(data_ini_txt) if data_ini_txt else data_ini_padrao
    data_fim = para_data_excel(data_fim_txt) if data_fim_txt else data_fim_padrao

    if not data_ini:
        data_ini = data_ini_padrao
    if not data_fim:
        data_fim = data_fim_padrao

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    linhas_totais = []
    linhas_filial = []
    filial_atual = None
    filial_anterior = None
    filial_proxima = None

    try:
        filiais = obter_filiais_ativas(cur, cod_empresa)

        if cod_filial_sel:
            try:
                cod_filial_int = int(cod_filial_sel)
            except Exception:
                cod_filial_int = None

            if cod_filial_int is not None:
                cur.execute("""
                    SELECT
                        data,
                        dia_semana,
                        SUM(quantidade) AS quantidade,
                        SUM(valor) AS valor,
                        SUM(margem_bruta) AS mb
                    FROM vendas_diarias
                    WHERE cod_empresa = %s
                      AND cod_filial = %s
                      AND data BETWEEN %s AND %s
                    GROUP BY data, dia_semana
                    ORDER BY data ASC
                """, (cod_empresa, cod_filial_int, data_ini, data_fim))
                linhas_filial = cur.fetchall()

                idx = next((i for i, f in enumerate(filiais) if int(f["cod_filial"]) == cod_filial_int), None)
                if idx is not None:
                    filial_atual = filiais[idx]
                    if idx > 0:
                        filial_anterior = filiais[idx - 1]
                    if idx < len(filiais) - 1:
                        filial_proxima = filiais[idx + 1]
        else:
            cur.execute("""
                SELECT
                    data,
                    dia_semana,
                    SUM(quantidade) AS quantidade,
                    SUM(valor) AS valor,
                    SUM(margem_bruta) AS mb
                FROM vendas_diarias
                WHERE cod_empresa = %s
                  AND data BETWEEN %s AND %s
                GROUP BY data, dia_semana
                ORDER BY data ASC
            """, (cod_empresa, data_ini, data_fim))
            linhas_totais = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    for linha in linhas_totais:
        quantidade = float(linha["quantidade"] or 0)
        valor = float(linha["valor"] or 0)
        mb = float(linha["mb"] or 0)

        linha["quantidade"] = quantidade
        linha["valor"] = valor
        linha["mb"] = mb
        linha["mun"] = (mb / quantidade) if quantidade else 0.0
        linha["data_fmt"] = formatar_data_brasil(linha["data"])

    for linha in linhas_filial:
        quantidade = float(linha["quantidade"] or 0)
        valor = float(linha["valor"] or 0)
        mb = float(linha["mb"] or 0)

        linha["quantidade"] = quantidade
        linha["valor"] = valor
        linha["mb"] = mb
        linha["mun"] = (mb / quantidade) if quantidade else 0.0
        linha["data_fmt"] = formatar_data_brasil(linha["data"])

    linhas_totais = aplicar_heatmap_consulta(linhas_totais)
    linhas_filial = aplicar_heatmap_consulta(linhas_filial)

    return render_template(
        "vendas_consultas.html",
        nome_empresa=nome_empresa,
        filiais=filiais,
        linhas_totais=linhas_totais,
        linhas_filial=linhas_filial,
        filial_atual=filial_atual,
        filial_anterior=filial_anterior,
        filial_proxima=filial_proxima,
        cod_filial_sel=cod_filial_sel,
        data_ini=data_ini.strftime("%Y-%m-%d"),
        data_fim=data_fim.strftime("%Y-%m-%d"),
        formatar_numero_br=formatar_numero_br,
        url_voltar=url_for("vendas.vendas_diarias"),
        texto_voltar="← Voltar"
    )


@vendas_bp.route("/diarias/importar/progresso/<job_id>")
def vendas_importar_diarias_progresso(job_id):
    if "cod_empresa" not in session:
        return {"ok": False, "erro": "sessao_expirada"}, 401

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT
                job_id,
                cod_empresa,
                tipo_importacao,
                status,
                etapa,
                mensagem,
                percentual,
                total_linhas,
                linhas_processadas
            FROM importacoes_progresso
            WHERE job_id = %s
              AND cod_empresa = %s
        """, (job_id, cod_empresa))

        row = cur.fetchone()

        if not row:
            return {"ok": False, "erro": "job_nao_encontrado"}, 404

        return {
            "ok": True,
            "job": {
                "job_id": row["job_id"],
                "cod_empresa": row["cod_empresa"],
                "tipo_importacao": row["tipo_importacao"],
                "status": row["status"],
                "etapa": row["etapa"],
                "mensagem": row["mensagem"],
                "percentual": row["percentual"],
                "total_linhas": row["total_linhas"],
                "linhas_processadas": row["linhas_processadas"],
            }
        }

    finally:
        cur.close()
        conn.close()

@vendas_bp.route("/consulta_produto")
def vendas_consulta_produto():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    hoje = datetime.now().date()
    data_ini_padrao = date(hoje.year, hoje.month, 1)

    cod_filial_sel = (request.args.get("cod_filial") or "").strip()
    data_ini_txt = (request.args.get("data_ini") or "").strip()
    produto_sel = (request.args.get("produto") or "").strip()

    data_ini = para_data_excel(data_ini_txt) if data_ini_txt else data_ini_padrao
    if not data_ini:
        data_ini = data_ini_padrao

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    linhas = []
    filiais = []
    produtos = []

    try:
        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = TRUE
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()

        cur.execute("""
            SELECT DISTINCT descricao
            FROM vendas_diarias
            WHERE cod_empresa = %s
              AND COALESCE(TRIM(descricao), '') <> ''
            ORDER BY descricao
        """, (cod_empresa,))
        produtos = [r["descricao"] for r in cur.fetchall()]

        where = ["cod_empresa = %s", "data >= %s"]
        params = [cod_empresa, data_ini]

        if cod_filial_sel:
            where.append("CAST(cod_filial AS TEXT) = %s")
            params.append(cod_filial_sel)

        if produto_sel:
            where.append("descricao = %s")
            params.append(produto_sel)

        where_sql = " AND ".join(where)

        cur.execute(f"""
            SELECT
                codigo_produto,
                cod_filial,
                data,
                dia_semana,
                descricao,
                custo,
                preco_venda,
                quantidade,
                valor,
                margem_bruta
            FROM vendas_diarias
            WHERE {where_sql}
            ORDER BY data ASC, cod_filial ASC, descricao ASC
        """, params)

        linhas = cur.fetchall()

        mapa_filiais = {int(f["cod_filial"]): f["nome_filial"] for f in filiais}

        for linha in linhas:
            qtd = float(linha["quantidade"] or 0)
            valor = float(linha["valor"] or 0)
            mb = float(linha["margem_bruta"] or 0)
            custo = float(linha["custo"] or 0)
            preco = float(linha["preco_venda"] or 0)

            linha["quantidade"] = qtd
            linha["valor"] = valor
            linha["margem_bruta"] = mb
            linha["custo"] = custo
            linha["preco_venda"] = preco
            linha["mun"] = (mb / qtd) if qtd else 0.0
            linha["data_fmt"] = formatar_data_brasil(linha["data"])
            linha["nome_filial"] = mapa_filiais.get(int(linha["cod_filial"]), str(linha["cod_filial"]))

    finally:
        cur.close()
        conn.close()

    linhas = aplicar_heatmap_variacoes(
        linhas,
        ["custo", "preco_venda", "mun", "quantidade", "valor", "margem_bruta"]
    )

    return render_template(
        "vendas_consulta_produto.html",
        nome_empresa=nome_empresa,
        filiais=filiais,
        produtos=produtos,
        linhas=linhas,
        cod_filial_sel=cod_filial_sel,
        data_ini=data_ini.strftime("%Y-%m-%d"),
        produto_sel=produto_sel,
        formatar_numero_br=formatar_numero_br,
        url_voltar=url_for("vendas.vendas_consultas"),
        texto_voltar="← Voltar"
    )


# =========================
# CADASTROS - FILIAIS
# =========================
@vendas_bp.route("/cadastros/filiais")
def vendas_filiais():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))
    return render_template("vendas_filiais.html")