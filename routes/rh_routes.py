from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from db import get_connection
from security_helpers import (
    permissao_obrigatoria,
    usuario_tem_permissao,
)

rh_bp = Blueprint("rh", __name__)


# ------------------------------------------
# MENU RH
# ------------------------------------------
@rh_bp.route("/menu")
@permissao_obrigatoria("RH", "MENU")
def menu_rh():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        permissoes = {
            "pode_avaliacoes": True,
            "pode_importar_abastecimentos": True,
            "pode_consultar_abastecimentos": True,
            "pode_configuracoes_rh": True,
        }
    else:
        permissoes = {
            "pode_avaliacoes": usuario_tem_permissao(
                id_usuario, cod_empresa, "RH", "AVALIACOES"
            ),
            "pode_importar_abastecimentos": usuario_tem_permissao(
                id_usuario, cod_empresa, "RH", "IMPORTAR_ABASTECIMENTOS"
            ),
            "pode_consultar_abastecimentos": usuario_tem_permissao(
                id_usuario, cod_empresa, "RH", "CONSULTAR_ABASTECIMENTOS"
            ),
            "pode_configuracoes_rh": usuario_tem_permissao(
                id_usuario, cod_empresa, "RH", "CONFIGURACOES_RH"
            ),
        }

    return render_template(
        "menu_rh.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        url_voltar=url_for("sistema.selecionar_sistema"),
        texto_voltar="← Voltar",
        **permissoes
    )


# ------------------------------------------
# AVALIAÇÕES
# ------------------------------------------
@rh_bp.route("/avaliacoes")
@permissao_obrigatoria(
    "RH",
    "AVALIACOES",
    redirecionar_para="rh.menu_rh",
)
def avaliacoes():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    return render_template(
        "rh_avaliacoes.html",
        cod_empresa=session.get("cod_empresa", ""),
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("rh.menu_rh"),
        texto_voltar="← Voltar",
    )


# ------------------------------------------
# IMPORTAR ABASTECIMENTOS
# ------------------------------------------
@rh_bp.route("/abastecimentos/importar", methods=["GET", "POST"])
@permissao_obrigatoria(
    "RH",
    "IMPORTAR_ABASTECIMENTOS",
    redirecionar_para="rh.menu_rh",
)
def importar_abastecimentos():
    from openpyxl import load_workbook
    import tempfile
    import os
    import re
    import unicodedata
    from datetime import datetime, date
    from psycopg2.extras import execute_batch

    def normalizar(txt):
        txt = str(txt or "").strip().lower()
        txt = unicodedata.normalize("NFKD", txt)
        txt = "".join(c for c in txt if not unicodedata.combining(c))
        return txt

    def numero(valor):
        if valor is None:
            return 0
        if isinstance(valor, (int, float)):
            return float(valor)

        txt = str(valor).strip()
        if not txt:
            return 0

        txt = txt.replace(".", "").replace(",", ".")

        try:
            return float(txt)
        except:
            return 0

    def data_valida(valor):
        if isinstance(valor, datetime):
            return valor.date()

        if isinstance(valor, date):
            return valor

        if isinstance(valor, str):
            txt = valor.strip()

            for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(txt, fmt).date()
                except:
                    pass

        return None

    def extrair_numero_abastecimentos(txt):
        txt = str(txt or "")
        nums = re.findall(r"\d+", txt.replace(".", ""))

        if not nums:
            return 0

        return int(nums[-1])

    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")
    resumo = None

    if request.method == "POST":
        arquivo = request.files.get("arquivo")

        if not arquivo or arquivo.filename == "":
            flash("Selecione um arquivo.", "error")
            return redirect(url_for("rh.importar_abastecimentos"))

        caminho_tmp = None
        conn = get_connection()
        cur = conn.cursor()

        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                caminho_tmp = tmp.name
                arquivo.save(caminho_tmp)

            wb = load_workbook(caminho_tmp, data_only=True)
            ws = wb.active

            linhas = []
            mapa = {}

            cod_filial_atual = None
            cache_filiais = {}
            filiais_nao_encontradas = set()

            funcionario_atual = None
            indices_funcionario_atual = []

            for row in ws.iter_rows(values_only=True):
                if not any(row):
                    continue

                col_a_original = str(row[0] or "").strip()
                col_b_original = str(row[1] or "").strip()

                col_a = normalizar(col_a_original)
                col_b = col_b_original.strip()

                # ------------------------------------------
                # QUEBRA DE FILIAL
                # ------------------------------------------
                if "filial" in col_a:
                    nome_filial_importacao = col_b_original.strip()

                    funcionario_atual = None
                    indices_funcionario_atual = []

                    if nome_filial_importacao in cache_filiais:
                        cod_filial_atual = cache_filiais[nome_filial_importacao]
                    else:
                        cur.execute("""
                            SELECT cod_filial
                            FROM filiais
                            WHERE cod_empresa = %s
                              AND (
                                    UPPER(TRIM(COALESCE(nome_filial_importacao, ''))) = UPPER(TRIM(%s))
                                 OR UPPER(TRIM(COALESCE(nome_filial, ''))) = UPPER(TRIM(%s))
                              )
                            LIMIT 1
                        """, (
                            cod_empresa,
                            nome_filial_importacao,
                            nome_filial_importacao,
                        ))

                        filial = cur.fetchone()
                        cod_filial_atual = filial[0] if filial else None

                        if cod_filial_atual is None:
                            filiais_nao_encontradas.add(nome_filial_importacao)

                        cache_filiais[nome_filial_importacao] = cod_filial_atual

                    continue

                # ------------------------------------------
                # LINHA DE TOTAL DO FUNCIONÁRIO
                # Exemplo: Número de Abastecimentos: 3490
                # ------------------------------------------
                if "numero de abastecimentos" in col_a:
                    qtd_abast = extrair_numero_abastecimentos(col_a_original)

                    if indices_funcionario_atual and qtd_abast > 0:
                        linhas[indices_funcionario_atual[0]]["numero_abastecimentos"] = qtd_abast

                    funcionario_atual = None
                    indices_funcionario_atual = []
                    continue

                textos = [normalizar(x) for x in row]
                linha_txt = " ".join(textos)

                # ------------------------------------------
                # DETECTAR CABEÇALHO
                # ------------------------------------------
                if "data" in linha_txt and "produto" in linha_txt and "funcion" in linha_txt:
                    mapa = {}

                    for idx, nome in enumerate(textos):
                        if "data" in nome:
                            mapa["data"] = idx
                        elif "produto" in nome:
                            mapa["produto"] = idx
                        elif "funcion" in nome:
                            mapa["funcionario"] = idx
                        elif "q" in nome and (
                            "abas" in nome
                            or "abast" in nome
                            or "litro" in nome
                            or "volume" in nome
                        ):
                            mapa["quantidade"] = idx
                        elif "medio" in nome or "preco" in nome:
                            mapa["preco"] = idx
                        elif "valor" in nome or "abastec" in nome:
                            mapa["valor"] = idx

                    continue

                if not mapa:
                    continue

                try:
                    data_raw = row[mapa.get("data")]
                    produto = str(row[mapa.get("produto")] or "").strip()
                    funcionario = str(row[mapa.get("funcionario")] or "").strip()
                except:
                    continue

                data_abast = data_valida(data_raw)

                if not data_abast or not produto or not funcionario:
                    continue

                quantidade = numero(row[mapa["quantidade"]]) if "quantidade" in mapa else 0
                preco = numero(row[mapa["preco"]]) if "preco" in mapa else 0
                valor = numero(row[mapa["valor"]]) if "valor" in mapa else 0

                if valor == 0 and quantidade > 0 and preco > 0:
                    valor = quantidade * preco

                chave_funcionario = (
                    cod_filial_atual,
                    funcionario.strip().upper()
                )

                if funcionario_atual != chave_funcionario:
                    funcionario_atual = chave_funcionario
                    indices_funcionario_atual = []

                linhas.append({
                    "cod_empresa": cod_empresa,
                    "cod_filial": cod_filial_atual,
                    "data_abastecimento": data_abast,
                    "produto": produto,
                    "funcionario": funcionario,
                    "quantidade": quantidade,
                    "preco_unitario": preco,
                    "valor_total": valor,
                    "arquivo_origem": arquivo.filename,
                    "numero_abastecimentos": 0,
                })

                indices_funcionario_atual.append(len(linhas) - 1)

            if not linhas:
                flash("Nenhum dado válido encontrado.", "error")
                return redirect(url_for("rh.importar_abastecimentos"))

            # Se alguma filial do arquivo não existir na empresa atual,
            # cancela antes de apagar ou inserir dados.
            if filiais_nao_encontradas:
                flash(
                    "Importação cancelada. Filiais não encontradas para esta empresa: "
                    + ", ".join(sorted(filiais_nao_encontradas)),
                    "error"
                )
                return redirect(url_for("rh.importar_abastecimentos"))

            meses_importados = sorted(set(
                (l["data_abastecimento"].year, l["data_abastecimento"].month)
                for l in linhas
            ))

            for ano, mes in meses_importados:
                cur.execute("""
                    DELETE FROM abastecimentos
                    WHERE cod_empresa = %s
                      AND EXTRACT(YEAR FROM data_abastecimento) = %s
                      AND EXTRACT(MONTH FROM data_abastecimento) = %s
                """, (cod_empresa, ano, mes))

            linhas_insert = [
                (
                    l["cod_empresa"],
                    l["cod_filial"],
                    l["data_abastecimento"],
                    l["produto"],
                    l["funcionario"],
                    l["quantidade"],
                    l["preco_unitario"],
                    l["valor_total"],
                    l["arquivo_origem"],
                    l["numero_abastecimentos"],
                )
                for l in linhas
            ]

            execute_batch(cur, """
                INSERT INTO abastecimentos (
                    cod_empresa,
                    cod_filial,
                    data_abastecimento,
                    produto,
                    funcionario,
                    quantidade,
                    preco_unitario,
                    valor_total,
                    arquivo_origem,
                    numero_abastecimentos
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, linhas_insert, page_size=500)

            conn.commit()

            resumo = {
                "arquivo": arquivo.filename,
                "linhas": len(linhas),
                "total_quantidade": float(sum(l["quantidade"] or 0 for l in linhas)),
                "total_valor": float(sum(l["valor_total"] or 0 for l in linhas)),
                "total_abastecimentos": int(sum(l["numero_abastecimentos"] or 0 for l in linhas)),
                "meses_importados": ", ".join([f"{mes:02d}/{ano}" for ano, mes in meses_importados]),
            }

            flash(f"{len(linhas)} registros importados com sucesso.", "success")

        except Exception as e:
            conn.rollback()
            flash(f"Erro ao importar: {e}", "error")

        finally:
            cur.close()
            conn.close()

            if caminho_tmp and os.path.exists(caminho_tmp):
                os.remove(caminho_tmp)

    return render_template(
        "importar_abastecimentos.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        resumo=resumo,
        url_voltar=url_for("rh.menu_rh"),
        texto_voltar="← Voltar",
    )
# ------------------------------------------
# CONSULTAR ABASTECIMENTOS
# ------------------------------------------
@rh_bp.route("/abastecimentos/consultar")
@permissao_obrigatoria(
    "RH",
    "CONSULTAR_ABASTECIMENTOS",
    redirecionar_para="rh.menu_rh",
)
def consultar_abastecimentos():
    from psycopg2.extras import RealDictCursor
    from datetime import datetime
    from zoneinfo import ZoneInfo

    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).date()

    if hoje.month == 1:
        mes_padrao = 12
        ano_padrao = hoje.year - 1
    else:
        mes_padrao = hoje.month - 1
        ano_padrao = hoje.year

    ano_sel = (request.args.get("ano") or str(ano_padrao)).strip()
    mes_sel = (request.args.get("mes") or str(mes_padrao)).strip()
    filial_sel = (request.args.get("cod_filial") or "").strip()
    ordem_sel = (request.args.get("ordem") or "qtd").strip()

    ordens_sql = {
        "qtd": "qtd_abastecimentos DESC",
        "volume": "total_litros DESC",
        "valor": "total_valor DESC",
        "ticket": "ticket_medio DESC",
        "litros_abast": "litros_por_abastecimento DESC",
    }

    ordem_sql = ordens_sql.get(ordem_sel, "qtd_abastecimentos DESC")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT DISTINCT
                a.cod_filial,
                COALESCE(f.nome_filial, 'Sem filial informada') AS nome_filial
            FROM abastecimentos a
            LEFT JOIN filiais f
              ON f.cod_empresa = a.cod_empresa
             AND f.cod_filial = a.cod_filial
            WHERE a.cod_empresa = %s
            ORDER BY a.cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall() or []

        filtros = ["a.cod_empresa = %s"]
        params = [cod_empresa]

        if ano_sel:
            filtros.append("EXTRACT(YEAR FROM a.data_abastecimento) = %s")
            params.append(int(ano_sel))

        if mes_sel:
            filtros.append("EXTRACT(MONTH FROM a.data_abastecimento) = %s")
            params.append(int(mes_sel))

        if filial_sel:
            filtros.append("a.cod_filial = %s")
            params.append(int(filial_sel))

        where_sql = " AND ".join(filtros)

        cur.execute(f"""
            SELECT
                COALESCE(a.cod_filial, 0) AS cod_filial,
                COALESCE(f.nome_filial, 'Sem filial informada') AS nome_filial,
                a.funcionario,
                COALESCE(SUM(a.numero_abastecimentos), 0) AS qtd_abastecimentos,
                COALESCE(SUM(a.quantidade), 0) AS total_litros,
                COALESCE(SUM(a.valor_total), 0) AS total_valor,
                CASE
                    WHEN COALESCE(SUM(a.numero_abastecimentos), 0) > 0
                    THEN COALESCE(SUM(a.valor_total), 0) / COALESCE(SUM(a.numero_abastecimentos), 0)
                    ELSE 0
                END AS ticket_medio,
                CASE
                    WHEN COALESCE(SUM(a.numero_abastecimentos), 0) > 0
                    THEN COALESCE(SUM(a.quantidade), 0) / COALESCE(SUM(a.numero_abastecimentos), 0)
                    ELSE 0
                END AS litros_por_abastecimento
            FROM abastecimentos a
            LEFT JOIN filiais f
              ON f.cod_empresa = a.cod_empresa
             AND f.cod_filial = a.cod_filial
            WHERE {where_sql}
            GROUP BY
                a.cod_filial,
                f.nome_filial,
                a.funcionario
            ORDER BY
                COALESCE(a.cod_filial, 0),
                {ordem_sql},
                a.funcionario
        """, params)

        linhas = cur.fetchall() or []

        heatmap = {}

        for l in linhas:
            cod = l["cod_filial"] or 0

            if cod not in heatmap:
                heatmap[cod] = {
                    "qtd": {"min": l["qtd_abastecimentos"] or 0, "max": l["qtd_abastecimentos"] or 0},
                    "litros": {"min": l["total_litros"] or 0, "max": l["total_litros"] or 0},
                    "valor": {"min": l["total_valor"] or 0, "max": l["total_valor"] or 0},
                    "ticket": {"min": l["ticket_medio"] or 0, "max": l["ticket_medio"] or 0},
                    "litros_abast": {"min": l["litros_por_abastecimento"] or 0, "max": l["litros_por_abastecimento"] or 0},
                }
            else:
                heatmap[cod]["qtd"]["min"] = min(heatmap[cod]["qtd"]["min"], l["qtd_abastecimentos"] or 0)
                heatmap[cod]["qtd"]["max"] = max(heatmap[cod]["qtd"]["max"], l["qtd_abastecimentos"] or 0)

                heatmap[cod]["litros"]["min"] = min(heatmap[cod]["litros"]["min"], l["total_litros"] or 0)
                heatmap[cod]["litros"]["max"] = max(heatmap[cod]["litros"]["max"], l["total_litros"] or 0)

                heatmap[cod]["valor"]["min"] = min(heatmap[cod]["valor"]["min"], l["total_valor"] or 0)
                heatmap[cod]["valor"]["max"] = max(heatmap[cod]["valor"]["max"], l["total_valor"] or 0)

                heatmap[cod]["ticket"]["min"] = min(heatmap[cod]["ticket"]["min"], l["ticket_medio"] or 0)
                heatmap[cod]["ticket"]["max"] = max(heatmap[cod]["ticket"]["max"], l["ticket_medio"] or 0)

                heatmap[cod]["litros_abast"]["min"] = min(heatmap[cod]["litros_abast"]["min"], l["litros_por_abastecimento"] or 0)
                heatmap[cod]["litros_abast"]["max"] = max(heatmap[cod]["litros_abast"]["max"], l["litros_por_abastecimento"] or 0)

        totais_filiais = {}

        for l in linhas:
            cod = l["cod_filial"] or 0

            if cod not in totais_filiais:
                totais_filiais[cod] = {
                    "qtd_abastecimentos": 0,
                    "total_litros": 0,
                    "total_valor": 0,
                    "ticket_medio": 0,
                    "litros_por_abastecimento": 0,
                }

            totais_filiais[cod]["qtd_abastecimentos"] += l["qtd_abastecimentos"] or 0
            totais_filiais[cod]["total_litros"] += l["total_litros"] or 0
            totais_filiais[cod]["total_valor"] += l["total_valor"] or 0

        for cod, t in totais_filiais.items():
            qtd = t["qtd_abastecimentos"] or 0
            if qtd > 0:
                t["ticket_medio"] = t["total_valor"] / qtd
                t["litros_por_abastecimento"] = t["total_litros"] / qtd

        cur.execute(f"""
            SELECT
                COALESCE(SUM(a.numero_abastecimentos), 0) AS qtd_abastecimentos,
                COALESCE(SUM(a.quantidade), 0) AS total_litros,
                COALESCE(SUM(a.valor_total), 0) AS total_valor,
                CASE
                    WHEN COALESCE(SUM(a.numero_abastecimentos), 0) > 0
                    THEN COALESCE(SUM(a.valor_total), 0) / COALESCE(SUM(a.numero_abastecimentos), 0)
                    ELSE 0
                END AS ticket_medio,
                CASE
                    WHEN COALESCE(SUM(a.numero_abastecimentos), 0) > 0
                    THEN COALESCE(SUM(a.quantidade), 0) / COALESCE(SUM(a.numero_abastecimentos), 0)
                    ELSE 0
                END AS litros_por_abastecimento
            FROM abastecimentos a
            WHERE {where_sql}
        """, params)

        totais = cur.fetchone() or {}

    finally:
        cur.close()
        conn.close()

    return render_template(
        "consultar_abastecimentos.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        ano_sel=ano_sel,
        mes_sel=mes_sel,
        filial_sel=filial_sel,
        ordem_sel=ordem_sel,
        filiais=filiais,
        linhas=linhas,
        totais=totais,
        heatmap=heatmap,
        totais_filiais=totais_filiais,
        url_voltar=url_for("rh.menu_rh"),
        texto_voltar="← Voltar",
    )

# ------------------------------------------
# CONFIGURAÇÕES RH
# ------------------------------------------
@rh_bp.route("/configuracoes")
@permissao_obrigatoria(
    "RH",
    "CONFIGURACOES_RH",
    redirecionar_para="rh.menu_rh",
)
def configuracoes_rh():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    return render_template(
        "menu_configuracoes_rh.html",
        cod_empresa=session.get("cod_empresa", ""),
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("rh.menu_rh"),
        texto_voltar="← Voltar",
    )