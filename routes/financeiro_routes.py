from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify
from datetime import datetime
import math
from psycopg2.extras import RealDictCursor
from db import get_connection
from services.dashboard_service import montar_dashboard
from utils.formatters import formatar_numero_br, formatar_int

financeiro_bp = Blueprint("financeiro", __name__)

def eh_ajax():
    return request.headers.get("X-Requested-With") == "XMLHttpRequest"


def filial_para_dict(f):
    return {
        "cod_filial": f[0],
        "nome_filial": f[1],
        "nome_filial_importacao": f[2] or "",
        "ativo": bool(f[3]),
        "qtde_lancamentos": int(f[4] or 0),
    }


def buscar_filiais_empresa(cur, cod_empresa):
    cur.execute("""
        SELECT
            f.cod_filial,
            f.nome_filial,
            f.nome_filial_importacao,
            f.ativo,
            COALESCE(l.qtde_lancamentos, 0) AS qtde_lancamentos
        FROM filiais f
        LEFT JOIN (
            SELECT
                cod_empresa,
                cod_filial,
                COUNT(*) AS qtde_lancamentos
            FROM lancamentos
            WHERE cod_empresa = %s
            GROUP BY cod_empresa, cod_filial
        ) l
            ON l.cod_empresa = f.cod_empresa
           AND l.cod_filial = f.cod_filial
        WHERE f.cod_empresa = %s
        ORDER BY f.cod_filial
    """, (cod_empresa, cod_empresa))
    return cur.fetchall()

def conta_para_ordenacao(valor):
    texto = str(valor or "").strip()
    try:
        return (0, int(texto))
    except Exception:
        return (1, texto.upper())

def obter_dados_matricial(cod_empresa, ano_sel="", mes_sel="", filial_sel=""):
    conn = get_connection()
    cur = conn.cursor()

    try:
        where = ["l.cod_empresa = %s"]
        params = [cod_empresa]

        if ano_sel:
            where.append("CAST(l.ano AS TEXT) = %s")
            params.append(str(ano_sel))

        if mes_sel:
            where.append("CAST(l.mes AS TEXT) = %s")
            params.append(str(mes_sel))

        if filial_sel:
            where.append("CAST(l.cod_filial AS TEXT) = %s")
            params.append(str(filial_sel))

        where_sql = " AND ".join(where)

        where_filiais = ["cod_empresa = %s", "ativo = TRUE"]
        params_filiais = [cod_empresa]

        if filial_sel:
            where_filiais.append("CAST(cod_filial AS TEXT) = %s")
            params_filiais.append(str(filial_sel))

        where_filiais_sql = " AND ".join(where_filiais)

        cur.execute(f"""
            SELECT
                cod_filial,
                nome_filial
            FROM filiais
            WHERE {where_filiais_sql}
            ORDER BY cod_filial
        """, params_filiais)
        filiais_colunas = cur.fetchall()

        cur.execute(f"""
            SELECT
                l.grupo,
                l.conta,
                COALESCE(NULLIF(TRIM(l.descricao_conta), ''), 'SEM DESCRIÇÃO') AS descricao_conta,
                l.cod_filial,
                COALESCE(SUM(l.valor), 0) AS total_valor
            FROM lancamentos l
            WHERE {where_sql}
              AND l.grupo IS NOT NULL
              AND l.conta IS NOT NULL
            GROUP BY
                l.grupo,
                l.conta,
                COALESCE(NULLIF(TRIM(l.descricao_conta), ''), 'SEM DESCRIÇÃO'),
                l.cod_filial
            ORDER BY
                l.grupo,
                l.conta,
                descricao_conta,
                l.cod_filial
        """, params)
        dados = cur.fetchall()

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
              AND ativo = TRUE
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    filiais_ids = [f[0] for f in filiais_colunas]

    mapa = {}
    for grupo, conta, descricao_conta, cod_filial, total_valor in dados:
        chave = (grupo, conta, descricao_conta)

        if chave not in mapa:
            mapa[chave] = {
                "grupo": grupo,
                "conta": conta,
                "descricao_conta": descricao_conta,
                "por_filial": {fid: 0.0 for fid in filiais_ids},
                "total": 0.0
            }

        valor = float(total_valor or 0)
        mapa[chave]["por_filial"][cod_filial] = valor
        mapa[chave]["total"] += valor

    linhas_matriciais = []
    for item in mapa.values():
        valores = [item["por_filial"].get(fid, 0.0) for fid in filiais_ids]
        linhas_matriciais.append({
            "grupo": item["grupo"],
            "conta": item["conta"],
            "descricao_conta": item["descricao_conta"],
            "valores": valores,
            "total": item["total"]
        })

    linhas_matriciais.sort(
        key=lambda x: (
            int(x["grupo"]) if str(x["grupo"]).isdigit() else 999,
            conta_para_ordenacao(x["conta"]),
            str(x["descricao_conta"]).upper()
        )
    )

    grupos_tmp = {}
    for linha in linhas_matriciais:
        grupo = linha["grupo"]

        if grupo not in grupos_tmp:
            grupos_tmp[grupo] = {
                "grupo": grupo,
                "linhas": [],
                "totais_filiais": [0.0 for _ in filiais_ids],
                "total_geral": 0.0
            }

        grupos_tmp[grupo]["linhas"].append(linha)
        grupos_tmp[grupo]["total_geral"] += linha["total"]

        for i, valor in enumerate(linha["valores"]):
            grupos_tmp[grupo]["totais_filiais"][i] += valor

    grupos_ordenados = sorted(
        grupos_tmp.values(),
        key=lambda g: int(g["grupo"]) if str(g["grupo"]).isdigit() else 999
    )

    total_geral_filiais = [0.0 for _ in filiais_ids]
    total_geral = 0.0

    for grupo in grupos_ordenados:
        for i, valor in enumerate(grupo["totais_filiais"]):
            total_geral_filiais[i] += valor
        total_geral += grupo["total_geral"]

    return {
        "filiais_colunas": filiais_colunas,
        "linhas_matriciais": linhas_matriciais,
        "grupos_ordenados": grupos_ordenados,
        "total_geral_filiais": total_geral_filiais,
        "total_geral": total_geral,
        "anos": anos,
        "meses": meses,
        "filiais": filiais
    }



# =========================
# MENU PRINCIPAL
# =========================
@financeiro_bp.route("/menu")
def menu_empresa():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    linhas, totais = montar_dashboard(session["cod_empresa"])

    return render_template(
        "menu_financeiro.html",
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        linhas_dashboard=linhas,
        totais_dashboard=totais,
        formatar_numero_br=formatar_numero_br,
        formatar_int=formatar_int,
        ano_atual=datetime.now().year,
        url_voltar=url_for("sistema.selecionar_sistema")   # 👈 ESSENCIAL
    )

# =========================
# CADASTROS
# =========================
@financeiro_bp.route("/cadastros")
def menu_cadastros():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    empresa_ativa = session.get("cod_empresa")
    nome_empresa_ativa = session.get("nome_empresa")

    return render_template(
        "menu_cadastros.html",
        empresa_ativa=empresa_ativa,
        nome_empresa_ativa=nome_empresa_ativa,
        url_voltar=url_for("financeiro.menu_empresa")
    )

# =========================
# FILIAIS
# =========================
@financeiro_bp.route("/filiais", methods=["GET", "POST"])
def cadastrar_filiais():
    if "cod_empresa" not in session:
        if eh_ajax():
            return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
        return redirect(url_for("auth.index"))

    mensagem = ""
    erro = ""
    filial_edicao = None
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        if request.method == "POST":
            acao = (request.form.get("acao") or "").strip().lower()

            try:
                cod_filial_raw = (request.form.get("cod_filial") or "").strip()
                nome_filial = (request.form.get("nome_filial") or "").strip()
                nome_filial_importacao = (request.form.get("nome_filial_importacao") or "").strip()
                ativo = True if request.form.get("ativo") == "on" else False

                if acao == "novo":
                    if not nome_filial:
                        raise ValueError("Informe o nome da filial.")

                    cur.execute("""
                        SELECT COALESCE(MAX(cod_filial), 0) + 1
                        FROM filiais
                        WHERE cod_empresa = %s
                    """, (cod_empresa,))
                    proximo_codigo = cur.fetchone()[0]

                    cur.execute("""
                        INSERT INTO filiais (
                            cod_empresa,
                            cod_filial,
                            nome_filial,
                            nome_filial_importacao,
                            ativo
                        )
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        cod_empresa,
                        proximo_codigo,
                        nome_filial,
                        nome_filial_importacao if nome_filial_importacao else None,
                        ativo
                    ))

                    conn.commit()
                    mensagem = f"Filial incluída com sucesso. Código gerado: {proximo_codigo}"

                    if eh_ajax():
                        filiais = buscar_filiais_empresa(cur, cod_empresa)
                        filial_nova = next((f for f in filiais if int(f[0]) == int(proximo_codigo)), None)

                        return jsonify({
                            "ok": True,
                            "mensagem": mensagem,
                            "acao": "novo",
                            "filial": filial_para_dict(filial_nova) if filial_nova else None
                        })

                elif acao == "alterar":
                    if not cod_filial_raw:
                        raise ValueError("Selecione uma filial para alterar.")
                    if not nome_filial:
                        raise ValueError("Informe o nome da filial.")

                    cod_filial = int(cod_filial_raw)

                    cur.execute("""
                        UPDATE filiais
                        SET nome_filial = %s,
                            nome_filial_importacao = %s,
                            ativo = %s
                        WHERE cod_empresa = %s
                          AND cod_filial = %s
                    """, (
                        nome_filial,
                        nome_filial_importacao if nome_filial_importacao else None,
                        ativo,
                        cod_empresa,
                        cod_filial
                    ))

                    if cur.rowcount == 0:
                        raise ValueError("Filial não encontrada para alteração.")

                    conn.commit()
                    mensagem = "Filial alterada com sucesso."

                    if eh_ajax():
                        filiais = buscar_filiais_empresa(cur, cod_empresa)
                        filial_alt = next((f for f in filiais if int(f[0]) == int(cod_filial)), None)

                        return jsonify({
                            "ok": True,
                            "mensagem": mensagem,
                            "acao": "alterar",
                            "filial": filial_para_dict(filial_alt) if filial_alt else None
                        })

                elif acao == "excluir":
                    if not cod_filial_raw:
                        raise ValueError("Selecione uma filial para excluir.")

                    cod_filial = int(cod_filial_raw)

                    cur.execute("""
                        SELECT COUNT(*)
                        FROM lancamentos
                        WHERE cod_empresa = %s
                          AND cod_filial = %s
                    """, (cod_empresa, cod_filial))
                    qtde_lanc = cur.fetchone()[0]

                    if qtde_lanc > 0:
                        raise ValueError("Esta filial possui lançamentos e não pode ser excluída.")

                    cur.execute("""
                        DELETE FROM filiais
                        WHERE cod_empresa = %s
                          AND cod_filial = %s
                    """, (cod_empresa, cod_filial))

                    if cur.rowcount == 0:
                        raise ValueError("Filial não encontrada para exclusão.")

                    conn.commit()
                    mensagem = "Filial excluída com sucesso."

                    if eh_ajax():
                        return jsonify({
                            "ok": True,
                            "mensagem": mensagem,
                            "acao": "excluir",
                            "cod_filial": cod_filial
                        })

                elif acao == "carregar":
                    if not cod_filial_raw:
                        raise ValueError("Selecione uma filial para carregar.")

                    cod_filial = int(cod_filial_raw)

                    cur.execute("""
                        SELECT
                            cod_filial,
                            nome_filial,
                            nome_filial_importacao,
                            ativo
                        FROM filiais
                        WHERE cod_empresa = %s
                          AND cod_filial = %s
                    """, (cod_empresa, cod_filial))
                    filial_edicao = cur.fetchone()

                    if not filial_edicao:
                        raise ValueError("Filial não encontrada.")

                    if eh_ajax():
                        return jsonify({
                            "ok": True,
                            "acao": "carregar",
                            "filial": {
                                "cod_filial": filial_edicao[0],
                                "nome_filial": filial_edicao[1],
                                "nome_filial_importacao": filial_edicao[2] or "",
                                "ativo": bool(filial_edicao[3]),
                            }
                        })

            except Exception as e:
                conn.rollback()
                erro = str(e)

                if eh_ajax():
                    return jsonify({"ok": False, "erro": erro}), 400

        filiais = buscar_filiais_empresa(cur, cod_empresa)

    finally:
        cur.close()
        conn.close()

    return render_template(
        "filiais.html",
        filiais=filiais,
        filial_edicao=filial_edicao,
        mensagem=mensagem,
        erro=erro,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("sistema.selecionar_sistema"),
        url_menu_modulo=url_for("financeiro.menu_empresa"),
        texto_menu_modulo="Menu do Financeiro"
    )

# =========================
# GRUPOS GERENCIAIS
# =========================
@financeiro_bp.route("/grupos_gerenciais")
def grupos_gerenciais():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT cod_grupo, abreviatura, descricao
            FROM grupos_gerenciais
            ORDER BY cod_grupo
        """)
        grupos = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return render_template(
        "grupos_gerenciais.html",
        grupos=grupos,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_cadastros"),
        texto_voltar="← Voltar"
    )


# =========================
# CONTAS GERENCIAIS
# =========================
@financeiro_bp.route("/contas_gerenciais", methods=["GET", "POST"])
def contas_gerenciais():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    mensagem = ""
    erro = ""
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        if request.method == "POST":
            try:
                descricoes = request.form.getlist("descricao[]")
                grupos = request.form.getlist("cod_grupo[]")
                contas_post = request.form.getlist("cod_conta[]")

                if not (len(descricoes) == len(grupos) == len(contas_post)):
                    raise ValueError("Os dados enviados estão inconsistentes.")

                for i in range(len(descricoes)):
                    cod_grupo = int(grupos[i])
                    cod_conta = int(contas_post[i])
                    descricao = (descricoes[i] or "").strip()

                    if descricao == "":
                        descricao = None

                    cur.execute("""
                        UPDATE contas_gerenciais
                        SET descricao = %s
                        WHERE cod_empresa = %s
                          AND cod_grupo = %s
                          AND cod_conta = %s
                    """, (descricao, cod_empresa, cod_grupo, cod_conta))

                conn.commit()
                mensagem = "Descrições atualizadas com sucesso."

            except Exception as e:
                conn.rollback()
                erro = str(e)

        cur.execute("""
            SELECT
                c.cod_grupo,
                g.abreviatura AS nome_grupo,
                c.cod_conta,
                c.descricao
            FROM contas_gerenciais c
            LEFT JOIN grupos_gerenciais g
                ON c.cod_grupo = g.cod_grupo
            WHERE c.cod_empresa = %s
            ORDER BY c.cod_grupo, c.cod_conta
        """, (cod_empresa,))
        contas = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return render_template(
        "contas_gerenciais.html",
        contas=contas,
        mensagem=mensagem,
        erro=erro,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_cadastros"),
        texto_voltar="← Voltar"
    )


# =========================
# CLASSIFICAÇÕES AUTOMÁTICAS
# =========================
@financeiro_bp.route("/classificacoes_automaticas", methods=["GET", "POST"])
def classificacoes_automaticas():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    mensagem = ""
    erro = ""
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        if request.method == "POST":
            acao = (request.form.get("acao") or "").strip().lower()

            try:
                if acao == "salvar":
                    ids = request.form.getlist("id_classificacao[]")
                    textos = request.form.getlist("texto[]")
                    grupos = request.form.getlist("cod_grupo[]")
                    contas = request.form.getlist("cod_conta[]")
                    complementos = request.form.getlist("complemento[]")

                    if not (len(ids) == len(textos) == len(grupos) == len(contas) == len(complementos)):
                        raise ValueError("Os dados enviados estão inconsistentes.")

                    for i in range(len(ids)):
                        id_classificacao = (ids[i] or "").strip()
                        texto = (textos[i] or "").strip()
                        grupo_raw = (grupos[i] or "").strip()
                        conta_raw = (contas[i] or "").strip()
                        complemento = (complementos[i] or "").strip()

                        if not texto:
                            continue

                        if not grupo_raw or not conta_raw:
                            raise ValueError(f"Informe grupo e conta na linha {i + 1}.")

                        cod_grupo = int(grupo_raw)
                        cod_conta = int(conta_raw)

                        if cod_grupo < 1 or cod_grupo > 7:
                            raise ValueError(f"Grupo inválido na linha {i + 1}.")
                        if cod_conta < 1 or cod_conta > 15:
                            raise ValueError(f"Conta inválida na linha {i + 1}.")

                        if id_classificacao:
                            cur.execute("""
                                UPDATE classificacoes_automaticas
                                   SET texto = %s,
                                       cod_grupo = %s,
                                       cod_conta = %s,
                                       complemento = %s
                                 WHERE id_classificacao = %s
                                   AND cod_empresa = %s
                            """, (
                                texto,
                                cod_grupo,
                                cod_conta,
                                complemento if complemento else None,
                                int(id_classificacao),
                                cod_empresa
                            ))
                        else:
                            cur.execute("""
                                INSERT INTO classificacoes_automaticas
                                    (cod_empresa, texto, cod_grupo, cod_conta, complemento)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (
                                cod_empresa,
                                texto,
                                cod_grupo,
                                cod_conta,
                                complemento if complemento else None
                            ))

                    conn.commit()
                    mensagem = "Classificações automáticas salvas com sucesso."

                elif acao == "excluir":
                    id_excluir = (request.form.get("id_excluir") or "").strip()
                    if not id_excluir:
                        raise ValueError("Registro não informado para exclusão.")

                    cur.execute("""
                        DELETE FROM classificacoes_automaticas
                        WHERE id_classificacao = %s
                          AND cod_empresa = %s
                    """, (int(id_excluir), cod_empresa))

                    if cur.rowcount == 0:
                        raise ValueError("Registro não encontrado para exclusão.")

                    conn.commit()
                    mensagem = "Registro excluído com sucesso."

            except Exception as e:
                conn.rollback()
                erro = str(e)

        cur.execute("""
            SELECT
                ca.id_classificacao,
                ca.texto,
                ca.cod_grupo,
                ca.cod_conta,
                cg.descricao AS descricao_conta,
                ca.complemento
            FROM classificacoes_automaticas ca
            LEFT JOIN contas_gerenciais cg
                   ON cg.cod_empresa = ca.cod_empresa
                  AND cg.cod_grupo = ca.cod_grupo
                  AND cg.cod_conta = ca.cod_conta
            WHERE ca.cod_empresa = %s
            ORDER BY LOWER(ca.texto)
        """, (cod_empresa,))
        classificacoes = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return render_template(
        "classificacoes_automaticas.html",
        classificacoes=classificacoes,
        mensagem=mensagem,
        erro=erro,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_cadastros"),
        texto_voltar="← Voltar"
    )


# =========================
# LANÇAMENTOS
# =========================
@financeiro_bp.route("/lancamentos")
def listar_lancamentos():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    page = request.args.get("page", default=1, type=int)
    if page < 1:
        page = 1

    filial_sel = (request.args.get("filial") or "").strip()
    grupo_sel = (request.args.get("grupo") or "").strip()
    conta_sel = (request.args.get("conta") or "").strip()
    data_ini = (request.args.get("data_ini") or "").strip()
    data_fim = (request.args.get("data_fim") or "").strip()
    busca = (request.args.get("busca") or "").strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT DISTINCT nome_filial
            FROM lancamentos
            WHERE cod_empresa = %s
              AND COALESCE(TRIM(nome_filial), '') <> ''
            ORDER BY nome_filial
        """, (cod_empresa,))
        filiais = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT grupo
            FROM lancamentos
            WHERE cod_empresa = %s
              AND grupo IS NOT NULL
            ORDER BY grupo
        """, (cod_empresa,))
        grupos = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT conta
            FROM lancamentos
            WHERE cod_empresa = %s
              AND conta IS NOT NULL
            ORDER BY conta
        """, (cod_empresa,))
        contas = [r[0] for r in cur.fetchall()]

        where = ["cod_empresa = %s"]
        params = [cod_empresa]

        if filial_sel:
            where.append("nome_filial = %s")
            params.append(filial_sel)

        if grupo_sel:
            where.append("CAST(grupo AS TEXT) = %s")
            params.append(grupo_sel)

        if conta_sel:
            where.append("CAST(conta AS TEXT) = %s")
            params.append(conta_sel)

        if data_ini:
            where.append("data >= %s")
            params.append(data_ini)

        if data_fim:
            where.append("data <= %s")
            params.append(data_fim)

        if busca:
            where.append("""
                (
                    UPPER(COALESCE(historico, '')) LIKE UPPER(%s)
                    OR UPPER(COALESCE(descricao_conta, '')) LIKE UPPER(%s)
                    OR UPPER(COALESCE(complemento, '')) LIKE UPPER(%s)
                )
            """)
            termo = f"%{busca}%"
            params.extend([termo, termo, termo])

        where_sql = " AND ".join(where)

        cur.execute(f"""
            SELECT COUNT(*)
            FROM lancamentos
            WHERE {where_sql}
        """, params)
        total_rows = cur.fetchone()[0]

        page_size = 100
        total_pages = max(1, math.ceil(total_rows / page_size))

        if page > total_pages:
            page = total_pages

        offset = (page - 1) * page_size

        cur.execute(f"""
            SELECT
                cod_filial,
                nome_filial,
                conta_banco,
                ano,
                mes,
                data,
                historico,
                valor,
                descricao_conta,
                grupo,
                conta,
                complemento,
                id_lancamento,
                cod_empresa
            FROM lancamentos
            WHERE {where_sql}
            ORDER BY id_lancamento
            LIMIT %s OFFSET %s
        """, params + [page_size, offset])

        colnames = [d[0] for d in cur.description]
        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return render_template(
        "lancamentos.html",
        rows=rows,
        colnames=colnames,
        page=page,
        total_pages=total_pages,
        has_prev=page > 1,
        has_next=page < total_pages,
        total_rows=total_rows,
        page_size=page_size,
        filiais=filiais,
        grupos=grupos,
        contas=contas,
        filial_sel=filial_sel,
        grupo_sel=grupo_sel,
        conta_sel=conta_sel,
        data_ini=data_ini,
        data_fim=data_fim,
        busca=busca,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_empresa"),
        texto_voltar="← Voltar"
    )


# =========================
# ATUALIZAR LANÇAMENTO
# =========================
@financeiro_bp.route("/lancamentos/atualizar/<int:id_lancamento>", methods=["POST"])
def atualizar_lancamento(id_lancamento):
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    grupo = (request.form.get("grupo") or "").strip()
    conta = (request.form.get("conta") or "").strip()

    if not grupo or not conta:
        return "Grupo e conta são obrigatórios.", 400

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE lancamentos
            SET grupo = %s,
                conta = %s
            WHERE cod_empresa = %s
              AND id_lancamento = %s
        """, (grupo, conta, cod_empresa, id_lancamento))

        conn.commit()

    finally:
        cur.close()
        conn.close()

    return redirect(url_for(
        "financeiro.listar_lancamentos",
        page=request.form.get("page", 1),
        filial=request.form.get("filial", ""),
        grupo=request.form.get("filtro_grupo", ""),
        conta=request.form.get("filtro_conta", ""),
        data_ini=request.form.get("data_ini", ""),
        data_fim=request.form.get("data_fim", ""),
        busca=request.form.get("busca", "")
    ))


# =========================
# RESULTADO POR MARGEM BRUTA
# =========================
@financeiro_bp.route("/resultado_mb", methods=["GET"])
def resultado_mb():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    ano = request.args.get("ano", type=int)
    mes = request.args.get("mes", type=int)

    hoje = datetime.now()

    # Sugere o mês anterior
    if not ano and not mes:
        if hoje.month == 1:
            ano = hoje.year - 1
            mes = 12
        else:
            ano = hoje.year
            mes = hoje.month - 1
    elif ano and not mes:
        if hoje.month == 1:
            mes = 12
            if ano == hoje.year:
                ano = ano - 1
        else:
            mes = hoje.month - 1
    elif mes and not ano:
        if hoje.month == 1 and mes == 12:
            ano = hoje.year - 1
        else:
            ano = hoje.year

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = true
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()

        def buscar_valores(tabela, filtro_extra):
            where_mes = ""
            params = [cod_empresa, ano]

            if mes:
                where_mes = "AND mes = %s"
                params.append(mes)

            campo_valor = "valor"
            if tabela == "vendas_mb_sintetico":
                campo_valor = "margem_bruta"

            cur.execute(f"""
                SELECT cod_filial, COALESCE(SUM({campo_valor}), 0)
                FROM {tabela}
                WHERE cod_empresa = %s
                AND ano = %s
                {where_mes}
                {filtro_extra}
                GROUP BY cod_filial
            """, params)

            return {r[0]: float(r[1]) for r in cur.fetchall()}

        mb = buscar_valores("vendas_mb_sintetico", "")
        desp = buscar_valores("lancamentos", "AND grupo = '4'")
        inv = buscar_valores("lancamentos", "AND grupo = '5'")
        div = buscar_valores("lancamentos", "AND grupo = '6'")

        def montar_linha(nome, base):
            linha = {"nome": nome, "total": 0.0, "por_filial": {}}
            for cod_filial, _ in filiais:
                v = base.get(cod_filial, 0.0)
                linha["por_filial"][cod_filial] = v
                linha["total"] += v
            return linha

        linha_mb = montar_linha("MB", mb)
        linha_desp = montar_linha("DESPESAS", desp)

        linha_res1 = montar_linha("RESULTADO 1", {
            cod_filial: mb.get(cod_filial, 0.0) + desp.get(cod_filial, 0.0)
            for cod_filial, _ in filiais
        })

        linha_inv = montar_linha("INVESTIMENTOS / AMORTIZAÇÕES", inv)

        linha_res2 = montar_linha("RESULTADO 2", {
            cod_filial: linha_res1["por_filial"][cod_filial] + inv.get(cod_filial, 0.0)
            for cod_filial, _ in filiais
        })

        linha_div = montar_linha("ANTECIPAÇÃO DIVIDENDOS", div)

        linha_res3 = montar_linha("RESULTADO 3", {
            cod_filial: linha_res2["por_filial"][cod_filial] + div.get(cod_filial, 0.0)
            for cod_filial, _ in filiais
        })

        linhas = [
            linha_mb,
            linha_desp,
            linha_res1,
            linha_inv,
            linha_res2,
            linha_div,
            linha_res3,
        ]

    finally:
        cur.close()
        conn.close()

    return render_template(
        "resultado_mb.html",
        filiais=filiais,
        linhas=linhas,
        ano=ano,
        mes=mes,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_empresa")
    )

# =========================
# MATRICIAL
# =========================


@financeiro_bp.route("/matricial")
def matricial():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    hoje = datetime.now()

    ano_sel = (request.args.get("ano") or "").strip()
    mes_sel = (request.args.get("mes") or "").strip()
    filial_sel = (request.args.get("filial") or "").strip()

    if not ano_sel and not mes_sel:
        if hoje.month == 1:
            ano_sel = str(hoje.year - 1)
            mes_sel = "12"
        else:
            ano_sel = str(hoje.year)
            mes_sel = str(hoje.month - 1)

    dados = obter_dados_matricial(
        cod_empresa,
        ano_sel,
        mes_sel,
        filial_sel
    )

    return render_template(
        "matricial.html",
        filiais_colunas=dados["filiais_colunas"],
        linhas_matriciais=dados["linhas_matriciais"],
        grupos_ordenados=dados["grupos_ordenados"],
        total_geral_filiais=dados["total_geral_filiais"],
        total_geral=dados["total_geral"],
        anos=dados["anos"],
        meses=dados["meses"],
        filiais=dados["filiais"],
        ano_sel=ano_sel,
        mes_sel=mes_sel,
        filial_sel=filial_sel,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        formatar_numero_br=formatar_numero_br,
        url_voltar=url_for("financeiro.menu_empresa"),
        texto_voltar="← Voltar"
    )

# =========================
# VARIACOES 
# =========================

@financeiro_bp.route("/variacoes")
def variacoes():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    ano_sel = (request.args.get("ano") or "").strip()
    filial_sel = (request.args.get("filial") or "").strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:

        # captura o parâmetro original
        ano_param = request.args.get("ano", None)

        # lista de anos
        cur.execute("""
            SELECT DISTINCT ano
            FROM lancamentos
            WHERE cod_empresa = %s
              AND ano IS NOT NULL
            ORDER BY ano
        """, (cod_empresa,))
        anos = [r["ano"] for r in cur.fetchall()]

        # definição do ano selecionado
        if ano_param is None:
            # primeira vez: seleciona o último ano existente na base
            ano_sel = str(anos[-1]) if anos else ""
        else:
            # veio da tela (pode ser "" = Todos ou um ano específico)
            ano_sel = ano_param


        # lista de filiais
        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = TRUE
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()

        # contas gerenciais por grupo
        cur.execute("""
            SELECT
                cg.cod_grupo,
                gg.descricao AS nome_grupo,
                cg.cod_conta,
                cg.descricao
            FROM contas_gerenciais cg
            LEFT JOIN grupos_gerenciais gg
              ON gg.cod_grupo = cg.cod_grupo
            WHERE cg.cod_empresa = %s
            ORDER BY cg.cod_grupo, cg.cod_conta
        """, (cod_empresa,))
        contas = cur.fetchall()

        contas_por_grupo = {}
        for c in contas:
            g = c["cod_grupo"]
            if g not in contas_por_grupo:
                contas_por_grupo[g] = {
                    "nome_grupo": c["nome_grupo"] or f"Grupo {g}",
                    "contas": []
                }
            contas_por_grupo[g]["contas"].append({
                "cod_conta": c["cod_conta"],
                "descricao": c["descricao"] or "-"
            })

        # valores dos lançamentos
        params = [cod_empresa]
        sql_ano = ""
        sql_filial = ""

        if ano_sel:
            sql_ano = "AND l.ano >= %s"
            params.append(int(ano_sel))

        if filial_sel:
            sql_filial = "AND l.cod_filial = %s"
            params.append(filial_sel)

        cur.execute(f"""
            SELECT
                l.grupo,
                l.conta,
                l.ano,
                l.mes,
                COALESCE(SUM(l.valor), 0) AS valor
            FROM lancamentos l
            WHERE l.cod_empresa = %s
            {sql_ano}
            {sql_filial}
            AND l.grupo IS NOT NULL
            AND l.conta IS NOT NULL
            GROUP BY l.grupo, l.conta, l.ano, l.mes
            ORDER BY l.ano, l.mes, l.grupo, l.conta
        """, params)

        valores = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    mapa_valores = {}
    for v in valores:
        chave = (
            str(v["grupo"]).strip(),
            str(v["conta"]).strip(),
            int(v["ano"]),
            int(v["mes"])
        )
        mapa_valores[chave] = float(v["valor"] or 0)

    mapa_meses = {
        1: "JAN", 2: "FEV", 3: "MAR", 4: "ABR",
        5: "MAI", 6: "JUN", 7: "JUL", 8: "AGO",
        9: "SET", 10: "OUT", 11: "NOV", 12: "DEZ"
    }
    periodos = sorted(
    {(int(v["ano"]), int(v["mes"])) for v in valores},
    key=lambda x: (x[0], x[1])
    )    

    grupos = []
    for grupo in sorted(contas_por_grupo.keys(), key=lambda x: int(x)):
        info = contas_por_grupo[grupo]
        linhas = []
        ultimo_ano = None

        for ano, num_mes in periodos:
            linha = {
                "ano": ano,
                "mes_num": num_mes,
                "mes_nome": mapa_meses[num_mes],
                "valores": [],
                "total_mes": 0.0,
                "quebra_ano": False
            }

            if ultimo_ano is not None and ano != ultimo_ano:
                linha["quebra_ano"] = True

            ultimo_ano = ano

            for conta in info["contas"]:
                valor = mapa_valores.get(
                    (str(grupo), str(conta["cod_conta"]), ano, num_mes),
                    0.0
                )
                linha["valores"].append(valor)
                linha["total_mes"] += valor

            linhas.append(linha)

        grupos.append({
            "cod_grupo": grupo,
            "nome_grupo": info["nome_grupo"],
            "contas": info["contas"],
            "linhas": linhas
        })
    def cor_excel(valor, vmin, vmax):
        if valor is None:
            return ""

        if vmax == vmin:
            return ""

        ratio = (valor - vmin) / (vmax - vmin)
        ratio = max(0, min(1, ratio))

        if ratio < 0.5:
            r = 255
            g = int(255 * (ratio * 2))
            b = 0
        else:
            r = int(255 * (1 - (ratio - 0.5) * 2))
            g = 255
            b = 0

        return f"background-color: rgb({r},{g},{b});"

    for grupo in grupos:
        colunas = list(zip(*[linha["valores"] for linha in grupo["linhas"]]))
        grupo["min_max"] = []

        for col in colunas:
            valores_validos = [v for v in col if v is not None]

            if valores_validos:
                grupo["min_max"].append({
                    "min": min(valores_validos),
                    "max": max(valores_validos)
                })
            else:
                grupo["min_max"].append({
                    "min": 0,
                    "max": 0
                })
    return render_template(
        "variacoes.html",
        anos=anos,
        ano_sel=ano_sel,
        filiais=filiais,
        filial_sel=filial_sel,
        grupos=grupos,
        nome_empresa=session.get("nome_empresa", ""),
        empresa_ativa=session.get("cod_empresa", ""),
        url_voltar=url_for("financeiro.menu_empresa"),
        texto_voltar="← Voltar",
        formatar_numero_br=formatar_numero_br,
        cor_excel=cor_excel
    )

# =========================
# DADOS DETALHADOS
# =========================

@financeiro_bp.route("/dados_detalhados")
def dados_detalhados():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    ano = request.args.get("ano", type=int)
    mes = request.args.get("mes", type=int)
    grupo = request.args.get("grupo", default=4, type=int)

    hoje = datetime.now()
    if not ano:
        ano = hoje.year
    if not mes:
        mes = hoje.month

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = true
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais_rows = cur.fetchall()

        filiais = [r[0] for r in filiais_rows]
        mapa_filiais = {r[0]: r[1] for r in filiais_rows}

        cur.execute("""
            SELECT
                conta,
                COALESCE(NULLIF(TRIM(descricao_conta), ''), 'SEM DESCRIÇÃO') AS descricao_conta,
                COALESCE(NULLIF(TRIM(historico), ''), 'SEM HISTÓRICO') AS historico,
                cod_filial,
                COALESCE(SUM(valor), 0) AS total_valor
            FROM lancamentos
            WHERE cod_empresa = %s
              AND ano = %s
              AND mes = %s
              AND grupo = %s
            GROUP BY
                conta,
                COALESCE(NULLIF(TRIM(descricao_conta), ''), 'SEM DESCRIÇÃO'),
                COALESCE(NULLIF(TRIM(historico), ''), 'SEM HISTÓRICO'),
                cod_filial
            ORDER BY
                conta,
                descricao_conta,
                historico,
                cod_filial
        """, (cod_empresa, ano, mes, grupo))

        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    def conta_para_ordenacao(valor):
        texto = str(valor or "").strip()
        try:
            return (0, int(texto))
        except Exception:
            return (1, texto.upper())

    def classe_heatmap(valor, minimo, maximo):
        if maximo <= minimo:
            return "hm-25"

        proporcao = (valor - minimo) / (maximo - minimo)
        proporcao = max(0, min(1, proporcao))

        faixa = int(round(proporcao * 50))
        return f"hm-{faixa}"

    dados = {}
    totais_gerais = {f: 0.0 for f in filiais}

    for conta, descricao_conta, historico, cod_filial, total_valor in rows:
        valor = float(total_valor or 0)

        if conta not in dados:
            dados[conta] = {
                "nome": descricao_conta,
                "linhas": {},
                "totais": {f: 0.0 for f in filiais}
            }

        if historico not in dados[conta]["linhas"]:
            dados[conta]["linhas"][historico] = {
                "filiais": {f: 0.0 for f in filiais},
                "classes": {f: "" for f in filiais}
            }

        dados[conta]["linhas"][historico]["filiais"][cod_filial] = valor
        dados[conta]["totais"][cod_filial] += valor
        totais_gerais[cod_filial] += valor

    # aplica heatmap por linha de histórico
    for conta, info in dados.items():
        for hist, linha in info["linhas"].items():
            valores = list(linha["filiais"].values())
            minimo = min(valores) if valores else 0
            maximo = max(valores) if valores else 0

            for f in filiais:
                linha["classes"][f] = classe_heatmap(
                    linha["filiais"][f],
                    minimo,
                    maximo
                )

    # ordena contas
    dados_ordenados = dict(
        sorted(
            dados.items(),
            key=lambda item: conta_para_ordenacao(item[0])
        )
    )

    return render_template(
        "dados_detalhados.html",
        ano=ano,
        mes=mes,
        grupo=grupo,
        filiais=filiais,
        mapa_filiais=mapa_filiais,
        dados=dados_ordenados,
        totais_gerais=totais_gerais,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_empresa"),
        texto_voltar="← Voltar"
    )

# =========================
# MARGEM BRUTA
# =========================

@financeiro_bp.route("/margem_bruta", methods=["GET", "POST"])
def margem_bruta():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    mensagem = ""
    erro = ""

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = TRUE
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()

        if request.method == "POST":
            try:
                for chave, valor_txt in request.form.items():
                    if not chave.startswith("valor_"):
                        continue

                    _, ano_txt, mes_txt, cod_filial_txt = chave.split("_", 3)

                    ano = int(ano_txt)
                    mes = int(mes_txt)
                    cod_filial = int(cod_filial_txt)

                    valor_txt = (valor_txt or "").strip()
                    if valor_txt == "":
                        valor = 0.0
                    else:
                        valor = float(valor_txt.replace(".", "").replace(",", "."))

                    cur.execute("""
                        INSERT INTO vendas_mb_sintetico (
                            cod_empresa,
                            cod_filial,
                            ano,
                            mes,
                            margem_bruta
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (cod_empresa, cod_filial, ano, mes)
                        DO UPDATE SET
                            margem_bruta = EXCLUDED.margem_bruta,
                            data_importacao = NOW()
                    """, (cod_empresa, cod_filial, ano, mes, valor))

                conn.commit()
                mensagem = "Margem bruta salva com sucesso."

            except Exception as e:
                conn.rollback()
                erro = str(e)

        cur.execute("""
            SELECT DISTINCT ano, mes
            FROM vendas_mb_sintetico
            WHERE cod_empresa = %s
            ORDER BY ano, mes
        """, (cod_empresa,))
        periodos = cur.fetchall()

        cur.execute("""
            SELECT cod_filial, ano, mes, margem_bruta
            FROM vendas_mb_sintetico
            WHERE cod_empresa = %s
            ORDER BY ano, mes, cod_filial
        """, (cod_empresa,))
        registros = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    mapa = {}
    for cod_filial, ano, mes, margem_bruta in registros:
        chave = (ano, mes)
        if chave not in mapa:
            mapa[chave] = {
                "ano": ano,
                "mes": mes,
                "valores": {},
                "total": 0.0
            }

        v = float(margem_bruta or 0)
        mapa[chave]["valores"][cod_filial] = v
        mapa[chave]["total"] += v

    linhas = []
    for ano, mes in periodos:
        chave = (ano, mes)
        if chave in mapa:
            linhas.append(mapa[chave])
        else:
            linhas.append({
                "ano": ano,
                "mes": mes,
                "valores": {},
                "total": 0.0
            })

    return render_template(
        "margem_bruta.html",
        filiais=filiais,
        linhas=linhas,
        mensagem=mensagem,
        erro=erro,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_empresa"),
        texto_voltar="← Voltar"
    )

    
# =========================
# EXCLUSÕES DE LANÇAMENTOS
# =========================
@financeiro_bp.route("/exclusoes", methods=["GET", "POST"])
def exclusoes():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    mensagem = ""
    erro = ""

    # filtros
    ano_sel = request.values.get("ano", "")
    mes_sel = request.values.get("mes", "")
    filial_sel = request.values.get("filial", "")
    grupo_sel = request.values.get("grupo", "")
    conta_sel = request.values.get("conta", "")
    data_ini = request.values.get("data_ini", "")
    data_fim = request.values.get("data_fim", "")
    busca = request.values.get("busca", "")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # =========================
        # EXCLUSÃO (POST)
        # =========================
        if request.method == "POST":
            ids = request.form.getlist("ids_marcados")

            if ids:
                cur.execute(f"""
                    DELETE FROM lancamentos
                    WHERE id_lancamento = ANY(%s)
                      AND cod_empresa = %s
                """, (ids, cod_empresa))

                conn.commit()
                mensagem = f"{len(ids)} lançamento(s) excluído(s) com sucesso."
            else:
                erro = "Nenhum registro selecionado."

        # =========================
        # FILTROS DINÂMICOS
        # =========================
        where = ["cod_empresa = %s"]
        params = [cod_empresa]

        if ano_sel:
            where.append("ano = %s")
            params.append(ano_sel)

        if mes_sel:
            where.append("mes = %s")
            params.append(mes_sel)

        if filial_sel:
            where.append("nome_filial = %s")
            params.append(filial_sel)

        if grupo_sel:
            where.append("grupo = %s")
            params.append(grupo_sel)

        if conta_sel:
            where.append("conta = %s")
            params.append(conta_sel)

        if data_ini:
            where.append("data >= %s")
            params.append(data_ini)

        if data_fim:
            where.append("data <= %s")
            params.append(data_fim)

        if busca:
            where.append("""
                (historico ILIKE %s OR
                 descricao_conta ILIKE %s OR
                 complemento ILIKE %s)
            """)
            like = f"%{busca}%"
            params.extend([like, like, like])

        where_sql = " AND ".join(where)

        # =========================
        # CONSULTA PRINCIPAL
        # =========================
        cur.execute(f"""
            SELECT
                id_lancamento,
                data,
                nome_filial,
                historico,
                valor,
                grupo,
                conta,
                descricao_conta,
                complemento,
                cod_empresa
            FROM lancamentos
            WHERE {where_sql}
            ORDER BY data DESC, id_lancamento DESC
            LIMIT 1000
        """, params)

        rows = cur.fetchall()

        # =========================
        # COMBOS (FILTROS)
        # =========================
        cur.execute("""
            SELECT DISTINCT ano FROM lancamentos
            WHERE cod_empresa = %s
            ORDER BY ano DESC
        """, (cod_empresa,))
        anos = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT mes FROM lancamentos
            WHERE cod_empresa = %s
            ORDER BY mes
        """, (cod_empresa,))
        meses = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT nome_filial FROM lancamentos
            WHERE cod_empresa = %s
            ORDER BY nome_filial
        """, (cod_empresa,))
        filiais = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT grupo FROM lancamentos
            WHERE cod_empresa = %s
            ORDER BY grupo
        """, (cod_empresa,))
        grupos = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT conta FROM lancamentos
            WHERE cod_empresa = %s
            ORDER BY conta
        """, (cod_empresa,))
        contas = [r[0] for r in cur.fetchall()]

    finally:
        cur.close()
        conn.close()

    return render_template(
        "exclusoes.html",
        rows=rows,
        anos=anos,
        meses=meses,
        filiais=filiais,
        grupos=grupos,
        contas=contas,
        ano_sel=ano_sel,
        mes_sel=mes_sel,
        filial_sel=filial_sel,
        grupo_sel=grupo_sel,
        conta_sel=conta_sel,
        data_ini=data_ini,
        data_fim=data_fim,
        busca=busca,
        mensagem=mensagem,
        erro=erro,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_empresa"),
        texto_voltar="← Voltar"
    )