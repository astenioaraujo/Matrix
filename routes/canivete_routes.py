from datetime import date
from flask import Blueprint, render_template, redirect, url_for, session, flash, request, jsonify
from psycopg2.extras import RealDictCursor
from security_helpers import usuario_tem_permissao
from db import get_connection

canivete_bp = Blueprint("canivete", __name__, url_prefix="/canivete")


def _checar_login():
    if "id_usuario" not in session or "cod_empresa" not in session:
        return redirect(url_for("auth.index"))
    return None


def _tipo_global():
    return str(session.get("tipo_global") or "").strip().lower()


def _tem_perm(id_usuario, cod_empresa, opcao):
    return _tipo_global() == "superusuario" or usuario_tem_permissao(
        id_usuario, cod_empresa, "CANIVETE", opcao
    )


# -----------------------------------------------------------
# MENU CANIVETE
# -----------------------------------------------------------

@canivete_bp.route("/menu")
def menu_canivete():
    r = _checar_login()
    if r:
        return r

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()

    if not _tem_perm(id_usuario, cod_empresa, "MENU"):
        flash("Você não tem permissão para acessar o Canivete Suíço.", "error")
        return redirect(url_for("sistema.selecionar_sistema"))

    session["sistema_ativo"] = "canivete"

    return render_template(
        "menu_canivete.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema"),
        pode_financas_pessoais=_tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_MENU"),
    )


# -----------------------------------------------------------
# FINANÇAS PESSOAIS — MENU
# -----------------------------------------------------------

@canivete_bp.route("/financas-pessoais")
def menu_financas_pessoais():
    r = _checar_login()
    if r:
        return r

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()

    if not _tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_MENU"):
        flash("Você não tem permissão para acessar Finanças Pessoais.", "error")
        return redirect(url_for("canivete.menu_canivete"))

    return render_template(
        "canivete/financas_pessoais/menu.html",
        nome_usuario=session.get("nome_usuario"),
        url_voltar=url_for("canivete.menu_canivete"),
        pode_lancar=_tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_LANCAR"),
        pode_consultar=_tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_CONSULTAR"),
        pode_configuracoes=_tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_CONFIGURACOES"),
    )


# -----------------------------------------------------------
# FINANÇAS PESSOAIS — LANÇAR
# -----------------------------------------------------------

@canivete_bp.route("/financas-pessoais/lancar", methods=["GET", "POST"])
def fp_lancar():
    r = _checar_login()
    if r:
        return r

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()

    if not _tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_LANCAR"):
        flash("Sem permissão.", "error")
        return redirect(url_for("canivete.menu_financas_pessoais"))

    hoje = date.today()
    mes_sel = int(request.args.get("mes") or request.form.get("mes") or hoje.month)
    ano_sel = int(request.args.get("ano") or request.form.get("ano") or hoje.year)

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    try:
        # Salvar novo lançamento
        if request.method == "POST" and request.form.get("acao") == "incluir":
            data_lancamento = request.form.get("data_lancamento")
            descricao = (request.form.get("descricao") or "").strip()
            valor_raw = (request.form.get("valor") or "0").replace(".", "").replace(",", ".")
            id_classificacao = request.form.get("id_classificacao") or None

            try:
                valor = float(valor_raw)
            except ValueError:
                valor = 0.0

            id_conta_bancaria = request.form.get("id_conta_bancaria") or None

            if descricao and data_lancamento:
                cur.execute("""
                    INSERT INTO fp_lancamentos (id_usuario, data, descricao, valor, id_classificacao, id_conta_bancaria)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (id_usuario, data_lancamento, descricao, valor, id_classificacao, id_conta_bancaria))
                novo_id = cur.fetchone()["id"]
                conn.commit()

                if is_ajax:
                    cur.execute("""
                        SELECT l.id, l.data, l.descricao, l.valor,
                               c.nome AS classificacao, l.id_classificacao,
                               b.nome AS conta_bancaria, l.id_conta_bancaria
                        FROM fp_lancamentos l
                        LEFT JOIN fp_classificacoes c ON c.id = l.id_classificacao
                        LEFT JOIN fp_contas_bancarias b ON b.id = l.id_conta_bancaria
                        WHERE l.id = %s
                    """, (novo_id,))
                    row = cur.fetchone()
                    cur.close()
                    conn.close()
                    return jsonify({
                        "ok": True,
                        "id": row["id"],
                        "data": row["data"].strftime("%d/%m/%Y"),
                        "data_iso": row["data"].isoformat(),
                        "descricao": row["descricao"],
                        "valor": float(row["valor"]),
                        "id_classificacao": row["id_classificacao"],
                        "classificacao": row["classificacao"] or "",
                        "id_conta_bancaria": row["id_conta_bancaria"],
                        "conta_bancaria": row["conta_bancaria"] or "—",
                    })

                flash("Lançamento incluído.", "success")

        # Alterar classificação
        if request.method == "POST" and request.form.get("acao") == "classificar":
            id_lanc = request.form.get("id_lanc")
            id_classificacao = request.form.get("id_classificacao") or None
            if id_lanc:
                cur.execute("""
                    UPDATE fp_lancamentos SET id_classificacao = %s
                    WHERE id = %s AND id_usuario = %s
                """, (id_classificacao, id_lanc, id_usuario))
                conn.commit()
            if is_ajax:
                cur.close()
                conn.close()
                return jsonify({"ok": True})
            proxima_url = request.form.get("proxima_url")
            if proxima_url:
                cur.close()
                conn.close()
                return redirect(proxima_url)

        # Excluir lançamento
        if request.method == "POST" and request.form.get("acao") == "excluir":
            id_excluir = request.form.get("id_excluir")
            if id_excluir:
                cur.execute("DELETE FROM fp_lancamentos WHERE id = %s AND id_usuario = %s",
                            (id_excluir, id_usuario))
                conn.commit()
            if is_ajax:
                cur.close()
                conn.close()
                return jsonify({"ok": True})
            flash("Lançamento excluído.", "success")

        # Buscar lançamentos do mês/ano
        cur.execute("""
            SELECT l.id, l.data, l.descricao, l.valor,
                   c.nome AS classificacao, l.id_classificacao,
                   b.nome AS conta_bancaria, l.id_conta_bancaria
            FROM fp_lancamentos l
            LEFT JOIN fp_classificacoes c ON c.id = l.id_classificacao
            LEFT JOIN fp_contas_bancarias b ON b.id = l.id_conta_bancaria
            WHERE l.id_usuario = %s
              AND EXTRACT(MONTH FROM l.data) = %s
              AND EXTRACT(YEAR  FROM l.data) = %s
            ORDER BY l.data, l.id
        """, (id_usuario, mes_sel, ano_sel))
        lancamentos = cur.fetchall() or []

        # Classificações ativas
        cur.execute("""
            SELECT id, nome FROM fp_classificacoes
            WHERE id_usuario = %s AND ativo = TRUE
            ORDER BY nome
        """, (id_usuario,))
        classificacoes = cur.fetchall() or []

        # Contas bancárias ativas
        cur.execute("""
            SELECT id, nome FROM fp_contas_bancarias
            WHERE id_usuario = %s AND ativo = TRUE
            ORDER BY nome
        """, (id_usuario,))
        contas_bancarias = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    anos = list(range(hoje.year - 3, hoje.year + 2))
    meses = list(range(1, 13))
    nomes_meses = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                   "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]

    return render_template(
        "canivete/financas_pessoais/lancar.html",
        nome_usuario=session.get("nome_usuario"),
        url_voltar=url_for("canivete.menu_financas_pessoais"),
        lancamentos=lancamentos,
        classificacoes=classificacoes,
        contas_bancarias=contas_bancarias,
        mes_sel=mes_sel,
        ano_sel=ano_sel,
        anos=anos,
        meses=meses,
        nomes_meses=nomes_meses,
        hoje=hoje.strftime("%Y-%m-%d"),
    )


# -----------------------------------------------------------
# FINANÇAS PESSOAIS — CONSULTAR
# -----------------------------------------------------------

@canivete_bp.route("/financas-pessoais/consultar")
def fp_consultar():
    r = _checar_login()
    if r:
        return r

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()

    if not _tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_CONSULTAR"):
        flash("Sem permissão.", "error")
        return redirect(url_for("canivete.menu_financas_pessoais"))

    hoje = date.today()
    mes_sel = int(request.args.get("mes") or hoje.month)
    ano_sel = int(request.args.get("ano") or hoje.year)
    filtro = request.args.get("filtro", "todos")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT l.id, l.data, l.descricao, l.valor,
                   COALESCE(c.nome, '(Sem classificação)') AS classificacao,
                   COALESCE(l.id_classificacao::text, '') AS id_classificacao,
                   COALESCE(b.nome, '—') AS conta
            FROM fp_lancamentos l
            LEFT JOIN fp_classificacoes c ON c.id = l.id_classificacao
            LEFT JOIN fp_contas_bancarias b ON b.id = l.id_conta_bancaria
            WHERE l.id_usuario = %s
              AND EXTRACT(MONTH FROM l.data) = %s
              AND EXTRACT(YEAR  FROM l.data) = %s
            ORDER BY l.data, l.id
        """, (id_usuario, mes_sel, ano_sel))
        lancamentos_raw = cur.fetchall() or []

        import json as _json
        from collections import defaultdict as _dd
        grupos_lanc = _dd(list)
        for lx in lancamentos_raw:
            grupos_lanc[lx["classificacao"]].append({
                "id": lx["id"],
                "data": lx["data"].strftime("%d/%m/%Y"),
                "descricao": lx["descricao"],
                "valor": float(lx["valor"]),
                "conta": lx["conta"],
                "id_classificacao": lx["id_classificacao"],
            })
        lancamentos_json = _json.dumps(dict(grupos_lanc), ensure_ascii=False)

        cur.execute("""
            SELECT id, nome FROM fp_classificacoes
            WHERE id_usuario = %s AND ativo = TRUE
            ORDER BY nome
        """, (id_usuario,))
        classificacoes_consulta = cur.fetchall() or []

        cur.execute("""
            SELECT COALESCE(SUM(valor), 0) AS total
            FROM fp_lancamentos
            WHERE id_usuario = %s
              AND EXTRACT(MONTH FROM data) = %s
              AND EXTRACT(YEAR  FROM data) = %s
        """, (id_usuario, mes_sel, ano_sel))
        total = cur.fetchone()["total"]

        cur.execute("""
            SELECT c.nome AS classificacao,
                   COALESCE(c.valor_orcado, 0) AS orcado,
                   COALESCE(SUM(l.valor), 0) AS realizado,
                   c.ajustar_ao_real
            FROM fp_classificacoes c
            LEFT JOIN fp_lancamentos l
                ON l.id_classificacao = c.id
               AND l.id_usuario = %s
               AND EXTRACT(MONTH FROM l.data) = %s
               AND EXTRACT(YEAR  FROM l.data) = %s
            WHERE c.id_usuario = %s
              AND c.ativo = TRUE
            GROUP BY c.id, c.nome, c.valor_orcado, c.ajustar_ao_real

            UNION ALL

            SELECT '(Sem classificação)' AS classificacao,
                   0 AS orcado,
                   COALESCE(SUM(valor), 0) AS realizado,
                   FALSE AS ajustar_ao_real
            FROM fp_lancamentos
            WHERE id_usuario = %s
              AND id_classificacao IS NULL
              AND EXTRACT(MONTH FROM data) = %s
              AND EXTRACT(YEAR  FROM data) = %s
            HAVING COALESCE(SUM(valor), 0) <> 0

            ORDER BY classificacao
        """, (id_usuario, mes_sel, ano_sel, id_usuario, id_usuario, mes_sel, ano_sel))
        totais_classificacao = cur.fetchall() or []

        total_orcado = sum(r["orcado"] for r in totais_classificacao)

    finally:
        cur.close()
        conn.close()

    anos = list(range(hoje.year - 3, hoje.year + 2))
    meses = list(range(1, 13))
    nomes_meses = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                   "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]

    return render_template(
        "canivete/financas_pessoais/consultar.html",
        nome_usuario=session.get("nome_usuario"),
        url_voltar=url_for("canivete.menu_financas_pessoais"),
        filtro=filtro,
        lancamentos_json=lancamentos_json,
        classificacoes_consulta=classificacoes_consulta,
        total=total,
        totais_classificacao=totais_classificacao,
        total_orcado=total_orcado,
        mes_sel=mes_sel,
        ano_sel=ano_sel,
        anos=anos,
        meses=meses,
        nomes_meses=nomes_meses,
    )


# -----------------------------------------------------------
# FINANÇAS PESSOAIS — CONFIGURAÇÕES (classificações)
# -----------------------------------------------------------

@canivete_bp.route("/financas-pessoais/configuracoes", methods=["GET", "POST"])
def fp_configuracoes():
    r = _checar_login()
    if r:
        return r

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()

    if not _tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_CONFIGURACOES"):
        flash("Sem permissão.", "error")
        return redirect(url_for("canivete.menu_financas_pessoais"))

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            acao = request.form.get("acao")
            tabela = request.form.get("tabela", "classificacoes")

            if tabela == "classificacoes":
                if acao == "incluir":
                    nome = (request.form.get("nome") or "").strip()
                    if nome:
                        cur.execute("INSERT INTO fp_classificacoes (id_usuario, nome) VALUES (%s, %s)", (id_usuario, nome))
                        conn.commit()
                        flash("Classificação incluída.", "success")
                elif acao == "excluir":
                    id_excluir = request.form.get("id_excluir")
                    if id_excluir:
                        cur.execute("DELETE FROM fp_classificacoes WHERE id = %s AND id_usuario = %s", (id_excluir, id_usuario))
                        conn.commit()
                        flash("Classificação excluída.", "success")
                elif acao == "editar":
                    id_editar = request.form.get("id_editar")
                    nome = (request.form.get("nome_editar") or "").strip()
                    if id_editar and nome:
                        cur.execute("UPDATE fp_classificacoes SET nome = %s, atualizado_em = NOW() WHERE id = %s AND id_usuario = %s", (nome, id_editar, id_usuario))
                        conn.commit()
                        flash("Classificação atualizada.", "success")

            elif tabela == "orcamento":
                is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
                for key, val in request.form.items():
                    if key.startswith("orcado_"):
                        id_class = key.replace("orcado_", "")
                        try:
                            valor = float((val or "0").replace(".", "").replace(",", "."))
                        except ValueError:
                            valor = 0.0
                        ajustar = request.form.get(f"ajustar_{id_class}") == "1"
                        cur.execute("""
                            UPDATE fp_classificacoes SET valor_orcado = %s, ajustar_ao_real = %s
                            WHERE id = %s AND id_usuario = %s
                        """, (valor, ajustar, id_class, id_usuario))
                conn.commit()
                if is_ajax:
                    cur.close()
                    conn.close()
                    return jsonify({"ok": True})
                flash("Orçamento salvo.", "success")

            elif tabela == "contas":
                if acao == "incluir":
                    nome = (request.form.get("nome") or "").strip()
                    if nome:
                        cur.execute("INSERT INTO fp_contas_bancarias (id_usuario, nome) VALUES (%s, %s)", (id_usuario, nome))
                        conn.commit()
                        flash("Conta bancária incluída.", "success")
                elif acao == "excluir":
                    id_excluir = request.form.get("id_excluir")
                    if id_excluir:
                        cur.execute("DELETE FROM fp_contas_bancarias WHERE id = %s AND id_usuario = %s", (id_excluir, id_usuario))
                        conn.commit()
                        flash("Conta bancária excluída.", "success")
                elif acao == "editar":
                    id_editar = request.form.get("id_editar")
                    nome = (request.form.get("nome_editar") or "").strip()
                    if id_editar and nome:
                        cur.execute("UPDATE fp_contas_bancarias SET nome = %s, atualizado_em = NOW() WHERE id = %s AND id_usuario = %s", (nome, id_editar, id_usuario))
                        conn.commit()
                        flash("Conta bancária atualizada.", "success")

        cur.execute("SELECT id, nome, ativo, valor_orcado, ajustar_ao_real FROM fp_classificacoes WHERE id_usuario = %s ORDER BY nome", (id_usuario,))
        classificacoes = cur.fetchall() or []

        cur.execute("SELECT id, nome, ativo FROM fp_contas_bancarias WHERE id_usuario = %s ORDER BY nome", (id_usuario,))
        contas_bancarias = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "canivete/financas_pessoais/configuracoes.html",
        nome_usuario=session.get("nome_usuario"),
        url_voltar=url_for("canivete.menu_financas_pessoais"),
        classificacoes=classificacoes,
        contas_bancarias=contas_bancarias,
    )
