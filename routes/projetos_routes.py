import json as _json
from datetime import date
from flask import Blueprint, render_template, redirect, url_for, session, flash, request, jsonify
from psycopg2.extras import RealDictCursor
from db import get_connection
from security_helpers import usuario_tem_permissao

projetos_bp = Blueprint("projetos", __name__, url_prefix="/projetos")

NOMES_MESES = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
               "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]


def _checar_acesso():
    if "id_usuario" not in session or "cod_empresa" not in session:
        return redirect(url_for("auth.index"))
    tipo_global = str(session.get("tipo_global") or "").strip().lower()
    if tipo_global != "superusuario":
        id_usuario  = session["id_usuario"]
        cod_empresa = str(session["cod_empresa"]).strip()
        if not usuario_tem_permissao(id_usuario, cod_empresa, "PROJETOS", "MENU"):
            flash("Você não tem permissão para acessar Projetos.", "error")
            return redirect(url_for("sistema.selecionar_sistema"))
    return None


def _pastas_rec(cur, cod_empresa, apenas_ativas=True):
    filtro = "AND ativo = TRUE" if apenas_ativas else ""
    cur.execute(f"""
        SELECT id, nome, ordem, ativo FROM projetos_rec_pastas
        WHERE cod_empresa = %s {filtro} ORDER BY ordem, nome
    """, (cod_empresa,))
    return cur.fetchall()


# ─── MENU ────────────────────────────────────────────────────────────────────

@projetos_bp.route("/")
def menu_projetos():
    redir = _checar_acesso()
    if redir:
        return redir
    return render_template(
        "menu_projetos.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema"),
    )


# ─── MELHORIAS CONTÍNUAS ─────────────────────────────────────────────────────

@projetos_bp.route("/melhorias-continuas")
def melhorias_continuas():
    redir = _checar_acesso()
    if redir:
        return redir

    cod_empresa = str(session["cod_empresa"]).strip()
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id, nome FROM projetos_mc_pastas
            WHERE cod_empresa = %s AND ativo = TRUE
            ORDER BY ordem, nome
        """, (cod_empresa,))
        pastas = cur.fetchall()
    finally:
        cur.close(); conn.close()

    if len(pastas) == 1:
        return redirect(url_for("projetos.melhorias_pasta", id_pasta=pastas[0]["id"]))

    return render_template(
        "projetos/melhorias_continuas.html",
        nome_empresa=session.get("nome_empresa"),
        pastas=pastas,
        url_voltar=url_for("projetos.menu_projetos"),
    )


@projetos_bp.route("/melhorias-continuas/<int:id_pasta>", methods=["GET", "POST"])
def melhorias_pasta(id_pasta):
    redir = _checar_acesso()
    if redir:
        return redir

    cod_empresa = str(session["cod_empresa"]).strip()
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT id, nome FROM projetos_mc_pastas
            WHERE id = %s AND cod_empresa = %s AND ativo = TRUE
        """, (id_pasta, cod_empresa))
        pasta = cur.fetchone()
        if not pasta:
            flash("Pasta não encontrada.", "error")
            return redirect(url_for("projetos.melhorias_continuas"))

        if request.method == "POST":
            acao = request.form.get("acao")

            if acao == "incluir":
                meta    = (request.form.get("meta") or "").strip()
                prazo   = request.form.get("prazo") or None
                status  = request.form.get("status") or ""
                impacto = (request.form.get("impacto") or "").strip()
                cur.execute("SELECT COALESCE(MAX(ordem),0)+1 AS prox FROM projetos_mc_itens WHERE id_pasta=%s", (id_pasta,))
                ordem = cur.fetchone()["prox"]
                cur.execute("""
                    INSERT INTO projetos_mc_itens (id_pasta, cod_empresa, meta, prazo, status, impacto, ordem)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id, ordem
                """, (id_pasta, cod_empresa, meta, prazo, status, impacto, ordem))
                row = cur.fetchone()
                conn.commit()
                return jsonify({"ok": True, "id": row["id"], "ordem": row["ordem"]})

            elif acao == "editar":
                id_item = int(request.form.get("id") or 0)
                campo   = request.form.get("campo")
                valor   = request.form.get("valor") or ""
                if campo in {"meta", "prazo", "status", "impacto"} and id_item:
                    if campo == "status" and valor == "CONCLUIDO":
                        cur.execute("""
                            UPDATE projetos_mc_itens SET status=%s, data_conclusao=NOW()
                            WHERE id=%s AND id_pasta=%s AND cod_empresa=%s
                        """, (valor, id_item, id_pasta, cod_empresa))
                    elif campo == "status":
                        cur.execute("""
                            UPDATE projetos_mc_itens SET status=%s, data_conclusao=NULL
                            WHERE id=%s AND id_pasta=%s AND cod_empresa=%s
                        """, (valor, id_item, id_pasta, cod_empresa))
                    else:
                        cur.execute(f"""
                            UPDATE projetos_mc_itens SET {campo} = %s
                            WHERE id = %s AND id_pasta = %s AND cod_empresa = %s
                        """, (valor or None if campo == "prazo" else valor, id_item, id_pasta, cod_empresa))
                    conn.commit()
                return jsonify({"ok": True})

            elif acao == "excluir":
                id_item = int(request.form.get("id") or 0)
                if id_item:
                    cur.execute("DELETE FROM projetos_mc_itens WHERE id=%s AND id_pasta=%s AND cod_empresa=%s",
                                (id_item, id_pasta, cod_empresa))
                    conn.commit()
                return jsonify({"ok": True})

            elif acao == "reordenar":
                ids = _json.loads(request.form.get("ids") or "[]")
                for i, iid in enumerate(ids):
                    cur.execute("UPDATE projetos_mc_itens SET ordem=%s WHERE id=%s AND id_pasta=%s AND cod_empresa=%s",
                                (i, iid, id_pasta, cod_empresa))
                conn.commit()
                return jsonify({"ok": True})

        cur.execute("""
            SELECT id, meta, prazo, status, impacto, ordem, data_conclusao
            FROM projetos_mc_itens
            WHERE id_pasta = %s AND cod_empresa = %s AND (status IS NULL OR status != 'CONCLUIDO')
            ORDER BY ordem, id
        """, (id_pasta, cod_empresa))
        itens = cur.fetchall()

        # filtro para concluídas
        fil_mes = request.args.get("fil_mes", "")
        fil_ano = request.args.get("fil_ano", str(date.today().year))

        if fil_mes and fil_ano:
            cur.execute("""
                SELECT id, meta, prazo, status, impacto, ordem, data_conclusao
                FROM projetos_mc_itens
                WHERE id_pasta=%s AND cod_empresa=%s AND status='CONCLUIDO'
                  AND EXTRACT(MONTH FROM data_conclusao)=%s
                  AND EXTRACT(YEAR  FROM data_conclusao)=%s
                ORDER BY data_conclusao DESC
            """, (id_pasta, cod_empresa, int(fil_mes), int(fil_ano)))
        elif fil_ano:
            cur.execute("""
                SELECT id, meta, prazo, status, impacto, ordem, data_conclusao
                FROM projetos_mc_itens
                WHERE id_pasta=%s AND cod_empresa=%s AND status='CONCLUIDO'
                  AND EXTRACT(YEAR FROM data_conclusao)=%s
                ORDER BY data_conclusao DESC
            """, (id_pasta, cod_empresa, int(fil_ano)))
        else:
            cur.execute("""
                SELECT id, meta, prazo, status, impacto, ordem, data_conclusao
                FROM projetos_mc_itens
                WHERE id_pasta=%s AND cod_empresa=%s AND status='CONCLUIDO'
                ORDER BY data_conclusao DESC
            """, (id_pasta, cod_empresa))
        concluidas = cur.fetchall()

        cur.execute("SELECT id, nome FROM projetos_mc_pastas WHERE cod_empresa=%s AND ativo=TRUE ORDER BY ordem, nome", (cod_empresa,))
        pastas = cur.fetchall()

    finally:
        cur.close(); conn.close()

    url_voltar = (url_for("projetos.menu_projetos") if len(pastas) <= 1
                  else url_for("projetos.melhorias_continuas"))

    return render_template(
        "projetos/melhorias_pasta.html",
        nome_empresa=session.get("nome_empresa"),
        pasta=pasta,
        itens=itens,
        concluidas=concluidas,
        pastas=pastas,
        fil_mes=fil_mes,
        fil_ano=fil_ano,
        ano_atual=date.today().year,
        mes_atual=date.today().month,
        url_voltar=url_voltar,
    )


# ─── RECORRÊNCIAS MENSAIS ────────────────────────────────────────────────────

@projetos_bp.route("/recorrencias-mensais")
def recorrencias_mensais():
    redir = _checar_acesso()
    if redir:
        return redir
    return render_template(
        "projetos/recorrencias_mensais.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("projetos.menu_projetos"),
    )


# ── Cadastrar modelos ─────────────────────────────────────────────────────────

@projetos_bp.route("/recorrencias-mensais/cadastrar")
def rec_cadastrar():
    redir = _checar_acesso()
    if redir:
        return redir

    cod_empresa = str(session["cod_empresa"]).strip()
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        pastas = _pastas_rec(cur, cod_empresa)
    finally:
        cur.close(); conn.close()

    if len(pastas) == 1:
        return redirect(url_for("projetos.rec_cadastrar_pasta", id_pasta=pastas[0]["id"]))

    return render_template(
        "projetos/rec_selecionar_pasta.html",
        nome_empresa=session.get("nome_empresa"),
        pastas=pastas,
        modo="cadastrar",
        titulo="Cadastrar Recorrências",
        url_voltar=url_for("projetos.recorrencias_mensais"),
    )


@projetos_bp.route("/recorrencias-mensais/cadastrar/<int:id_pasta>", methods=["GET", "POST"])
def rec_cadastrar_pasta(id_pasta):
    redir = _checar_acesso()
    if redir:
        return redir

    cod_empresa = str(session["cod_empresa"]).strip()
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("SELECT id, nome FROM projetos_rec_pastas WHERE id=%s AND cod_empresa=%s AND ativo=TRUE",
                    (id_pasta, cod_empresa))
        pasta = cur.fetchone()
        if not pasta:
            flash("Pasta não encontrada.", "error")
            return redirect(url_for("projetos.rec_cadastrar"))

        if request.method == "POST":
            acao = request.form.get("acao")

            if acao == "incluir":
                descricao  = (request.form.get("descricao") or "").strip()
                qtd_etapas = min(int(request.form.get("qtd_etapas") or 0), 7)
                etapas     = (request.form.get("etapas") or "").strip()
                cur.execute("SELECT COALESCE(MAX(ordem),0)+1 AS prox FROM projetos_rec_modelos WHERE id_pasta=%s", (id_pasta,))
                ordem = cur.fetchone()["prox"]
                cur.execute("""
                    INSERT INTO projetos_rec_modelos (id_pasta, cod_empresa, descricao, ordem, qtd_etapas, etapas)
                    VALUES (%s,%s,%s,%s,%s,%s) RETURNING id
                """, (id_pasta, cod_empresa, descricao, ordem, qtd_etapas, etapas))
                row = cur.fetchone()
                conn.commit()
                return jsonify({"ok": True, "id": row["id"], "ordem": ordem,
                                "qtd_etapas": qtd_etapas, "etapas": etapas})

            elif acao == "editar":
                id_mod = int(request.form.get("id") or 0)
                campo  = request.form.get("campo")
                valor  = request.form.get("valor") or ""
                if campo in ("descricao", "etapas") and id_mod:
                    cur.execute(f"UPDATE projetos_rec_modelos SET {campo}=%s WHERE id=%s AND id_pasta=%s AND cod_empresa=%s",
                                (valor, id_mod, id_pasta, cod_empresa))
                    conn.commit()
                elif campo == "qtd_etapas" and id_mod:
                    cur.execute("UPDATE projetos_rec_modelos SET qtd_etapas=%s WHERE id=%s AND id_pasta=%s AND cod_empresa=%s",
                                (min(int(valor or 0), 7), id_mod, id_pasta, cod_empresa))
                    conn.commit()
                return jsonify({"ok": True})

            elif acao == "excluir":
                id_mod = int(request.form.get("id") or 0)
                if id_mod:
                    cur.execute("DELETE FROM projetos_rec_modelos WHERE id=%s AND id_pasta=%s AND cod_empresa=%s",
                                (id_mod, id_pasta, cod_empresa))
                    conn.commit()
                return jsonify({"ok": True})

            elif acao == "reordenar":
                ids = _json.loads(request.form.get("ids") or "[]")
                for i, iid in enumerate(ids):
                    cur.execute("UPDATE projetos_rec_modelos SET ordem=%s WHERE id=%s AND id_pasta=%s AND cod_empresa=%s",
                                (i, iid, id_pasta, cod_empresa))
                conn.commit()
                return jsonify({"ok": True})

        cur.execute("""
            SELECT id, descricao, ordem, qtd_etapas, etapas FROM projetos_rec_modelos
            WHERE id_pasta=%s AND cod_empresa=%s AND ativo=TRUE ORDER BY ordem, id
        """, (id_pasta, cod_empresa))
        modelos = cur.fetchall()

        pastas = _pastas_rec(cur, cod_empresa)

    finally:
        cur.close(); conn.close()

    url_voltar = (url_for("projetos.recorrencias_mensais") if len(pastas) <= 1
                  else url_for("projetos.rec_cadastrar"))

    return render_template(
        "projetos/rec_cadastrar_pasta.html",
        nome_empresa=session.get("nome_empresa"),
        pasta=pasta,
        modelos=modelos,
        pastas=pastas,
        url_voltar=url_voltar,
    )


# ── Executar recorrências ─────────────────────────────────────────────────────

@projetos_bp.route("/recorrencias-mensais/executar")
def rec_executar():
    redir = _checar_acesso()
    if redir:
        return redir

    cod_empresa = str(session["cod_empresa"]).strip()
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        pastas = _pastas_rec(cur, cod_empresa)
    finally:
        cur.close(); conn.close()

    if len(pastas) == 1:
        return redirect(url_for("projetos.rec_executar_pasta", id_pasta=pastas[0]["id"]))

    return render_template(
        "projetos/rec_selecionar_pasta.html",
        nome_empresa=session.get("nome_empresa"),
        pastas=pastas,
        modo="executar",
        titulo="Executar Recorrências",
        url_voltar=url_for("projetos.recorrencias_mensais"),
    )


@projetos_bp.route("/recorrencias-mensais/executar/<int:id_pasta>", methods=["GET", "POST"])
def rec_executar_pasta(id_pasta):
    redir = _checar_acesso()
    if redir:
        return redir

    cod_empresa = str(session["cod_empresa"]).strip()
    hoje        = date.today()
    ano_sel     = int(request.args.get("ano")  or request.form.get("ano")  or hoje.year)
    mes_sel     = int(request.args.get("mes")  or request.form.get("mes")  or hoje.month)

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("SELECT id, nome FROM projetos_rec_pastas WHERE id=%s AND cod_empresa=%s AND ativo=TRUE",
                    (id_pasta, cod_empresa))
        pasta = cur.fetchone()
        if not pasta:
            flash("Pasta não encontrada.", "error")
            return redirect(url_for("projetos.rec_executar"))

        if request.method == "POST":
            acao = request.form.get("acao")
            try:
                if acao in ("carregar", "recarregar"):
                    cur.execute("""
                        SELECT id, descricao, ordem, qtd_etapas, etapas FROM projetos_rec_modelos
                        WHERE id_pasta=%s AND cod_empresa=%s AND ativo=TRUE ORDER BY ordem, id
                    """, (id_pasta, cod_empresa))
                    modelos = cur.fetchall()

                    cur.execute("""
                        SELECT id_modelo_origem FROM projetos_rec_execucoes
                        WHERE id_pasta=%s AND cod_empresa=%s AND ano=%s AND mes=%s
                    """, (id_pasta, cod_empresa, ano_sel, mes_sel))
                    ja_carregados = {r["id_modelo_origem"] for r in cur.fetchall() if r["id_modelo_origem"]}

                    novos = 0
                    for m in modelos:
                        if m["id"] not in ja_carregados:
                            cur.execute("""
                                INSERT INTO projetos_rec_execucoes
                                    (id_pasta, cod_empresa, ano, mes, descricao, ordem,
                                     id_modelo_origem, qtd_etapas, etapas, etapas_concluidas)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'[]')
                            """, (id_pasta, cod_empresa, ano_sel, mes_sel,
                                  m["descricao"], m["ordem"], m["id"],
                                  m["qtd_etapas"], m["etapas"]))
                            novos += 1
                    conn.commit()
                    return jsonify({"ok": True, "novos": novos})

                elif acao == "set_etapa":
                    import json as _js
                    id_exec = int(request.form.get("id") or 0)
                    idx     = int(request.form.get("idx") or 0)
                    checked = request.form.get("checked") == "1"
                    if id_exec:
                        cur.execute("SELECT etapas_concluidas, qtd_etapas FROM projetos_rec_execucoes WHERE id=%s AND id_pasta=%s AND cod_empresa=%s",
                                    (id_exec, id_pasta, cod_empresa))
                        row = cur.fetchone()
                        if row:
                            try:
                                estado = _js.loads(row["etapas_concluidas"] or "[]")
                            except Exception:
                                estado = []
                            # garante tamanho correto
                            qtd = row["qtd_etapas"] or 0
                            while len(estado) < qtd:
                                estado.append(False)
                            if 0 <= idx < qtd:
                                estado[idx] = checked
                            cur.execute("UPDATE projetos_rec_execucoes SET etapas_concluidas=%s WHERE id=%s AND id_pasta=%s AND cod_empresa=%s",
                                        (_js.dumps(estado), id_exec, id_pasta, cod_empresa))
                            conn.commit()
                    return jsonify({"ok": True})

                elif acao == "set_status":
                    id_exec = int(request.form.get("id") or 0)
                    novo_status = request.form.get("status") or ""
                    VALIDOS = {"", "execucao", "pausa", "concluida", "cancelado"}
                    if id_exec and novo_status in VALIDOS:
                        if novo_status == "concluida":
                            cur.execute("""
                                UPDATE projetos_rec_execucoes
                                SET status=%s, concluido=TRUE, data_conclusao=NOW(),
                                    id_usuario_conclusao=%s, nome_usuario_conclusao=%s
                                WHERE id=%s AND id_pasta=%s AND cod_empresa=%s
                            """, (novo_status, session.get("id_usuario"), session.get("nome_usuario"),
                                  id_exec, id_pasta, cod_empresa))
                        else:
                            cur.execute("""
                                UPDATE projetos_rec_execucoes
                                SET status=%s, concluido=FALSE, data_conclusao=NULL,
                                    id_usuario_conclusao=NULL, nome_usuario_conclusao=NULL
                                WHERE id=%s AND id_pasta=%s AND cod_empresa=%s
                            """, (novo_status, id_exec, id_pasta, cod_empresa))
                        conn.commit()
                    return jsonify({"ok": True})

                elif acao == "reordenar":
                    ids = _json.loads(request.form.get("ids") or "[]")
                    for i, iid in enumerate(ids):
                        cur.execute("UPDATE projetos_rec_execucoes SET ordem=%s WHERE id=%s AND id_pasta=%s AND cod_empresa=%s",
                                    (i, iid, id_pasta, cod_empresa))
                    conn.commit()
                    return jsonify({"ok": True})

            except Exception as e:
                conn.rollback()
                return jsonify({"ok": False, "erro": str(e)}), 200

        # GET
        cur.execute("""
            SELECT id, descricao, ordem, status, concluido, data_conclusao, nome_usuario_conclusao,
                   qtd_etapas, etapas, etapas_concluidas
            FROM projetos_rec_execucoes
            WHERE id_pasta=%s AND cod_empresa=%s AND ano=%s AND mes=%s
            ORDER BY ordem, id
        """, (id_pasta, cod_empresa, ano_sel, mes_sel))
        execucoes = cur.fetchall()

        # parseia etapas_concluidas de JSON para lista
        import json as _js
        for e in execucoes:
            try:
                e["etapas_lista"]     = [s.strip() for s in (e["etapas"] or "").split(",") if s.strip()]
                e["concluidas_lista"] = _js.loads(e["etapas_concluidas"] or "[]")
            except Exception:
                e["etapas_lista"]     = []
                e["concluidas_lista"] = []

        STATUS_FINALIZADOS = {"concluida", "cancelado"}
        execucoes_ativas      = [e for e in execucoes if (e["status"] or "") not in STATUS_FINALIZADOS]
        execucoes_finalizadas = [e for e in execucoes if (e["status"] or "") in STATUS_FINALIZADOS]

        pastas = _pastas_rec(cur, cod_empresa)

        # anos disponíveis (atual ± 2)
        anos_disp = list(range(hoje.year - 2, hoje.year + 3))

    finally:
        cur.close(); conn.close()

    url_voltar = (url_for("projetos.recorrencias_mensais") if len(pastas) <= 1
                  else url_for("projetos.rec_executar"))

    return render_template(
        "projetos/rec_executar_pasta.html",
        nome_empresa=session.get("nome_empresa"),
        pasta=pasta,
        pastas=pastas,
        execucoes=execucoes,
        execucoes_ativas=execucoes_ativas,
        execucoes_finalizadas=execucoes_finalizadas,
        ano_sel=ano_sel,
        mes_sel=mes_sel,
        anos_disp=anos_disp,
        nomes_meses=NOMES_MESES,
        url_voltar=url_voltar,
    )


# ─── CONFIGURAÇÕES ───────────────────────────────────────────────────────────

@projetos_bp.route("/configuracoes", methods=["GET", "POST"])
def configuracoes_projetos():
    redir = _checar_acesso()
    if redir:
        return redir

    cod_empresa = str(session["cod_empresa"]).strip()
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            acao = request.form.get("acao")

            # ── Pastas Melhorias Contínuas ──
            if acao == "mc_incluir":
                nome  = (request.form.get("nome") or "").strip()
                ordem = int(request.form.get("ordem") or 0)
                if nome:
                    cur.execute("INSERT INTO projetos_mc_pastas (cod_empresa, nome, ordem) VALUES (%s,%s,%s)",
                                (cod_empresa, nome, ordem))
                    conn.commit()
                    flash("Pasta de Melhoria Contínua criada.", "success")

            elif acao == "mc_editar":
                id_ed = request.form.get("id_editar")
                nome  = (request.form.get("nome_editar") or "").strip()
                ordem = int(request.form.get("ordem_editar") or 0)
                if id_ed and nome:
                    cur.execute("UPDATE projetos_mc_pastas SET nome=%s, ordem=%s WHERE id=%s AND cod_empresa=%s",
                                (nome, ordem, id_ed, cod_empresa))
                    conn.commit()
                    flash("Pasta atualizada.", "success")

            elif acao == "mc_inativar":
                id_in = request.form.get("id_inativar")
                if id_in:
                    cur.execute("UPDATE projetos_mc_pastas SET ativo = NOT ativo WHERE id=%s AND cod_empresa=%s",
                                (id_in, cod_empresa))
                    conn.commit()
                    flash("Status da pasta alterado.", "success")

            elif acao == "mc_excluir":
                id_ex = request.form.get("id_excluir")
                if id_ex:
                    cur.execute("SELECT COUNT(*) AS n FROM projetos_mc_itens WHERE id_pasta=%s AND cod_empresa=%s",
                                (id_ex, cod_empresa))
                    if cur.fetchone()["n"] > 0:
                        flash("Esta pasta possui itens e não pode ser excluída. Use Inativar.", "error")
                    else:
                        cur.execute("DELETE FROM projetos_mc_pastas WHERE id=%s AND cod_empresa=%s", (id_ex, cod_empresa))
                        conn.commit()
                        flash("Pasta excluída.", "success")

            # ── Pastas Recorrências ──
            elif acao == "rec_incluir":
                nome  = (request.form.get("rec_nome") or "").strip()
                ordem = int(request.form.get("rec_ordem") or 0)
                if nome:
                    cur.execute("INSERT INTO projetos_rec_pastas (cod_empresa, nome, ordem) VALUES (%s,%s,%s)",
                                (cod_empresa, nome, ordem))
                    conn.commit()
                    flash("Pasta de Recorrência criada.", "success")

            elif acao == "rec_editar":
                id_ed = request.form.get("rec_id_editar")
                nome  = (request.form.get("rec_nome_editar") or "").strip()
                ordem = int(request.form.get("rec_ordem_editar") or 0)
                if id_ed and nome:
                    cur.execute("UPDATE projetos_rec_pastas SET nome=%s, ordem=%s WHERE id=%s AND cod_empresa=%s",
                                (nome, ordem, id_ed, cod_empresa))
                    conn.commit()
                    flash("Pasta de Recorrência atualizada.", "success")

            elif acao == "rec_inativar":
                id_in = request.form.get("rec_id_inativar")
                if id_in:
                    cur.execute("UPDATE projetos_rec_pastas SET ativo = NOT ativo WHERE id=%s AND cod_empresa=%s",
                                (id_in, cod_empresa))
                    conn.commit()
                    flash("Status da pasta alterado.", "success")

            elif acao == "rec_excluir":
                id_ex = request.form.get("rec_id_excluir")
                if id_ex:
                    cur.execute("SELECT COUNT(*) AS n FROM projetos_rec_modelos WHERE id_pasta=%s AND cod_empresa=%s",
                                (id_ex, cod_empresa))
                    if cur.fetchone()["n"] > 0:
                        flash("Esta pasta possui modelos e não pode ser excluída. Use Inativar.", "error")
                    else:
                        cur.execute("DELETE FROM projetos_rec_pastas WHERE id=%s AND cod_empresa=%s", (id_ex, cod_empresa))
                        conn.commit()
                        flash("Pasta excluída.", "success")

        cur.execute("SELECT id, nome, ordem, ativo FROM projetos_mc_pastas WHERE cod_empresa=%s ORDER BY ordem, nome", (cod_empresa,))
        pastas_mc = cur.fetchall()

        cur.execute("SELECT id, nome, ordem, ativo FROM projetos_rec_pastas WHERE cod_empresa=%s ORDER BY ordem, nome", (cod_empresa,))
        pastas_rec = cur.fetchall()

    finally:
        cur.close(); conn.close()

    return render_template(
        "projetos/configuracoes_projetos.html",
        nome_empresa=session.get("nome_empresa"),
        pastas_mc=pastas_mc,
        pastas_rec=pastas_rec,
        url_voltar=url_for("projetos.menu_projetos"),
    )


# ─── PROJECT JOURNAL ─────────────────────────────────────────────────────────

@projetos_bp.route("/project-journal", methods=["GET", "POST"])
def project_journal():
    redir = _checar_acesso()
    if redir:
        return redir

    cod_empresa = str(session["cod_empresa"]).strip()
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    erro = sucesso = None

    try:
        acao = request.form.get("acao") if request.method == "POST" else None

        if acao == "incluir":
            data_str  = (request.form.get("data") or "").strip()
            descricao = (request.form.get("descricao") or "").strip()
            if not data_str or not descricao:
                erro = "Preencha a data e a descrição."
            else:
                cur.execute("""
                    INSERT INTO project_journal (cod_empresa, data, descricao)
                    VALUES (%s, %s, %s)
                """, (cod_empresa, data_str, descricao))
                conn.commit()
                sucesso = "Registro adicionado."

        elif acao == "editar":
            id_reg    = request.form.get("id")
            data_str  = (request.form.get("data") or "").strip()
            descricao = (request.form.get("descricao") or "").strip()
            if not id_reg or not data_str or not descricao:
                erro = "Dados incompletos."
            else:
                cur.execute("""
                    UPDATE project_journal
                    SET data=%s, descricao=%s, atualizado_em=NOW()
                    WHERE id=%s AND cod_empresa=%s
                """, (data_str, descricao, id_reg, cod_empresa))
                conn.commit()
                sucesso = "Registro atualizado."

        elif acao == "excluir":
            id_reg = request.form.get("id")
            if id_reg:
                cur.execute("DELETE FROM project_journal WHERE id=%s AND cod_empresa=%s",
                            (id_reg, cod_empresa))
                conn.commit()
                sucesso = "Registro excluído."

        cur.execute("""
            SELECT id, data, descricao, criado_em
            FROM project_journal
            WHERE cod_empresa = %s
            ORDER BY data DESC, criado_em DESC
        """, (cod_empresa,))
        registros = cur.fetchall()

    finally:
        cur.close(); conn.close()

    return render_template(
        "projetos/project_journal.html",
        nome_empresa=session.get("nome_empresa"),
        registros=registros,
        erro=erro,
        sucesso=sucesso,
        hoje=date.today().isoformat(),
        url_voltar=url_for("projetos.menu_projetos"),
    )
