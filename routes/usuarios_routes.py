from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from psycopg2.extras import RealDictCursor
from werkzeug.security import generate_password_hash
from db import get_connection
from auth_helpers import admin_empresa_obrigatorio, superusuario_obrigatorio

usuarios_bp = Blueprint("usuarios", __name__, url_prefix="/usuarios")


# ----------------------------------------------------------
# FUNCAO AUXILIAR EXCLUSAO DE USUARIOS
# ----------------------------------------------------------
def usuario_pode_ser_excluido(cur, id_usuario, cod_empresa):
    # 1) não pode excluir superusuario
    cur.execute("""
        SELECT tipo_global
        FROM usuarios
        WHERE id_usuario = %s
    """, (id_usuario,))
    u = cur.fetchone()
    if not u:
        return False

    tipo_global = u["tipo_global"] if isinstance(u, dict) else u[0]
    if tipo_global == "superusuario":
        return False

    # 2) não pode excluir se for o único admin da empresa
    cur.execute("""
        SELECT COUNT(*) AS qt
        FROM usuarios_empresas
        WHERE cod_empresa = %s
          AND perfil_empresa = 'admin'
          AND ativo = TRUE
    """, (cod_empresa,))
    qt_admins = cur.fetchone()
    qt_admins = qt_admins["qt"] if isinstance(qt_admins, dict) else qt_admins[0]

    cur.execute("""
        SELECT perfil_empresa, ativo
        FROM usuarios_empresas
        WHERE id_usuario = %s
          AND cod_empresa = %s
    """, (id_usuario, cod_empresa))
    ue = cur.fetchone()

    if ue:
        perfil_empresa = ue["perfil_empresa"] if isinstance(ue, dict) else ue[0]
        ativo = ue["ativo"] if isinstance(ue, dict) else ue[1]
        if perfil_empresa == "admin" and ativo and qt_admins <= 1:
            return False

    return True


@usuarios_bp.route("/menu")
@admin_empresa_obrigatorio
def menu_usuarios():
    nome_empresa = session.get("nome_empresa", "")

    return render_template(
        "menu_usuarios.html",
        nome_empresa=nome_empresa,
        url_voltar=url_for("sistema.selecionar_sistema"),
        texto_voltar="← Voltar",
        mostrar_menu_modulo=False
    )


@usuarios_bp.route("/")
@admin_empresa_obrigatorio
def listar_usuarios():
    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT
                u.id_usuario,
                u.nome,
                u.email,
                u.ativo,
                u.tipo_global,
                ue.perfil_empresa
            FROM usuarios u
            JOIN usuarios_empresas ue
              ON ue.id_usuario = u.id_usuario
            WHERE ue.cod_empresa = %s
            ORDER BY u.nome
        """, (cod_empresa,))
        usuarios = cur.fetchall()

        for u in usuarios:
            u["pode_excluir"] = usuario_pode_ser_excluido(cur, u["id_usuario"], cod_empresa)

    finally:
        cur.close()
        conn.close()

    return render_template(
        "usuarios.html",
        usuarios=usuarios,
        nome_empresa=nome_empresa,
        url_voltar=url_for("usuarios.menu_usuarios"),
        texto_voltar="← Voltar",
        mostrar_menu_modulo=False
    )


# -----------------------------------------------------
# NOVO USUÁRIO
# -----------------------------------------------------
@usuarios_bp.route("/novo", methods=["GET", "POST"])
@admin_empresa_obrigatorio
def novo_usuario():
    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    if request.method == "GET":
        return render_template(
            "usuario_form.html",
            nome_empresa=nome_empresa,
            url_voltar=url_for("usuarios.listar_usuarios"),
            texto_voltar="← Voltar",
            mostrar_menu_modulo=False
        )

    nome = (request.form.get("nome") or "").strip()
    email = (request.form.get("email") or "").strip().lower()
    senha = request.form.get("senha") or ""
    perfil_empresa = (request.form.get("perfil_empresa") or "consulta").strip()
    ativo = True if request.form.get("ativo") == "on" else False

    if not nome or not email or not senha:
        flash("Preencha nome, email e senha.", "error")
        return render_template(
            "usuario_form.html",
            nome_empresa=nome_empresa,
            url_voltar=url_for("usuarios.listar_usuarios"),
            texto_voltar="← Voltar",
            mostrar_menu_modulo=False
        )

    if perfil_empresa not in ("admin", "operador", "consulta"):
        flash("Perfil inválido.", "error")
        return render_template(
            "usuario_form.html",
            nome_empresa=nome_empresa,
            url_voltar=url_for("usuarios.listar_usuarios"),
            texto_voltar="← Voltar",
            mostrar_menu_modulo=False
        )

    senha_hash = generate_password_hash(senha)

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT id_usuario
            FROM usuarios
            WHERE LOWER(email) = %s
        """, (email,))
        existente = cur.fetchone()

        if existente:
            id_usuario = existente["id_usuario"]

            cur.execute("""
                INSERT INTO usuarios_empresas (id_usuario, cod_empresa, perfil_empresa, ativo)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id_usuario, cod_empresa)
                DO UPDATE SET
                    perfil_empresa = EXCLUDED.perfil_empresa,
                    ativo = EXCLUDED.ativo
            """, (id_usuario, cod_empresa, perfil_empresa, ativo))

            cur.execute("""
                UPDATE usuarios
                   SET nome = %s,
                       ativo = %s
                 WHERE id_usuario = %s
            """, (nome, ativo, id_usuario))
        else:
            cur.execute("""
                INSERT INTO usuarios (
                    nome,
                    email,
                    senha_hash,
                    tipo_global,
                    ativo
                )
                VALUES (%s, %s, %s, 'normal', %s)
                RETURNING id_usuario
            """, (nome, email, senha_hash, ativo))

            id_usuario = cur.fetchone()["id_usuario"]

            cur.execute("""
                INSERT INTO usuarios_empresas (id_usuario, cod_empresa, perfil_empresa, ativo)
                VALUES (%s, %s, %s, %s)
            """, (id_usuario, cod_empresa, perfil_empresa, ativo))

        # marca automaticamente TODAS as filiais da empresa para o usuário
        cur.execute("""
            SELECT cod_filial
            FROM filiais
            WHERE cod_empresa = %s
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()

        for f in filiais:
            cur.execute("""
                INSERT INTO usuarios_filiais (id_usuario, cod_empresa, cod_filial, ativo)
                VALUES (%s, %s, %s, TRUE)
                ON CONFLICT (id_usuario, cod_empresa, cod_filial)
                DO UPDATE SET ativo = TRUE
            """, (id_usuario, cod_empresa, f["cod_filial"]))

        conn.commit()
        flash("Usuário salvo com sucesso.", "success")
        return redirect(url_for("usuarios.listar_usuarios"))

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao salvar usuário: {e}", "error")
        return render_template(
            "usuario_form.html",
            nome_empresa=nome_empresa,
            url_voltar=url_for("usuarios.listar_usuarios"),
            texto_voltar="← Voltar",
            mostrar_menu_modulo=False
        )

    finally:
        cur.close()
        conn.close()


@usuarios_bp.route("/toggle/<int:id_usuario>")
@admin_empresa_obrigatorio
def toggle_usuario(id_usuario):
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE usuarios_empresas
               SET ativo = NOT ativo
             WHERE id_usuario = %s
               AND cod_empresa = %s
        """, (id_usuario, cod_empresa))

        cur.execute("""
            UPDATE usuarios
               SET ativo = NOT ativo
             WHERE id_usuario = %s
        """, (id_usuario,))

        conn.commit()
        flash("Status do usuário atualizado.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Erro: {e}", "error")
    finally:
        cur.close()
        conn.close()

    return redirect(url_for("usuarios.listar_usuarios"))


# -----------------------------------------------------
# PERMISSÕES FILIAIS
# -----------------------------------------------------
@usuarios_bp.route("/permissoes-filiais")
@admin_empresa_obrigatorio
def permissoes_filiais():
    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    filiais = []
    usuarios = []
    vinculos = {}

    try:
        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall() or []

        for f in filiais:
            f["cod_filial"] = str(f["cod_filial"]).strip()

        cur.execute("""
            SELECT
                u.id_usuario,
                u.nome,
                u.email,
                ue.perfil_empresa,
                u.ativo
            FROM usuarios u
            JOIN usuarios_empresas ue
              ON ue.id_usuario = u.id_usuario
            WHERE ue.cod_empresa = %s
            ORDER BY u.nome
        """, (cod_empresa,))
        usuarios = cur.fetchall() or []

        cur.execute("""
            SELECT id_usuario, cod_filial, ativo
            FROM usuarios_filiais
            WHERE cod_empresa = %s
        """, (cod_empresa,))
        vinculos_raw = cur.fetchall() or []

        for v in vinculos_raw:
            id_usuario = v["id_usuario"]
            cod_filial = str(v["cod_filial"]).strip()
            ativo = bool(v["ativo"])

            if id_usuario not in vinculos:
                vinculos[id_usuario] = {}

            vinculos[id_usuario][cod_filial] = ativo

    finally:
        cur.close()
        conn.close()

    return render_template(
        "usuarios_filiais.html",
        nome_empresa=nome_empresa,
        cod_empresa=cod_empresa,
        usuarios=usuarios,
        filiais=filiais,
        vinculos=vinculos,
        url_voltar=url_for("usuarios.menu_usuarios"),
        texto_voltar="← Voltar",
        mostrar_menu_modulo=False
    )

@usuarios_bp.route("/ajax/toggle-filial", methods=["POST"])
@admin_empresa_obrigatorio
def toggle_filial_usuario():
    cod_empresa = str(session["cod_empresa"]).strip()

    id_usuario = request.form.get("id_usuario")
    cod_filial = (request.form.get("cod_filial") or "").strip()
    ativo = request.form.get("ativo") == "true"

    if not id_usuario or not cod_filial:
        return jsonify({"ok": False, "erro": "Dados inválidos"}), 400

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO usuarios_filiais (id_usuario, cod_empresa, cod_filial, ativo)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id_usuario, cod_empresa, cod_filial)
            DO UPDATE SET ativo = EXCLUDED.ativo
        """, (id_usuario, cod_empresa, cod_filial, ativo))

        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()


# -----------------------------------------------------
# AJAX
# -----------------------------------------------------
@usuarios_bp.route("/ajax/filiais-em-lote", methods=["POST"])
@admin_empresa_obrigatorio
def filiais_em_lote():
    cod_empresa = str(session["cod_empresa"]).strip()

    id_usuario = request.form.get("id_usuario")
    ativo = request.form.get("ativo") == "true"

    if not id_usuario:
        return jsonify({"ok": False, "erro": "Usuário inválido"}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT cod_filial
            FROM filiais
            WHERE cod_empresa = %s
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()

        for f in filiais:
            cur.execute("""
                INSERT INTO usuarios_filiais (id_usuario, cod_empresa, cod_filial, ativo)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id_usuario, cod_empresa, cod_filial)
                DO UPDATE SET ativo = EXCLUDED.ativo
            """, (id_usuario, cod_empresa, f["cod_filial"], ativo))

        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()

#-------------------------------------------
# ROTA PERMISSOES SISTEMAS
#-------------------------------------------

@usuarios_bp.route("/permissoes-sistema")
@admin_empresa_obrigatorio
def permissoes_sistema():
    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    usuarios = []
    permissoes = []
    vinculos = {}

    try:
        cur.execute("""
            SELECT
                u.id_usuario,
                u.nome,
                u.email,
                ue.perfil_empresa,
                u.ativo
            FROM usuarios u
            JOIN usuarios_empresas ue
              ON ue.id_usuario = u.id_usuario
            WHERE ue.cod_empresa = %s
            ORDER BY u.nome
        """, (cod_empresa,))
        usuarios = cur.fetchall() or []

        cur.execute("""
            SELECT sistema, opcao, descricao, ordem
            FROM permissoes_catalogo
            WHERE ativo = TRUE
            ORDER BY ordem, sistema, opcao
        """, ())
        permissoes = cur.fetchall() or []

        cur.execute("""
            SELECT id_usuario, sistema, opcao, ativo
            FROM usuarios_permissoes
            WHERE cod_empresa = %s
        """, (cod_empresa,))
        vinculos_raw = cur.fetchall() or []

        for v in vinculos_raw:
            id_usuario = v["id_usuario"]
            sistema = str(v["sistema"]).strip()
            opcao = str(v["opcao"]).strip()

            if id_usuario not in vinculos:
                vinculos[id_usuario] = {}

            vinculos[id_usuario][f"{sistema}|{opcao}"] = bool(v["ativo"])

    finally:
        cur.close()
        conn.close()

    return render_template(
        "usuarios_permissoes.html",
        nome_empresa=nome_empresa,
        cod_empresa=cod_empresa,
        usuarios=usuarios,
        permissoes=permissoes,
        vinculos=vinculos,
        url_voltar=url_for("usuarios.menu_usuarios"),
        texto_voltar="← Voltar",
        mostrar_menu_modulo=False
    )

@usuarios_bp.route("/editar/<int:id_usuario>", methods=["GET", "POST"])
@admin_empresa_obrigatorio
def editar_usuario(id_usuario):
    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "GET":
            cur.execute("""
                SELECT
                    u.id_usuario,
                    u.nome,
                    u.email,
                    u.ativo,
                    ue.perfil_empresa
                FROM usuarios u
                JOIN usuarios_empresas ue
                  ON ue.id_usuario = u.id_usuario
                WHERE u.id_usuario = %s
                  AND ue.cod_empresa = %s
            """, (id_usuario, cod_empresa))
            usuario = cur.fetchone()

            if not usuario:
                flash("Usuário não encontrado.", "error")
                return redirect(url_for("usuarios.listar_usuarios"))

            return render_template(
                "usuario_form.html",
                usuario=usuario,
                nome_empresa=nome_empresa,
                url_voltar=url_for("usuarios.listar_usuarios"),
                texto_voltar="← Voltar",
                mostrar_menu_modulo=False
            )

        nome = (request.form.get("nome") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        perfil_empresa = (request.form.get("perfil_empresa") or "consulta").strip()
        ativo = True if request.form.get("ativo") == "on" else False
        senha = request.form.get("senha") or ""

        if not nome or not email:
            flash("Preencha nome e email.", "error")
            return redirect(url_for("usuarios.editar_usuario", id_usuario=id_usuario))

        if perfil_empresa not in ("admin", "operador", "consulta"):
            flash("Perfil inválido.", "error")
            return redirect(url_for("usuarios.editar_usuario", id_usuario=id_usuario))

        cur.execute("""
            SELECT perfil_empresa, ativo
            FROM usuarios_empresas
            WHERE id_usuario = %s
              AND cod_empresa = %s
        """, (id_usuario, cod_empresa))
        atual = cur.fetchone()

        cur.execute("""
            SELECT COUNT(*) AS qt
            FROM usuarios_empresas
            WHERE cod_empresa = %s
              AND perfil_empresa = 'admin'
              AND ativo = TRUE
        """, (cod_empresa,))
        qt_admins = cur.fetchone()["qt"]

        if atual and atual["perfil_empresa"] == "admin" and atual["ativo"]:
            vai_perder_admin = (perfil_empresa != "admin") or (not ativo)
            if vai_perder_admin and qt_admins <= 1:
                flash("Não é permitido remover o único administrador ativo da empresa.", "error")
                return redirect(url_for("usuarios.editar_usuario", id_usuario=id_usuario))

        cur.execute("""
            UPDATE usuarios
               SET nome = %s,
                   email = %s,
                   ativo = %s
             WHERE id_usuario = %s
        """, (nome, email, ativo, id_usuario))

        if senha.strip():
            senha_hash = generate_password_hash(senha.strip())
            cur.execute("""
                UPDATE usuarios
                   SET senha_hash = %s
                 WHERE id_usuario = %s
            """, (senha_hash, id_usuario))

        cur.execute("""
            UPDATE usuarios_empresas
               SET perfil_empresa = %s,
                   ativo = %s
             WHERE id_usuario = %s
               AND cod_empresa = %s
        """, (perfil_empresa, ativo, id_usuario, cod_empresa))

        conn.commit()
        flash("Usuário atualizado com sucesso.", "success")
        return redirect(url_for("usuarios.listar_usuarios"))

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao atualizar usuário: {e}", "error")
        return redirect(url_for("usuarios.listar_usuarios"))

    finally:
        cur.close()
        conn.close()


@usuarios_bp.route("/ajax/excluir/<int:id_usuario>", methods=["POST"])
@admin_empresa_obrigatorio
def excluir_usuario_ajax(id_usuario):
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT tipo_global
            FROM usuarios
            WHERE id_usuario = %s
        """, (id_usuario,))
        u = cur.fetchone()

        if not u:
            return jsonify({"ok": False, "erro": "Usuário não encontrado"}), 404

        if u["tipo_global"] == "superusuario":
            return jsonify({"ok": False, "erro": "Superusuário não pode ser excluído"}), 400

        cur.execute("""
            SELECT COUNT(*) AS qt
            FROM usuarios_empresas
            WHERE cod_empresa = %s
              AND perfil_empresa = 'admin'
              AND ativo = TRUE
        """, (cod_empresa,))
        qt_admins = cur.fetchone()["qt"]

        cur.execute("""
            SELECT perfil_empresa, ativo
            FROM usuarios_empresas
            WHERE id_usuario = %s
              AND cod_empresa = %s
        """, (id_usuario, cod_empresa))
        ue = cur.fetchone()

        if ue and ue["perfil_empresa"] == "admin" and ue["ativo"] and qt_admins <= 1:
            return jsonify({"ok": False, "erro": "Não é permitido excluir o único admin"}), 400

        cur.execute("""
            DELETE FROM usuarios_permissoes
            WHERE id_usuario = %s
              AND cod_empresa = %s
        """, (id_usuario, cod_empresa))

        cur.execute("""
            DELETE FROM usuarios_filiais
            WHERE id_usuario = %s
              AND cod_empresa = %s
        """, (id_usuario, cod_empresa))

        cur.execute("""
            DELETE FROM usuarios_empresas
            WHERE id_usuario = %s
              AND cod_empresa = %s
        """, (id_usuario, cod_empresa))

        cur.execute("""
            SELECT COUNT(*) AS qt
            FROM usuarios_empresas
            WHERE id_usuario = %s
        """, (id_usuario,))
        qt = cur.fetchone()["qt"]

        if qt == 0:
            cur.execute("""
                DELETE FROM usuarios
                WHERE id_usuario = %s
            """, (id_usuario,))

        conn.commit()
        return jsonify({"ok": True})

    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 500

    finally:
        cur.close()
        conn.close()

@usuarios_bp.route("/ajax/toggle-permissao", methods=["POST"])
@admin_empresa_obrigatorio
def toggle_permissao_usuario():
    cod_empresa = str(session["cod_empresa"]).strip()

    id_usuario = request.form.get("id_usuario")
    sistema = str(request.form.get("sistema") or "").strip()
    opcao = str(request.form.get("opcao") or "").strip()
    ativo = request.form.get("ativo") == "true"

    if not id_usuario or not sistema or not opcao:
        return jsonify({"ok": False, "erro": "Dados inválidos"}), 400

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
            DO UPDATE SET ativo = EXCLUDED.ativo
        """, (id_usuario, cod_empresa, sistema, opcao, ativo))

        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@usuarios_bp.route("/ajax/permissoes-em-lote", methods=["POST"])
@admin_empresa_obrigatorio
def permissoes_em_lote():
    cod_empresa = str(session["cod_empresa"]).strip()

    id_usuario = request.form.get("id_usuario")
    ativo = request.form.get("ativo") == "true"

    if not id_usuario:
        return jsonify({"ok": False, "erro": "Usuário inválido"}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT sistema, opcao
            FROM permissoes_catalogo
            WHERE ativo = TRUE
        """)
        permissoes = cur.fetchall() or []

        for p in permissoes:
            cur.execute("""
                INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
                DO UPDATE SET ativo = EXCLUDED.ativo
            """, (id_usuario, cod_empresa, p["sistema"], p["opcao"], ativo))

        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# ------------------------------------------------
# CATALOGO PERMISSOES
# ------------------------------------------------        
@usuarios_bp.route("/catalogo-permissoes")
@superusuario_obrigatorio
def catalogo_permissoes():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT
                id_permissao_catalogo,
                sistema,
                opcao,
                descricao,
                ordem,
                ativo
            FROM permissoes_catalogo
            ORDER BY ordem, sistema, opcao
        """)
        permissoes = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return render_template(
        "catalogo_permissoes.html",
        permissoes=permissoes,
        url_voltar=url_for("usuarios.menu_usuarios"),
        texto_voltar="← Voltar",
        mostrar_menu_modulo=False
    )

@usuarios_bp.route("/catalogo-permissoes/novo", methods=["GET", "POST"])
@superusuario_obrigatorio
def novo_catalogo_permissao():
    if request.method == "GET":
        return render_template(
            "catalogo_permissao_form.html",
            url_voltar=url_for("usuarios.catalogo_permissoes"),
            texto_voltar="← Voltar",
            mostrar_menu_modulo=False
        )

    sistema = (request.form.get("sistema") or "").strip().upper()
    opcao = (request.form.get("opcao") or "").strip().upper()
    descricao = (request.form.get("descricao") or "").strip()
    ordem = request.form.get("ordem") or "0"
    ativo = True if request.form.get("ativo") == "on" else False

    if not sistema or not opcao or not descricao:
        flash("Preencha sistema, opção e descrição.", "error")
        return render_template(
            "catalogo_permissao_form.html",
            url_voltar=url_for("usuarios.catalogo_permissoes"),
            texto_voltar="← Voltar",
            mostrar_menu_modulo=False
        )

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO permissoes_catalogo (
                sistema,
                opcao,
                descricao,
                ordem,
                ativo
            )
            VALUES (%s, %s, %s, %s, %s)
        """, (sistema, opcao, descricao, int(ordem), ativo))

        conn.commit()
        flash("Permissão cadastrada com sucesso.", "success")
        return redirect(url_for("usuarios.catalogo_permissoes"))

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao salvar: {e}", "error")
        return render_template(
            "catalogo_permissao_form.html",
            url_voltar=url_for("usuarios.catalogo_permissoes"),
            texto_voltar="← Voltar",
            mostrar_menu_modulo=False
        )

    finally:
        cur.close()
        conn.close()

@usuarios_bp.route("/catalogo-permissoes/editar/<int:id_permissao_catalogo>", methods=["GET", "POST"])
@superusuario_obrigatorio
def editar_catalogo_permissao(id_permissao_catalogo):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "GET":
            cur.execute("""
                SELECT
                    id_permissao_catalogo,
                    sistema,
                    opcao,
                    descricao,
                    ordem,
                    ativo
                FROM permissoes_catalogo
                WHERE id_permissao_catalogo = %s
            """, (id_permissao_catalogo,))
            permissao = cur.fetchone()

            if not permissao:
                flash("Permissão não encontrada.", "error")
                return redirect(url_for("usuarios.catalogo_permissoes"))

            return render_template(
                "catalogo_permissao_form.html",
                permissao=permissao,
                url_voltar=url_for("usuarios.catalogo_permissoes"),
                texto_voltar="← Voltar",
                mostrar_menu_modulo=False
            )

        sistema = (request.form.get("sistema") or "").strip().upper()
        opcao = (request.form.get("opcao") or "").strip().upper()
        descricao = (request.form.get("descricao") or "").strip()
        ordem = request.form.get("ordem") or "0"
        ativo = True if request.form.get("ativo") == "on" else False

        if not sistema or not opcao or not descricao:
            flash("Preencha sistema, opção e descrição.", "error")
            return redirect(url_for("usuarios.editar_catalogo_permissao", id_permissao_catalogo=id_permissao_catalogo))

        cur.execute("""
            UPDATE permissoes_catalogo
               SET sistema = %s,
                   opcao = %s,
                   descricao = %s,
                   ordem = %s,
                   ativo = %s
             WHERE id_permissao_catalogo = %s
        """, (sistema, opcao, descricao, int(ordem), ativo, id_permissao_catalogo))

        conn.commit()
        flash("Permissão atualizada com sucesso.", "success")
        return redirect(url_for("usuarios.catalogo_permissoes"))

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao atualizar: {e}", "error")
        return redirect(url_for("usuarios.catalogo_permissoes"))

    finally:
        cur.close()
        conn.close()