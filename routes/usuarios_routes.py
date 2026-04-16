from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from psycopg2.extras import RealDictCursor
from werkzeug.security import generate_password_hash
from db import get_connection
from auth_helpers import admin_empresa_obrigatorio

usuarios_bp = Blueprint("usuarios", __name__, url_prefix="/usuarios")


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
                ue.perfil_empresa
            FROM usuarios u
            JOIN usuarios_empresas ue
              ON ue.id_usuario = u.id_usuario
            WHERE ue.cod_empresa = %s
            ORDER BY u.nome
        """, (cod_empresa,))
        usuarios = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return render_template(
        "usuarios.html",
        usuarios=usuarios,
        nome_empresa=nome_empresa
    )


@usuarios_bp.route("/novo", methods=["GET", "POST"])
@admin_empresa_obrigatorio
def novo_usuario():
    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    if request.method == "GET":
        return render_template("usuario_form.html", nome_empresa=nome_empresa)

    nome = (request.form.get("nome") or "").strip()
    email = (request.form.get("email") or "").strip().lower()
    senha = request.form.get("senha") or ""
    perfil_empresa = (request.form.get("perfil_empresa") or "consulta").strip()
    ativo = True if request.form.get("ativo") == "on" else False

    if not nome or not email or not senha:
        flash("Preencha nome, email e senha.", "error")
        return render_template("usuario_form.html", nome_empresa=nome_empresa)

    if perfil_empresa not in ("admin", "operador", "consulta"):
        flash("Perfil inválido.", "error")
        return render_template("usuario_form.html", nome_empresa=nome_empresa)

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

        conn.commit()
        flash("Usuário salvo com sucesso.", "success")
        return redirect(url_for("usuarios.listar_usuarios"))

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao salvar usuário: {e}", "error")
        return render_template("usuario_form.html", nome_empresa=nome_empresa)

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