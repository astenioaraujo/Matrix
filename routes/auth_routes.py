from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from werkzeug.security import check_password_hash
from psycopg2.extras import RealDictCursor
from db import get_connection

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        senha = request.form.get("senha") or ""

        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        try:
            cur.execute("""
                SELECT id_usuario, nome, email, senha_hash, tipo_global, ativo
                FROM usuarios
                WHERE LOWER(email) = %s
            """, (email,))
            usuario = cur.fetchone()

            if not usuario:
                flash("Usuário não encontrado.", "error")
                return render_template("login.html")

            if not usuario["ativo"]:
                flash("Usuário inativo.", "error")
                return render_template("login.html")

            if not check_password_hash(usuario["senha_hash"], senha):
                flash("Senha inválida.", "error")
                return render_template("login.html")

            session.clear()
            session["id_usuario"] = usuario["id_usuario"]
            session["nome_usuario"] = usuario["nome"]
            session["email_usuario"] = usuario["email"]
            session["tipo_global"] = usuario["tipo_global"]

            if usuario["tipo_global"] == "superusuario":
                cur.execute("""
                    SELECT cod_empresa, nome_fantasia
                    FROM empresas
                    ORDER BY cod_empresa
                """)
                empresas = cur.fetchall()

                session["empresas_disponiveis"] = [
                    {
                        "cod_empresa": str(e["cod_empresa"]).strip(),
                        "nome_fantasia": e["nome_fantasia"],
                        "perfil_empresa": "admin"
                    }
                    for e in empresas
                ]
            else:
                cur.execute("""
                    SELECT e.cod_empresa, e.nome_fantasia, ue.perfil_empresa
                    FROM usuarios_empresas ue
                    JOIN empresas e
                      ON e.cod_empresa = ue.cod_empresa
                    WHERE ue.id_usuario = %s
                      AND ue.ativo = TRUE
                    ORDER BY e.cod_empresa
                """, (usuario["id_usuario"],))
                empresas = cur.fetchall()

                if not empresas:
                    flash("Usuário sem empresa vinculada.", "error")
                    session.clear()
                    return render_template("login.html")

                session["empresas_disponiveis"] = [
                    {
                        "cod_empresa": str(e["cod_empresa"]).strip(),
                        "nome_fantasia": e["nome_fantasia"],
                        "perfil_empresa": e["perfil_empresa"]
                    }
                    for e in empresas
                ]

            if len(session["empresas_disponiveis"]) == 1:
                empresa = session["empresas_disponiveis"][0]
                session["cod_empresa"] = empresa["cod_empresa"]
                session["nome_empresa"] = empresa["nome_fantasia"]
                session["perfil_empresa"] = empresa["perfil_empresa"]
                return redirect(url_for("sistema.selecionar_sistema"))

            return redirect(url_for("auth.escolher_empresa"))

        finally:
            cur.close()
            conn.close()

    return render_template("login.html")


@auth_bp.route("/escolher_empresa", methods=["GET", "POST"])
def escolher_empresa():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    empresas = session.get("empresas_disponiveis", [])

    if not empresas:
        flash("Nenhuma empresa disponível para este usuário.", "error")
        return redirect(url_for("auth.logout"))

    if request.method == "POST":
        cod = (request.form.get("cod_empresa") or "").strip()

        empresa = next((e for e in empresas if e["cod_empresa"] == cod), None)

        if not empresa:
            flash("Empresa inválida.", "error")
            return render_template("escolher_empresa.html", empresas=empresas)

        session["cod_empresa"] = empresa["cod_empresa"]
        session["nome_empresa"] = empresa["nome_fantasia"]
        session["perfil_empresa"] = empresa["perfil_empresa"]

        return redirect(url_for("sistema.selecionar_sistema"))

    return render_template("escolher_empresa.html", empresas=empresas)


@auth_bp.route("/trocar_empresa")
def trocar_empresa():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    session.pop("cod_empresa", None)
    session.pop("nome_empresa", None)
    session.pop("perfil_empresa", None)

    return redirect(url_for("auth.escolher_empresa"))


@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.index"))