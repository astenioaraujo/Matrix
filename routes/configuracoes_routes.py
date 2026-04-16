from flask import Blueprint, render_template, session, redirect, url_for, request, flash
import psycopg2
from psycopg2.extras import RealDictCursor

configuracoes_bp = Blueprint("configuracoes", __name__, url_prefix="/configuracoes")


def get_connection():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres.uaafkuovkzkozmscyapw",
        password="DataMatrix@1962#",
        host="aws-1-us-east-1.pooler.supabase.com",
        port=6543,
        sslmode="require"
    )


@configuracoes_bp.route("/menu")
def menu_configuracoes():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    return render_template(
        "menu_configuracoes.html",
        nome_empresa=session.get("nome_empresa", "")
    )


@configuracoes_bp.route("/filiais")
def config_filiais():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT
                f.cod_filial,
                f.nome_filial,
                f.nome_filial_importacao,
                f.ativo,
                COUNT(l.id_lancamento) AS qt_lancamentos
            FROM filiais f
            LEFT JOIN lancamentos l
                ON l.cod_empresa = f.cod_empresa
               AND l.cod_filial = f.cod_filial
            WHERE f.cod_empresa = %s
            GROUP BY
                f.cod_filial,
                f.nome_filial,
                f.nome_filial_importacao,
                f.ativo
            ORDER BY f.cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return render_template(
        "config_filiais.html",
        nome_empresa=session.get("nome_empresa", ""),
        cod_empresa=cod_empresa,
        filiais=filiais
    )


@configuracoes_bp.route("/filiais/novo", methods=["POST"])
def config_filiais_novo():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT COALESCE(MAX(cod_filial), 0) + 1 AS proximo
            FROM filiais
            WHERE cod_empresa = %s
        """, (cod_empresa,))
        proximo = cur.fetchone()["proximo"]

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
            proximo,
            "",
            None,
            True
        ))

        conn.commit()
        flash("Nova filial criada com sucesso.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Erro ao criar filial: {e}", "error")
    finally:
        cur.close()
        conn.close()

    return redirect(url_for("configuracoes.config_filiais"))


@configuracoes_bp.route("/filiais/salvar", methods=["POST"])
def config_filiais_salvar():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    cod_filial_original = (request.form.get("cod_filial_original") or "").strip()
    cod_filial = (request.form.get("cod_filial") or "").strip()
    nome_filial = (request.form.get("nome_filial") or "").strip()
    nome_filial_importacao = (request.form.get("nome_filial_importacao") or "").strip()
    ativo = request.form.get("ativo") == "on"

    if not cod_filial:
        flash("O código da filial é obrigatório.", "error")
        return redirect(url_for("configuracoes.config_filiais"))

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE filiais
            SET
                cod_filial = %s,
                nome_filial = %s,
                nome_filial_importacao = %s,
                ativo = %s
            WHERE cod_empresa = %s
              AND cod_filial = %s
        """, (
            cod_filial,
            nome_filial,
            nome_filial_importacao if nome_filial_importacao else None,
            ativo,
            cod_empresa,
            cod_filial_original
        ))

        conn.commit()
        flash("Filial salva com sucesso.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Erro ao salvar filial: {e}", "error")
    finally:
        cur.close()
        conn.close()

    return redirect(url_for("configuracoes.config_filiais"))


@configuracoes_bp.route("/filiais/excluir", methods=["POST"])
def config_filiais_excluir():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    cod_filial = (request.form.get("cod_filial") or "").strip()

    if not cod_filial:
        flash("Filial inválida.", "error")
        return redirect(url_for("configuracoes.config_filiais"))

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT COUNT(*) AS qt
            FROM lancamentos
            WHERE cod_empresa = %s
              AND cod_filial = %s
        """, (cod_empresa, cod_filial))
        qt = cur.fetchone()["qt"]

        if qt > 0:
            flash(f"Não é possível excluir. Esta filial possui {qt} lançamento(s).", "error")
            return redirect(url_for("configuracoes.config_filiais"))

        cur.execute("""
            DELETE FROM filiais
            WHERE cod_empresa = %s
              AND cod_filial = %s
        """, (cod_empresa, cod_filial))

        conn.commit()
        flash("Filial excluída com sucesso.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Erro ao excluir filial: {e}", "error")
    finally:
        cur.close()
        conn.close()

    return redirect(url_for("configuracoes.config_filiais"))


@configuracoes_bp.route("/usuarios")
def config_usuarios():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    return "Tela de Cadastro de Usuários"