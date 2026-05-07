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
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("sistema.selecionar_sistema"),
        texto_voltar="← Voltar"
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
        filiais=filiais,
        url_voltar=url_for("configuracoes.menu_configuracoes"),
        texto_voltar="← Voltar"
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

    return redirect(url_for("configuracoes.menu_configuracoes"))


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

# =========================================
# AREAS
# =========================================

@configuracoes_bp.route("/areas")
def config_areas():

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:

        cur.execute("""
            SELECT
                id_area,
                nome_area,
                ativo
            FROM areas
            WHERE cod_empresa = %s
            ORDER BY nome_area
        """, (cod_empresa,))

        areas = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "config_areas.html",
        nome_empresa=session.get("nome_empresa", ""),
        cod_empresa=cod_empresa,
        areas=areas,
        url_voltar=url_for("configuracoes.menu_configuracoes"),
        texto_voltar="← Voltar"
    )


@configuracoes_bp.route("/areas/novo", methods=["POST"])
def config_areas_novo():

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:

        cur.execute("""
            INSERT INTO areas (
                cod_empresa,
                nome_area,
                ativo
            )
            VALUES (%s, %s, TRUE)
        """, (
            cod_empresa,
            "Nova Área",
        ))

        conn.commit()

        flash("Área criada com sucesso.", "success")

    except Exception as e:

        conn.rollback()
        flash(f"Erro ao criar área: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("configuracoes.config_areas"))


@configuracoes_bp.route("/areas/salvar", methods=["POST"])
def config_areas_salvar():

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    id_area = (request.form.get("id_area") or "").strip()
    nome_area = (request.form.get("nome_area") or "").strip()
    ativo = request.form.get("ativo") == "on"

    if not id_area:
        flash("Área inválida.", "error")
        return redirect(url_for("configuracoes.config_areas"))

    conn = get_connection()
    cur = conn.cursor()

    try:

        cur.execute("""
            UPDATE areas
            SET
                nome_area = %s,
                ativo = %s,
                atualizado_em = NOW()
            WHERE cod_empresa = %s
              AND id_area = %s
        """, (
            nome_area,
            ativo,
            cod_empresa,
            id_area
        ))

        conn.commit()

        flash("Área salva com sucesso.", "success")

    except Exception as e:

        conn.rollback()
        flash(f"Erro ao salvar área: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("configuracoes.config_areas"))


@configuracoes_bp.route("/areas/excluir", methods=["POST"])
def config_areas_excluir():

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    id_area = (request.form.get("id_area") or "").strip()

    conn = get_connection()
    cur = conn.cursor()

    try:

        cur.execute("""
            DELETE FROM areas
            WHERE cod_empresa = %s
              AND id_area = %s
        """, (
            cod_empresa,
            id_area
        ))

        conn.commit()

        flash("Área excluída com sucesso.", "success")

    except Exception as e:

        conn.rollback()
        flash(f"Erro ao excluir área: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("configuracoes.config_areas"))


# =========================================
# CONFIGURACAO AREAS x FILIAIS
# =========================================

@configuracoes_bp.route("/areas-filiais")
def config_areas_filiais():

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:

        cur.execute("""
            SELECT
                id_area,
                nome_area
            FROM areas
            WHERE cod_empresa = %s
              AND ativo = TRUE
            ORDER BY nome_area
        """, (cod_empresa,))

        areas = cur.fetchall() or []

        cur.execute("""
            SELECT
                cod_filial,
                nome_filial
            FROM filiais
            WHERE cod_empresa = %s
            ORDER BY cod_filial
        """, (cod_empresa,))

        filiais = cur.fetchall() or []

        cur.execute("""
            SELECT
                af.id_area_filial,
                af.id_area,
                af.cod_filial,
                af.ordem,
                a.nome_area,
                f.nome_filial
            FROM areas_filiais af

            LEFT JOIN areas a
              ON a.id_area = af.id_area

            LEFT JOIN filiais f
              ON f.cod_empresa = af.cod_empresa
             AND f.cod_filial = af.cod_filial

            WHERE af.cod_empresa = %s

            ORDER BY
                a.nome_area,
                af.ordem,
                af.cod_filial
        """, (cod_empresa,))

        vinculacoes = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "config_areas_filiais.html",
        nome_empresa=session.get("nome_empresa", ""),
        cod_empresa=cod_empresa,
        areas=areas,
        filiais=filiais,
        vinculacoes=vinculacoes,
        url_voltar=url_for("configuracoes.menu_configuracoes"),
        texto_voltar="← Voltar"
    )


@configuracoes_bp.route("/areas-filiais/salvar", methods=["POST"])
def config_areas_filiais_salvar():

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    id_area = (request.form.get("id_area") or "").strip()
    cod_filial = (request.form.get("cod_filial") or "").strip()
    ordem = (request.form.get("ordem") or "10").strip()

    conn = get_connection()
    cur = conn.cursor()

    try:

        cur.execute("""
            INSERT INTO areas_filiais (
                cod_empresa,
                id_area,
                cod_filial,
                ordem
            )
            VALUES (%s, %s, %s, %s)

            ON CONFLICT (cod_empresa, cod_filial)
            DO UPDATE SET
                id_area = EXCLUDED.id_area,
                ordem = EXCLUDED.ordem,
                atualizado_em = NOW()
        """, (
            cod_empresa,
            id_area,
            cod_filial,
            ordem
        ))

        conn.commit()

        flash("Configuração salva com sucesso.", "success")

    except Exception as e:

        conn.rollback()
        flash(f"Erro ao salvar configuração: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("configuracoes.config_areas_filiais"))

@configuracoes_bp.route("/areas-filiais/alterar", methods=["POST"])
def config_areas_filiais_alterar():

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    id_area_filial = (request.form.get("id_area_filial") or "").strip()
    ordem = (request.form.get("ordem") or "10").strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE areas_filiais
            SET
                ordem = %s,
                atualizado_em = NOW()
            WHERE cod_empresa = %s
              AND id_area_filial = %s
        """, (
            ordem,
            cod_empresa,
            id_area_filial
        ))

        conn.commit()
        flash("Ordem alterada com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao alterar ordem: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("configuracoes.config_areas_filiais"))


@configuracoes_bp.route("/areas-filiais/excluir", methods=["POST"])
def config_areas_filiais_excluir():

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    id_area_filial = (request.form.get("id_area_filial") or "").strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            DELETE FROM areas_filiais
            WHERE cod_empresa = %s
              AND id_area_filial = %s
        """, (
            cod_empresa,
            id_area_filial
        ))

        conn.commit()
        flash("Configuração excluída com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao excluir configuração: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("configuracoes.config_areas_filiais"))