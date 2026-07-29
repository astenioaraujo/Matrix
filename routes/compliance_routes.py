from flask import (
    Blueprint, render_template, session, redirect, url_for, request, flash
)
from psycopg2.extras import RealDictCursor
from security_helpers import permissao_obrigatoria
from db import get_connection

compliance_bp = Blueprint("compliance", __name__)


# ---------------------------------------
# MENU COMPLIANCE
# ---------------------------------------
@compliance_bp.route("/menu")
@permissao_obrigatoria(
    "COMPLIANCE",
    "MENU",
    redirecionar_para="sistema.selecionar_sistema",
)
def menu_compliance():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    from security_helpers import usuario_tem_permissao

    id_usuario  = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_cadastrar = pode_consultar = True
    else:
        pode_cadastrar = usuario_tem_permissao(
            id_usuario, cod_empresa, "COMPLIANCE", "COMPLIANCES"
        )
        pode_consultar = usuario_tem_permissao(
            id_usuario, cod_empresa, "COMPLIANCE", "CONSULTAR_COMPLIANCES"
        )

    return render_template(
        "menu_compliance.html",
        cod_empresa=cod_empresa,
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema"),
        texto_voltar="← Voltar",
        pode_cadastrar=pode_cadastrar,
        pode_consultar=pode_consultar,
    )


# ---------------------------------------
# CADASTRAR COMPLIANCE
# ---------------------------------------
@compliance_bp.route("/compliances", methods=["GET", "POST"])
@permissao_obrigatoria(
    "COMPLIANCE",
    "COMPLIANCES",
    redirecionar_para="compliance.menu_compliance",
)
def compliances():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            titulo = (request.form.get("titulo") or "").strip()
            descricao = (request.form.get("descricao") or "").strip()

            if not titulo or not descricao:
                flash("Informe título e descrição.", "error")
                return redirect(url_for("compliance.compliances"))

            cur.execute("""
                INSERT INTO compliances (cod_empresa, titulo, descricao, ativo)
                VALUES (%s, %s, %s, TRUE)
            """, (cod_empresa, titulo, descricao))

            conn.commit()
            flash("Compliance cadastrada com sucesso.", "success")
            return redirect(url_for("compliance.compliances"))

        cur.execute("""
            SELECT id, titulo, descricao, ativo
            FROM compliances
            WHERE cod_empresa = %s
            ORDER BY ativo DESC, titulo
        """, (cod_empresa,))
        compliances_lista = cur.fetchall() or []

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao processar compliances: {e}", "error")
        compliances_lista = []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "compliances.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        compliances=compliances_lista,
        url_voltar=url_for("compliance.menu_compliance"),
        texto_voltar="← Voltar",
    )


# ------------------------------------------
# EDITAR COMPLIANCE
# ------------------------------------------
@compliance_bp.route("/compliances/<int:id_compliance>/editar", methods=["POST"])
@permissao_obrigatoria(
    "COMPLIANCE",
    "COMPLIANCES",
    redirecionar_para="compliance.menu_compliance",
)
def editar_compliance(id_compliance):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    titulo = (request.form.get("titulo") or "").strip()
    descricao = (request.form.get("descricao") or "").strip()
    ativo = True if request.form.get("ativo") == "on" else False

    if not titulo or not descricao:
        flash("Informe título e descrição.", "error")
        return redirect(url_for("compliance.compliances"))

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE compliances
            SET titulo = %s,
                descricao = %s,
                ativo = %s,
                atualizado_em = NOW()
            WHERE id = %s
              AND cod_empresa = %s
        """, (titulo, descricao, ativo, id_compliance, cod_empresa))

        conn.commit()
        flash("Compliance atualizada com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao atualizar compliance: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("compliance.compliances"))


# ------------------------------------------
# CONSULTAR COMPLIANCES
# ------------------------------------------
@compliance_bp.route("/compliances/consultar")
@permissao_obrigatoria(
    "COMPLIANCE",
    "CONSULTAR_COMPLIANCES",
    redirecionar_para="compliance.menu_compliance",
)
def consultar_compliances():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    busca = (request.args.get("busca") or "").strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        filtros = ["cod_empresa = %s", "ativo = TRUE"]
        params = [cod_empresa]

        if busca:
            filtros.append("(titulo ILIKE %s OR descricao ILIKE %s)")
            params.append(f"%{busca}%")
            params.append(f"%{busca}%")

        where_sql = " AND ".join(filtros)

        cur.execute(f"""
            SELECT id, titulo, descricao
            FROM compliances
            WHERE {where_sql}
            ORDER BY titulo
        """, params)

        compliances_lista = cur.fetchall() or []

    except Exception as e:
        flash(f"Erro ao consultar compliances: {e}", "error")
        compliances_lista = []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "consultar_compliances.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        compliances=compliances_lista,
        busca=busca,
        url_voltar=url_for("compliance.menu_compliance"),
        texto_voltar="← Voltar",
    )
