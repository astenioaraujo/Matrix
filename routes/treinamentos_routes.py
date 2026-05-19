from flask import Blueprint, render_template, redirect, url_for, session, flash, request
from psycopg2.extras import RealDictCursor
from datetime import date

from db import get_connection
from security_helpers import permissao_obrigatoria, usuario_tem_permissao

treinamentos_bp = Blueprint(
    "treinamentos",
    __name__,
    url_prefix="/treinamentos"
)

# -----------------------------------------------------------
# MENU DE TREINAMENTOS
# -----------------------------------------------------------
@treinamentos_bp.route("/menu")
def menu_treinamentos():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_cadastrar_treinamentos = True
        pode_incluir_participantes = True
        pode_consultar_treinamentos = True
        pode_emitir_certificados = True
        pode_configuracoes_treinamentos = True
    else:
        if not usuario_tem_permissao(id_usuario, cod_empresa, "TREINAMENTOS", "MENU"):
            flash("Você não tem permissão para acessar Treinamentos.", "error")
            return redirect(url_for("sistema.selecionar_sistema"))

        pode_cadastrar_treinamentos = usuario_tem_permissao(id_usuario, cod_empresa, "TREINAMENTOS", "CADASTRAR_TREINAMENTOS")
        pode_incluir_participantes = usuario_tem_permissao(id_usuario, cod_empresa, "TREINAMENTOS", "INCLUIR_PARTICIPANTES")
        pode_consultar_treinamentos = usuario_tem_permissao(id_usuario, cod_empresa, "TREINAMENTOS", "CONSULTAR_TREINAMENTOS")
        pode_emitir_certificados = usuario_tem_permissao(id_usuario, cod_empresa, "TREINAMENTOS", "EMITIR_CERTIFICADOS")
        pode_configuracoes_treinamentos = usuario_tem_permissao(id_usuario, cod_empresa, "TREINAMENTOS", "CONFIGURACOES_TREINAMENTOS")

    session["sistema_ativo"] = "treinamentos"

    return render_template(
        "menu_treinamentos.html",
        nome_empresa=session.get("nome_empresa"),
        empresa_ativa=session.get("cod_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema"),
        pode_cadastrar_treinamentos=pode_cadastrar_treinamentos,
        pode_incluir_participantes=pode_incluir_participantes,
        pode_consultar_treinamentos=pode_consultar_treinamentos,
        pode_emitir_certificados=pode_emitir_certificados,
        pode_configuracoes_treinamentos=pode_configuracoes_treinamentos,
    )

# -----------------------------------------------------------
# CADASTRO DE TREINAMENTOS
#------------------------------------------------------------

@treinamentos_bp.route("/cadastrar", methods=["GET", "POST"])
@permissao_obrigatoria(
    "TREINAMENTOS",
    "CADASTRAR_TREINAMENTOS",
    redirecionar_para="treinamentos.menu_treinamentos",
)

def cadastrar_treinamentos():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa")

    hoje = date.today()
    ano_sel = (request.args.get("ano") or str(hoje.year)).strip()

    data_ini = f"{ano_sel}-01-01"
    data_fim = f"{int(ano_sel) + 1}-01-01"

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            descricao = (request.form.get("descricao") or "").strip()
            data_treinamento = request.form.get("data_treinamento")
            texto = (request.form.get("texto") or "").strip()
            carga_horaria = (request.form.get("carga_horaria") or "").replace(",", ".").strip()
            instrutor = (request.form.get("instrutor") or "").strip()
            validade_meses = (request.form.get("validade_meses") or "").strip()

            if not descricao or not data_treinamento:
                flash("Informe descrição e data do treinamento.", "error")
                return redirect(url_for("treinamentos.cadastrar_treinamentos"))

            cur.execute("""
                INSERT INTO treinamentos (
                    cod_empresa,
                    descricao,
                    data_treinamento,
                    texto,
                    carga_horaria,
                    instrutor,
                    validade_meses,
                    criado_em,
                    atualizado_em
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING id
            """, (
                cod_empresa,
                descricao,
                data_treinamento,
                texto,
                carga_horaria if carga_horaria else None,
                instrutor,
                validade_meses if validade_meses else None,
            ))

            novo_id = cur.fetchone()["id"]

            cur.execute("""
                UPDATE treinamentos
                SET cod_treinamento = %s,
                    atualizado_em = NOW()
                WHERE id = %s
            """, (
                str(novo_id),
                novo_id,
            ))

            conn.commit()
            flash("Treinamento cadastrado com sucesso.", "success")

            return redirect(url_for(
                "treinamentos.cadastrar_treinamentos",
                ano=data_treinamento[:4],
            ))

        cur.execute("""
            SELECT
                id,
                cod_treinamento,
                descricao,
                data_treinamento,
                texto,
                carga_horaria,
                instrutor,
                validade_meses
            FROM treinamentos
            WHERE cod_empresa = %s
              AND data_treinamento >= %s
              AND data_treinamento < %s
            ORDER BY data_treinamento DESC, id DESC
        """, (cod_empresa, data_ini, data_fim))

        treinamentos_mes = cur.fetchall() or []

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao cadastrar treinamento: {e}", "error")
        treinamentos_mes = []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "cadastrar_treinamentos.html",
        nome_empresa=nome_empresa,
        treinamentos_mes=treinamentos_mes,
        ano_sel=ano_sel,
        today=hoje.strftime("%Y-%m-%d"),
        url_voltar=url_for("treinamentos.menu_treinamentos"),
        texto_voltar="← Voltar",
    )

@treinamentos_bp.route("/<int:id_treinamento>/editar")
def editar_treinamento(id_treinamento):
    flash("Edição de treinamento será criada na próxima etapa.", "success")
    return redirect(url_for("treinamentos.cadastrar_treinamentos"))

@treinamentos_bp.route("/<int:id_treinamento>/excluir", methods=["POST"])
def excluir_treinamento(id_treinamento):
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            DELETE FROM treinamentos
            WHERE id = %s
              AND cod_empresa = %s
        """, (id_treinamento, cod_empresa))

        conn.commit()
        flash("Treinamento excluído com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao excluir treinamento: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("treinamentos.cadastrar_treinamentos"))

# ---------------------------------------
# ATUALIZAR TREINAMENTO
# ---------------------------------------
@treinamentos_bp.route("/<int:id_treinamento>/atualizar", methods=["POST"])
def atualizar_treinamento(id_treinamento):
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    descricao = (request.form.get("descricao") or "").strip()
    data_treinamento = request.form.get("data_treinamento")
    carga_horaria = (request.form.get("carga_horaria") or "").replace(",", ".").strip()
    instrutor = (request.form.get("instrutor") or "").strip()
    validade_meses = (request.form.get("validade_meses") or "").strip()

    if not descricao or not data_treinamento:
        flash("Informe descrição e data do treinamento.", "error")
        return redirect(url_for("treinamentos.cadastrar_treinamentos"))

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE treinamentos
            SET
                descricao = %s,
                data_treinamento = %s,
                carga_horaria = %s,
                instrutor = %s,
                validade_meses = %s,
                atualizado_em = NOW()
            WHERE id = %s
              AND cod_empresa = %s
        """, (
            descricao,
            data_treinamento,
            carga_horaria if carga_horaria else None,
            instrutor,
            validade_meses if validade_meses else None,
            id_treinamento,
            cod_empresa,
        ))

        conn.commit()
        flash("Treinamento atualizado com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao atualizar treinamento: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for(
        "treinamentos.cadastrar_treinamentos",
        ano=data_treinamento[:4]
    ))

# -----------------------------------------------------------
# DESCRITIVO DO TREINAMENTO
# -----------------------------------------------------------
@treinamentos_bp.route("/<int:id_treinamento>/descritivo", methods=["GET", "POST"])
def descritivo_treinamento(id_treinamento):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            texto = request.form.get("texto") or ""

            cur.execute("""
                UPDATE treinamentos
                   SET texto = %s,
                       atualizado_em = NOW()
                 WHERE id = %s
                   AND cod_empresa = %s
            """, (texto, id_treinamento, cod_empresa))

            conn.commit()
            flash("Descritivo salvo com sucesso.", "success")

            return redirect(url_for("treinamentos.cadastrar_treinamentos"))

        cur.execute("""
            SELECT
                id,
                cod_treinamento,
                descricao,
                data_treinamento,
                texto
            FROM treinamentos
            WHERE id = %s
              AND cod_empresa = %s
        """, (id_treinamento, cod_empresa))

        treinamento = cur.fetchone()

        if not treinamento:
            flash("Treinamento não encontrado.", "error")
            return redirect(url_for("treinamentos.cadastrar_treinamentos"))

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao acessar descritivo: {e}", "error")
        return redirect(url_for("treinamentos.cadastrar_treinamentos"))

    finally:
        cur.close()
        conn.close()

    return render_template(
        "descritivo_treinamento.html",
        treinamento=treinamento,
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("treinamentos.cadastrar_treinamentos"),
        texto_voltar="← Voltar",
    )