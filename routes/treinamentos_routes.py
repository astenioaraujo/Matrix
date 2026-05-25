from flask import Blueprint, render_template, redirect, url_for, session, flash, request, current_app
from psycopg2.extras import RealDictCursor
from datetime import date

import os
from werkzeug.utils import secure_filename

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
        pode_vincular_participantes = True
        pode_consultar_treinamentos = True
        pode_emitir_certificados = True
        pode_configuracoes_treinamentos = True
    else:
        if not usuario_tem_permissao(id_usuario, cod_empresa, "TREINAMENTOS", "MENU"):
            flash("Você não tem permissão para acessar Treinamentos.", "error")
            return redirect(url_for("sistema.selecionar_sistema"))

        pode_cadastrar_treinamentos = usuario_tem_permissao(id_usuario, cod_empresa, "TREINAMENTOS", "CADASTRAR_TREINAMENTOS")
        pode_vincular_participantes = usuario_tem_permissao(id_usuario, cod_empresa, "TREINAMENTOS", "INCLUIR_PARTICIPANTES")
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
        pode_vincular_participantes=pode_vincular_participantes,
        pode_consultar_treinamentos=pode_consultar_treinamentos,
        pode_emitir_certificados=pode_emitir_certificados,
        pode_configuracoes_treinamentos=pode_configuracoes_treinamentos,
    )

# -----------------------------------------------------------
# CADASTRO DE TREINAMENTOS
# -----------------------------------------------------------

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
            carga_horaria = (request.form.get("carga_horaria") or "").replace(",", ".").strip()
            instrutor = (request.form.get("instrutor") or "").strip()
            validade_meses = (request.form.get("validade_meses") or "").strip()

            texto = (request.form.get("texto") or "").strip()
            texto_certificado = (request.form.get("texto_certificado") or "").strip()
            instituicao_certificado = (request.form.get("instituicao_certificado") or "").strip()

            if not descricao or not data_treinamento:
                flash("Informe descrição e data do treinamento.", "error")
                return redirect(url_for("treinamentos.cadastrar_treinamentos"))

            cur.execute("""
                INSERT INTO treinamentos (
                    cod_empresa,
                    descricao,
                    data_treinamento,
                    texto,
                    texto_certificado,
                    instituicao_certificado,
                    carga_horaria,
                    instrutor,
                    validade_meses,
                    criado_em,
                    atualizado_em
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING id_treinamento
            """, (
                cod_empresa,
                descricao,
                data_treinamento,
                texto,
                texto_certificado,
                instituicao_certificado,
                carga_horaria if carga_horaria else None,
                instrutor,
                validade_meses if validade_meses else None,
            ))

            novo_id = cur.fetchone()["id_treinamento"]

            cur.execute("""
                UPDATE treinamentos
                SET cod_treinamento = %s,
                    atualizado_em = NOW()
                WHERE id_treinamento = %s
            """, (
                str(novo_id),
                novo_id,
            ))

            conn.commit()
            flash("Treinamento cadastrado com sucesso.", "success")

            return redirect(url_for(
                "treinamentos.editar_treinamento",
                id_treinamento=novo_id
            ))

        cur.execute("""
            SELECT
                t.id_treinamento,
                t.id_treinamento AS id,
                t.cod_treinamento,
                t.descricao,
                t.data_treinamento,
                COUNT(tp.id_funcionario) AS qtd_participantes,
                t.texto,
                t.texto_certificado,
                t.instituicao_certificado,
                t.carga_horaria,
                t.instrutor,
                t.validade_meses
            FROM treinamentos t
            LEFT JOIN treinamentos_participantes tp
                   ON tp.cod_empresa = t.cod_empresa
                  AND tp.id_treinamento = t.id_treinamento
            WHERE t.cod_empresa = %s
              AND t.data_treinamento >= %s
              AND t.data_treinamento < %s
            GROUP BY
                t.id_treinamento,
                t.cod_treinamento,
                t.descricao,
                t.data_treinamento,
                t.texto,
                t.texto_certificado,
                t.instituicao_certificado,
                t.carga_horaria,
                t.instrutor,
                t.validade_meses
            ORDER BY t.data_treinamento DESC, t.id_treinamento DESC
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
    
 # -----------------------------------------------------------
# EDITAR TREINAMENTO
# -----------------------------------------------------------
@treinamentos_bp.route(
    "/<int:id_treinamento>/editar",
    methods=["GET", "POST"]
)
@permissao_obrigatoria(
    "TREINAMENTOS",
    "CADASTRAR_TREINAMENTOS",
    redirecionar_para="treinamentos.menu_treinamentos",
)
def editar_treinamento(id_treinamento):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            descricao = (request.form.get("descricao") or "").strip()
            data_treinamento = request.form.get("data_treinamento")
            carga_horaria = (request.form.get("carga_horaria") or "").replace(",", ".").strip()
            instrutor = (request.form.get("instrutor") or "").strip()
            validade_meses = (request.form.get("validade_meses") or "").strip()
            texto = (request.form.get("texto") or "").strip()
            texto_certificado = (request.form.get("texto_certificado") or "").strip()
            instituicao_certificado = (request.form.get("instituicao_certificado") or "").strip()
            id_modelo_certificado = request.form.get("id_modelo_certificado") or None

            if not descricao or not data_treinamento:
                flash("Informe descrição e data do treinamento.", "error")
                return redirect(url_for(
                    "treinamentos.editar_treinamento",
                    id_treinamento=id_treinamento
                ))

            cur.execute("""
                UPDATE treinamentos
                   SET descricao = %s,
                       data_treinamento = %s,
                       carga_horaria = %s,
                       instrutor = %s,
                       validade_meses = %s,
                       texto = %s,
                       texto_certificado = %s,
                       instituicao_certificado = %s,
                       id_modelo_certificado = %s,
                       atualizado_em = NOW()
                 WHERE id_treinamento = %s
                   AND cod_empresa = %s
            """, (
                descricao,
                data_treinamento,
                carga_horaria if carga_horaria else None,
                instrutor,
                validade_meses if validade_meses else None,
                texto,
                texto_certificado,
                instituicao_certificado,
                id_modelo_certificado,
                id_treinamento,
                cod_empresa,
            ))

            conn.commit()
            flash("Treinamento atualizado com sucesso.", "success")

            return redirect(url_for(
                "treinamentos.editar_treinamento",
                id_treinamento=id_treinamento
            ))

        cur.execute("""
            SELECT
                id_treinamento,
                cod_treinamento,
                descricao,
                data_treinamento,
                carga_horaria,
                instrutor,
                validade_meses,
                texto,
                texto_certificado,
                instituicao_certificado,
                id_modelo_certificado
            FROM treinamentos
            WHERE id_treinamento = %s
              AND cod_empresa = %s
        """, (id_treinamento, cod_empresa))

        treinamento = cur.fetchone()

        if not treinamento:
            flash("Treinamento não encontrado.", "error")
            return redirect(url_for("treinamentos.cadastrar_treinamentos"))

        cur.execute("""
            SELECT
                id,
                nome_modelo,
                arquivo_fundo
            FROM modelos_certificados
            WHERE cod_empresa = %s
              AND COALESCE(ativo, true) = true
            ORDER BY nome_modelo
        """, (cod_empresa,))

        modelos_certificados = cur.fetchall() or []

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao editar treinamento: {e}", "error")
        return redirect(url_for("treinamentos.cadastrar_treinamentos"))

    finally:
        cur.close()
        conn.close()

    return render_template(
        "editar_treinamento.html",
        nome_empresa=session.get("nome_empresa"),
        treinamento=treinamento,
        modelos_certificados=modelos_certificados,
        url_voltar=url_for("treinamentos.cadastrar_treinamentos"),
        texto_voltar="← Voltar",
    )

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
            WHERE id_treinamento = %s
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
            WHERE id_treinamento = %s
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
                 WHERE id_treinamento = %s
                   AND cod_empresa = %s
            """, (texto, id_treinamento, cod_empresa))

            conn.commit()
            flash("Descritivo salvo com sucesso.", "success")

            return redirect(url_for("treinamentos.cadastrar_treinamentos"))

        cur.execute("""
            SELECT
                id_treinamento,
                cod_treinamento,
                descricao,
                data_treinamento,
                texto
            FROM treinamentos
            WHERE id_treinamento = %s
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

# -----------------------------------------------------------
# VINCULAR PARTICIPANTES
# -----------------------------------------------------------
@treinamentos_bp.route("/vincular-participantes", methods=["GET", "POST"])
@permissao_obrigatoria(
    "TREINAMENTOS",
    "VINCULAR_PARTICIPANTES",
    redirecionar_para="treinamentos.menu_treinamentos",
)
def vincular_participantes():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa")

    hoje = date.today()
    ano_sel = (request.values.get("ano") or str(hoje.year)).strip()
    id_treinamento = request.values.get("id_treinamento")
    filial_sel = request.values.get("filial", "")
    cargo_sel = request.values.get("cargo", "")

    treinamento = None
    funcionarios = []
    participantes_vinculados = []
    filiais = []
    cargos = []
    anos = [hoje.year]

    data_ini = f"{ano_sel}-01-01"
    data_fim = f"{int(ano_sel) + 1}-01-01"

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            id_treinamento = request.form.get("id_treinamento")
            funcionarios_ids = request.form.getlist("funcionarios")

            if not id_treinamento:
                flash("Selecione um treinamento.", "error")
                return redirect(url_for(
                    "treinamentos.vincular_participantes",
                    ano=ano_sel
                ))

            cur.execute("""
                DELETE FROM treinamentos_participantes
                WHERE id_treinamento = %s
                  AND cod_empresa = %s
            """, (id_treinamento, cod_empresa))

            for id_funcionario in funcionarios_ids:
                cur.execute("""
                    INSERT INTO treinamentos_participantes (
                        cod_empresa,
                        id_treinamento,
                        id_funcionario,
                        criado_em,
                        atualizado_em
                    )
                    VALUES (%s, %s, %s, NOW(), NOW())
                    ON CONFLICT (cod_empresa, id_treinamento, id_funcionario)
                    DO NOTHING
                """, (
                    cod_empresa,
                    id_treinamento,
                    id_funcionario,
                ))

            conn.commit()
            flash("Participantes vinculados com sucesso.", "success")

            return redirect(url_for(
                "treinamentos.vincular_participantes",
                ano=ano_sel,
                id_treinamento=id_treinamento
            ))

        cur.execute("""
            SELECT
                id_treinamento,
                cod_treinamento,
                descricao,
                data_treinamento,
                carga_horaria,
                instrutor
            FROM treinamentos
            WHERE cod_empresa = %s
              AND data_treinamento >= %s
              AND data_treinamento < %s
            ORDER BY data_treinamento DESC, id DESC
        """, (cod_empresa, data_ini, data_fim))

        treinamentos = cur.fetchall() or []
        
        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
            AND COALESCE(ativo, true) = true
            ORDER BY cod_filial
        """, (cod_empresa,))

        filiais = cur.fetchall() or []

        cur.execute("""
            SELECT id, codigo, descricao
            FROM cargos
            WHERE cod_empresa = %s
            AND COALESCE(ativo, true) = true
            ORDER BY id, descricao
        """, (cod_empresa,))

        cargos = cur.fetchall() or []

        cur.execute("""
            SELECT DISTINCT EXTRACT(YEAR FROM data_treinamento)::int AS ano
            FROM treinamentos
            WHERE cod_empresa = %s
            ORDER BY ano DESC
        """, (cod_empresa,))

        anos = [linha["ano"] for linha in cur.fetchall()] or [hoje.year]

        if id_treinamento:
            cur.execute("""
                SELECT
                    id_treinamento,
                    cod_treinamento,
                    descricao,
                    data_treinamento,
                    carga_horaria,
                    instrutor
                FROM treinamentos
                WHERE id_treinamento = %s
                  AND cod_empresa = %s
            """, (id_treinamento, cod_empresa))

            treinamento = cur.fetchone()

            if treinamento:
                
                cur.execute("""
                    SELECT
                        f.id,
                        f.nome,
                        f.cod_filial,
                        f.id_cargo,
                        c.descricao AS cargo,
                        fi.nome_filial
                    FROM funcionarios f
                    LEFT JOIN cargos c
                    ON c.cod_empresa = f.cod_empresa
                    AND c.id = f.id_cargo
                    LEFT JOIN filiais fi
                    ON fi.cod_empresa = f.cod_empresa
                    AND fi.cod_filial = f.cod_filial
                    WHERE f.cod_empresa = %s
                    AND COALESCE(f.ativo, true) = true
                    ORDER BY fi.cod_filial, c.id, f.nome
                """, (cod_empresa,))

                funcionarios = cur.fetchall() or []
                cur.execute("""
                    SELECT id_funcionario
                    FROM treinamentos_participantes
                    WHERE cod_empresa = %s
                    AND id_treinamento = %s
                """, (cod_empresa, id_treinamento))

                participantes_vinculados = [
                    linha["id_funcionario"] for linha in cur.fetchall()
                ]

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao vincular participantes: {e}", "error")
        treinamentos = []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "vincular_participantes_treinamento.html",
        nome_empresa=nome_empresa,
        ano_sel=ano_sel,
        anos=anos,
        treinamentos=treinamentos,
        treinamento=treinamento,
        funcionarios=funcionarios,
        participantes_vinculados=participantes_vinculados,
        filiais=filiais,
        cargos=cargos,
        filial_sel=filial_sel,
        cargo_sel=cargo_sel,
        url_voltar=url_for("treinamentos.menu_treinamentos"),
        texto_voltar="← Voltar",
    )

# -----------------------------------------------------------
# CONSULTAR TREINAMENTOS
# -----------------------------------------------------------
@treinamentos_bp.route("/consultar")
@permissao_obrigatoria(
    "TREINAMENTOS",
    "CONSULTAR_TREINAMENTOS",
    redirecionar_para="treinamentos.menu_treinamentos",
)
def consultar_treinamentos():

    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa")

    hoje = date.today()

    ano_sel = (request.args.get("ano") or str(hoje.year)).strip()
    filial_sel = (request.args.get("filial") or "").strip()

    data_ini = f"{ano_sel}-01-01"
    data_fim = f"{int(ano_sel) + 1}-01-01"

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    treinamentos = []
    filiais = []

    try:
        cur.execute("""
            SELECT
                cod_filial,
                nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND COALESCE(ativo, true) = true
            ORDER BY cod_filial
        """, (cod_empresa,))

        filiais = cur.fetchall() or []

        sql = """
            SELECT
                t.id_treinamento,
                t.id_treinamento AS id,
                t.cod_treinamento,
                t.descricao,
                t.data_treinamento,
                t.carga_horaria,
                t.instrutor,
                t.validade_meses,
                COUNT(tp.id_funcionario) AS qtd_participantes
            FROM treinamentos t
            LEFT JOIN treinamentos_participantes tp
                   ON tp.cod_empresa = t.cod_empresa
                  AND tp.id_treinamento = t.id_treinamento
        """

        params = [cod_empresa, data_ini, data_fim]

        if filial_sel:
            sql += """
                INNER JOIN funcionarios f
                        ON f.cod_empresa = tp.cod_empresa
                       AND f.id = tp.id_funcionario
                       AND f.cod_filial = %s
            """
            params.append(filial_sel)

        sql += """
            WHERE t.cod_empresa = %s
              AND t.data_treinamento >= %s
              AND t.data_treinamento < %s
            GROUP BY
                t.id_treinamento,
                t.cod_treinamento,
                t.descricao,
                t.data_treinamento,
                t.carga_horaria,
                t.instrutor,
                t.validade_meses
            ORDER BY
                t.data_treinamento DESC,
                t.id_treinamento DESC
        """

        cur.execute(sql, params)

        treinamentos = cur.fetchall() or []

    except Exception as e:
        flash(f"Erro ao consultar treinamentos: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return render_template(
        "consultar_treinamentos.html",
        nome_empresa=nome_empresa,
        treinamentos=treinamentos,
        filiais=filiais,
        ano_sel=ano_sel,
        filial_sel=filial_sel,
        url_voltar=url_for("treinamentos.menu_treinamentos"),
        texto_voltar="← Voltar",
    )
# -----------------------------------------------------------
# VISUALIZAR TREINAMENTO
# -----------------------------------------------------------
@treinamentos_bp.route("/<int:id_treinamento>/visualizar")
@permissao_obrigatoria(
    "TREINAMENTOS",
    "CONSULTAR_TREINAMENTOS",
    redirecionar_para="treinamentos.menu_treinamentos",
)
def visualizar_treinamento(id_treinamento):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT
                id_treinamento,
                cod_treinamento,
                descricao,
                data_treinamento,
                texto,
                carga_horaria,
                instrutor,
                instituicao_certificado,
                validade_meses
            FROM treinamentos
            WHERE id_treinamento = %s
              AND cod_empresa = %s
        """, (id_treinamento, cod_empresa))

        treinamento = cur.fetchone()

        if not treinamento:
            flash("Treinamento não encontrado.", "error")
            return redirect(url_for("treinamentos.consultar_treinamentos"))

        cur.execute("""
            SELECT
                f.id,
                f.nome,
                c.descricao AS cargo,
                fi.nome_filial
            FROM treinamentos_participantes tp
            JOIN funcionarios f
              ON f.cod_empresa = tp.cod_empresa
             AND f.id = tp.id_funcionario
            LEFT JOIN cargos c
              ON c.cod_empresa = f.cod_empresa
             AND c.id = f.id_cargo
            LEFT JOIN filiais fi
              ON fi.cod_empresa = f.cod_empresa
             AND fi.cod_filial = f.cod_filial
            WHERE tp.cod_empresa = %s
              AND tp.id_treinamento = %s
            ORDER BY fi.cod_filial, c.id, f.nome
        """, (cod_empresa, id_treinamento))

        participantes = cur.fetchall() or []

    except Exception as e:
        flash(f"Erro ao visualizar treinamento: {e}", "error")
        return redirect(url_for("treinamentos.consultar_treinamentos"))

    finally:
        cur.close()
        conn.close()

    return render_template(
        "visualizar_treinamento.html",
        nome_empresa=session.get("nome_empresa"),
        treinamento=treinamento,
        participantes=participantes,
        url_voltar=url_for("treinamentos.consultar_treinamentos"),
        texto_voltar="← Voltar",
    )
    
# -----------------------------------------------------------
# CONFIGURAÇÕES DE TREINAMENTOS
# -----------------------------------------------------------
@treinamentos_bp.route("/configuracoes")
@permissao_obrigatoria(
    "TREINAMENTOS",
    "CONFIGURACOES_TREINAMENTOS",
    redirecionar_para="treinamentos.menu_treinamentos",
)
def configuracoes_treinamentos():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    session["sistema_ativo"] = "treinamentos"

    return render_template(
        "configuracoes_treinamentos.html",
        nome_empresa=session.get("nome_empresa"),
        empresa_ativa=session.get("cod_empresa"),
        url_voltar=url_for("treinamentos.menu_treinamentos"),
        texto_voltar="← Voltar",
    )
    
# -----------------------------------------------------------
# MODELOS DE CERTIFICADOS
# -----------------------------------------------------------
@treinamentos_bp.route("/modelos-certificados", methods=["GET", "POST"])
@permissao_obrigatoria(
    "TREINAMENTOS",
    "CONFIGURACOES_TREINAMENTOS",
    redirecionar_para="treinamentos.menu_treinamentos",
)
def modelos_certificados():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            nome_modelo = (request.form.get("nome_modelo") or "").strip()
            arquivo = request.files.get("arquivo_fundo")

            if not nome_modelo:
                flash("Informe o nome do modelo.", "error")
                return redirect(url_for("treinamentos.modelos_certificados"))

            nome_arquivo = None

            if arquivo and arquivo.filename:
                nome_seguro = secure_filename(arquivo.filename)
                nome_arquivo = f"{cod_empresa}_{nome_seguro}"

                pasta_upload = os.path.join(
                    current_app.root_path,
                    "static",
                    "uploads",
                    "certificados"
                )

                os.makedirs(pasta_upload, exist_ok=True)

                caminho_arquivo = os.path.join(pasta_upload, nome_arquivo)
                arquivo.save(caminho_arquivo)

            cur.execute("""
                INSERT INTO modelos_certificados (
                    cod_empresa,
                    nome_modelo,
                    arquivo_fundo,
                    ativo,
                    criado_em,
                    atualizado_em
                )
                VALUES (%s, %s, %s, TRUE, NOW(), NOW())
            """, (
                cod_empresa,
                nome_modelo,
                nome_arquivo,
            ))

            conn.commit()
            flash("Modelo de certificado cadastrado com sucesso.", "success")

            return redirect(url_for("treinamentos.modelos_certificados"))

        cur.execute("""
            SELECT
                id,
                nome_modelo,
                arquivo_fundo,
                largura_px,
                altura_px,
                ativo
            FROM modelos_certificados
            WHERE cod_empresa = %s
            ORDER BY ativo DESC, nome_modelo
        """, (cod_empresa,))

        modelos = cur.fetchall() or []

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao acessar modelos de certificados: {e}", "error")
        modelos = []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "modelos_certificados.html",
        nome_empresa=nome_empresa,
        modelos=modelos,
        url_voltar=url_for("treinamentos.configuracoes_treinamentos"),
        texto_voltar="← Voltar",
    )
    
# -----------------------------------------------------------
# ATIVAR MODELO DE CERTIFICADO
# -----------------------------------------------------------
@treinamentos_bp.route("/modelos-certificados/<int:id_modelo>/ativar")
@permissao_obrigatoria(
    "TREINAMENTOS",
    "CONFIGURACOES_TREINAMENTOS",
    redirecionar_para="treinamentos.menu_treinamentos",
)
def ativar_modelo_certificado(id_modelo):

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:

        cur.execute("""
            UPDATE modelos_certificados
               SET ativo = TRUE,
                   atualizado_em = NOW()
             WHERE id_treinamento = %s
               AND cod_empresa = %s
        """, (
            id_modelo,
            cod_empresa,
        ))

        conn.commit()

        flash("Modelo ativado com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao ativar modelo: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("treinamentos.modelos_certificados"))


# -----------------------------------------------------------
# DESATIVAR MODELO DE CERTIFICADO
# -----------------------------------------------------------
@treinamentos_bp.route("/modelos-certificados/<int:id_modelo>/desativar")
@permissao_obrigatoria(
    "TREINAMENTOS",
    "CONFIGURACOES_TREINAMENTOS",
    redirecionar_para="treinamentos.menu_treinamentos",
)
def desativar_modelo_certificado(id_modelo):

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:

        cur.execute("""
            UPDATE modelos_certificados
               SET ativo = FALSE,
                   atualizado_em = NOW()
             WHERE id_treinamento = %s
               AND cod_empresa = %s
        """, (
            id_modelo,
            cod_empresa,
        ))

        conn.commit()

        flash("Modelo desativado com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao desativar modelo: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("treinamentos.modelos_certificados"))

# -----------------------------------------------------------
# CAMPOS DO MODELO DE CERTIFICADO
# -----------------------------------------------------------
@treinamentos_bp.route("/modelos-certificados/<int:id_modelo>/campos", methods=["GET", "POST"])
@permissao_obrigatoria(
    "TREINAMENTOS",
    "CONFIGURACOES_TREINAMENTOS",
    redirecionar_para="treinamentos.menu_treinamentos",
)
def campos_modelo_certificado(id_modelo):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT id, nome_modelo, arquivo_fundo
            FROM modelos_certificados
            WHERE id_treinamento = %s
              AND cod_empresa = %s
        """, (id_modelo, cod_empresa))

        modelo = cur.fetchone()

        if not modelo:
            flash("Modelo não encontrado.", "error")
            return redirect(url_for("treinamentos.modelos_certificados"))

        campos_padrao = [
            "texto_certificado",
            "data_certificado"
        ]

        for campo in campos_padrao:
            cur.execute("""
                INSERT INTO modelos_certificados_campos (
                    cod_empresa,
                    id_modelo,
                    campo,
                    x,
                    y,
                    largura,
                    altura,
                    fonte,
                    tamanho_fonte,
                    cor,
                    alinhamento,
                    texto_padrao,
                    ativo,
                    criado_em,
                    atualizado_em
                )
                VALUES (
                    %s, %s, %s,
                    0, 0,
                    0, 0,
                    'Helvetica',
                    18,
                    '#000000',
                    'center',
                    NULL,
                    TRUE,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (cod_empresa, id_modelo, campo)
                DO NOTHING
            """, (cod_empresa, id_modelo, campo))

        if request.method == "POST":
            for campo in campos_padrao:
                x = (request.form.get(f"{campo}_x") or "0").replace(",", ".")
                y = (request.form.get(f"{campo}_y") or "0").replace(",", ".")
                largura = (request.form.get(f"{campo}_largura") or "0").replace(",", ".")
                altura = (request.form.get(f"{campo}_altura") or "0").replace(",", ".")
                fonte = (request.form.get(f"{campo}_fonte") or "Helvetica").strip()
                tamanho_fonte = (request.form.get(f"{campo}_tamanho_fonte") or "18").replace(",", ".")
                cor = (request.form.get(f"{campo}_cor") or "#000000").strip()
                alinhamento = (request.form.get(f"{campo}_alinhamento") or "center").strip()

                cur.execute("""
                    UPDATE modelos_certificados_campos
                    SET x = %s,
                        y = %s,
                        largura = %s,
                        altura = %s,
                        fonte = %s,
                        tamanho_fonte = %s,
                        cor = %s,
                        alinhamento = %s,
                        atualizado_em = NOW()
                    WHERE cod_empresa = %s
                    AND id_modelo = %s
                    AND campo = %s
                """, (
                    x,
                    y,
                    largura,
                    altura,
                    fonte,
                    tamanho_fonte,
                    cor,
                    alinhamento,
                    cod_empresa,
                    id_modelo,
                    campo,
                ))

            conn.commit()
            flash("Campos do certificado salvos com sucesso.", "success")

            return redirect(url_for(
                "treinamentos.campos_modelo_certificado",
                id_modelo=id_modelo
            ))

        conn.commit()

        cur.execute("""
            SELECT
                campo,
                x,
                y,
                largura,
                altura,
                fonte,
                tamanho_fonte,
                cor,
                alinhamento,
                texto_padrao,
                ativo
            FROM modelos_certificados_campos
            WHERE cod_empresa = %s
            AND id_modelo = %s
            ORDER BY
                CASE campo
                    WHEN 'texto_certificado' THEN 2
                    WHEN 'data_certificado' THEN 3
                    ELSE 99
                END
        """, (
            cod_empresa,
            id_modelo,
        ))

        campos = cur.fetchall() or []

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao configurar campos do certificado: {e}", "error")
        return redirect(url_for("treinamentos.modelos_certificados"))

    finally:
        cur.close()
        conn.close()

    return render_template(
        "modelo_certificado_campos.html",
        nome_empresa=session.get("nome_empresa"),
        modelo=modelo,
        campos=campos,
        url_voltar=url_for("treinamentos.modelos_certificados"),
        texto_voltar="← Voltar",
    )

# -----------------------------------------------------------
# EMITIR CERTIFICADOS
# -----------------------------------------------------------
@treinamentos_bp.route("/emitir-certificados")
@permissao_obrigatoria(
    "TREINAMENTOS",
    "EMITIR_CERTIFICADOS",
    redirecionar_para="treinamentos.menu_treinamentos",
)
def emitir_certificados():

    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa")

    hoje = date.today()

    ano_sel = (request.args.get("ano") or str(hoje.year)).strip()
    filial_sel = (request.args.get("filial") or "").strip()

    data_ini = f"{ano_sel}-01-01"
    data_fim = f"{int(ano_sel) + 1}-01-01"

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    treinamentos = []
    filiais = []

    try:
        cur.execute("""
            SELECT
                cod_filial,
                nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND COALESCE(ativo, true) = true
            ORDER BY cod_filial
        """, (cod_empresa,))

        filiais = cur.fetchall() or []

        sql = """
            SELECT
                t.id_treinamento,
                t.id_treinamento AS id,
                t.cod_treinamento,
                t.descricao,
                t.data_treinamento,
                t.carga_horaria,
                t.instrutor,
                t.validade_meses,
                COUNT(tp.id_funcionario) AS qtd_participantes
            FROM treinamentos t
            LEFT JOIN treinamentos_participantes tp
                   ON tp.cod_empresa = t.cod_empresa
                  AND tp.id_treinamento = t.id_treinamento
        """

        params = [cod_empresa, data_ini, data_fim]

        if filial_sel:
            sql += """
                INNER JOIN funcionarios f
                        ON f.cod_empresa = tp.cod_empresa
                       AND f.id = tp.id_funcionario
                       AND f.cod_filial = %s
            """
            params.append(filial_sel)

        sql += """
            WHERE t.cod_empresa = %s
              AND t.data_treinamento >= %s
              AND t.data_treinamento < %s
            GROUP BY
                t.id_treinamento,
                t.cod_treinamento,
                t.descricao,
                t.data_treinamento,
                t.carga_horaria,
                t.instrutor,
                t.validade_meses
            ORDER BY
                t.data_treinamento DESC,
                t.id_treinamento DESC
        """

        cur.execute(sql, params)
        treinamentos = cur.fetchall() or []

    except Exception as e:
        flash(f"Erro ao consultar treinamentos para emissão: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return render_template(
        "emitir_certificados.html",
        nome_empresa=nome_empresa,
        treinamentos=treinamentos,
        filiais=filiais,
        ano_sel=ano_sel,
        filial_sel=filial_sel,
        url_voltar=url_for("treinamentos.menu_treinamentos"),
        texto_voltar="← Voltar",
    )
    
# -----------------------------------------------------------
# SELECIONAR PARTICIPANTES PARA CERTIFICADO
# -----------------------------------------------------------
@treinamentos_bp.route("/<int:id_treinamento>/certificados/participantes")
@permissao_obrigatoria(
    "TREINAMENTOS",
    "EMITIR_CERTIFICADOS",
    redirecionar_para="treinamentos.menu_treinamentos",
)
def selecionar_participantes_certificado(id_treinamento):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    treinamento = None
    participantes = []

    try:
        cur.execute("""
            SELECT
                id_treinamento,
                cod_treinamento,
                descricao,
                data_treinamento,
                carga_horaria,
                instrutor,
                instituicao_certificado,
                validade_meses,
                texto_certificado
            FROM treinamentos
            WHERE id_treinamento = %s
              AND cod_empresa = %s
        """, (id_treinamento, cod_empresa))

        treinamento = cur.fetchone()

        if not treinamento:
            flash("Treinamento não encontrado.", "error")
            return redirect(url_for("treinamentos.emitir_certificados"))

        cur.execute("""
            SELECT
                f.id AS id_funcionario,
                f.nome,
                f.cod_filial,
                c.descricao AS cargo,
                fi.nome_filial
            FROM treinamentos_participantes tp
            JOIN funcionarios f
              ON f.cod_empresa = tp.cod_empresa
             AND f.id = tp.id_funcionario
            LEFT JOIN cargos c
              ON c.cod_empresa = f.cod_empresa
             AND c.id = f.id_cargo
            LEFT JOIN filiais fi
              ON fi.cod_empresa = f.cod_empresa
             AND fi.cod_filial = f.cod_filial
            WHERE tp.cod_empresa = %s
              AND tp.id_treinamento = %s
            ORDER BY fi.cod_filial, c.id, f.nome
        """, (cod_empresa, id_treinamento))

        participantes = cur.fetchall() or []

    except Exception as e:
        flash(f"Erro ao selecionar participantes: {e}", "error")
        return redirect(url_for("treinamentos.emitir_certificados"))

    finally:
        cur.close()
        conn.close()

    return render_template(
        "selecionar_participantes_certificado.html",
        nome_empresa=nome_empresa,
        treinamento=treinamento,
        participantes=participantes,
        url_voltar=url_for("treinamentos.emitir_certificados"),
        texto_voltar="← Voltar",
    )

# -----------------------------------------------------------
# GERAR CERTIFICADOS EM PDF
# -----------------------------------------------------------
@treinamentos_bp.route("/<int:id_treinamento>/certificados/gerar", methods=["POST"])
@permissao_obrigatoria(
    "TREINAMENTOS",
    "EMITIR_CERTIFICADOS",
    redirecionar_para="treinamentos.menu_treinamentos",
)
def gerar_certificados_pdf(id_treinamento):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    participantes_ids = request.form.getlist("participantes")

    if not participantes_ids:
        flash("Selecione pelo menos um participante para emitir o certificado.", "error")
        return redirect(url_for(
            "treinamentos.selecionar_participantes_certificado",
            id_treinamento=id_treinamento
        ))

    flash(f"Foram selecionados {len(participantes_ids)} participante(s) para emissão.", "success")

    return redirect(url_for(
        "treinamentos.selecionar_participantes_certificado",
        id_treinamento=id_treinamento
    ))