from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from psycopg2.extras import RealDictCursor
from datetime import date

from db import get_connection
from security_helpers import permissao_obrigatoria, usuario_tem_permissao

vistorias_bp = Blueprint("vistorias", __name__)


def pode_editar_vistoria_data(data_vistoria):
    # 🔥 SUPERUSUÁRIO PODE TUDO
    tipo_global = str(session.get("tipo_global") or "").lower()
    if tipo_global == "superusuario":
        return True

    hoje = date.today()

    inicio_mes_atual = hoje.replace(day=1)

    if hoje.month == 1:
        inicio_mes_anterior = date(hoje.year - 1, 12, 1)
    else:
        inicio_mes_anterior = date(hoje.year, hoje.month - 1, 1)

    if hoje.month == 12:
        inicio_proximo_mes = date(hoje.year + 1, 1, 1)
    else:
        inicio_proximo_mes = date(hoje.year, hoje.month + 1, 1)

    return inicio_mes_anterior <= data_vistoria < inicio_proximo_mes

# ---------------------------------------
# MENU VISTORIAS
# ---------------------------------------
@vistorias_bp.route("/menu")
@permissao_obrigatoria("VISTORIAS", "MENU", redirecionar_para="sistema.selecionar_sistema")
def menu_vistorias():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_configurar_checklists = True
        pode_executar_vistorias = True
    else:
        pode_configurar_checklists = usuario_tem_permissao(id_usuario, cod_empresa, "VISTORIAS", "CONFIGURAR_CHECKLISTS")
        pode_executar_vistorias = usuario_tem_permissao(id_usuario, cod_empresa, "VISTORIAS", "EXECUTAR_VISTORIAS")

    return render_template(
        "menu_vistorias.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema"),
        texto_voltar="← Voltar",
        pode_configurar_checklists=pode_configurar_checklists,
        pode_executar_vistorias=pode_executar_vistorias,
    )


# ---------------------------------------
# CONFIGURAR CHECKLISTS - LISTA
# ---------------------------------------
@vistorias_bp.route("/checklists/configurar")
@permissao_obrigatoria("VISTORIAS", "CONFIGURAR_CHECKLISTS", redirecionar_para="vistorias.menu_vistorias")
def configurar_checklists():
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
                id_checklist,
                codigo_checklist,
                descricao,
                versao,
                status,
                criado_em,
                atualizado_em
            FROM vistorias_checklists
            WHERE cod_empresa = %s
            ORDER BY codigo_checklist, versao DESC
        """, (cod_empresa,))

        checklists = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "configurar_checklists.html",
        nome_empresa=session.get("nome_empresa"),
        checklists=checklists,
        url_voltar=url_for("vistorias.menu_vistorias"),
        texto_voltar="← Voltar",
    )


# ---------------------------------------
# NOVO CHECKLIST
# ---------------------------------------
@vistorias_bp.route("/checklists/novo", methods=["GET", "POST"])
@permissao_obrigatoria("VISTORIAS", "CONFIGURAR_CHECKLISTS", redirecionar_para="vistorias.menu_vistorias")
def novo_checklist():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    if request.method == "POST":
        codigo_checklist = (request.form.get("codigo_checklist") or "").strip().upper()
        descricao = (request.form.get("descricao") or "").strip()

        if not codigo_checklist or not descricao:
            flash("Informe o código e a descrição do checklist.", "error")
            return redirect(url_for("vistorias.novo_checklist"))

        conn = get_connection()
        cur = conn.cursor()

        try:
            cur.execute("""
                INSERT INTO vistorias_checklists (
                    cod_empresa,
                    codigo_checklist,
                    descricao,
                    versao,
                    status,
                    criado_em,
                    atualizado_em
                )
                VALUES (%s, %s, %s, 1, 'ATIVO', NOW(), NOW())
                RETURNING id_checklist
            """, (cod_empresa, codigo_checklist, descricao))

            id_checklist = cur.fetchone()[0]
            conn.commit()

            return redirect(url_for("vistorias.editar_checklist", id_checklist=id_checklist))

        except Exception as e:
            conn.rollback()
            flash(f"Erro ao criar checklist: {e}", "error")

        finally:
            cur.close()
            conn.close()

    return render_template(
        "novo_checklist.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("vistorias.configurar_checklists"),
        texto_voltar="← Voltar",
    )


# ---------------------------------------
# EDITAR CHECKLIST
# ---------------------------------------
@vistorias_bp.route("/checklists/<int:id_checklist>/editar")
@permissao_obrigatoria("VISTORIAS", "CONFIGURAR_CHECKLISTS", redirecionar_para="vistorias.menu_vistorias")
def editar_checklist(id_checklist):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT *
            FROM vistorias_checklists
            WHERE id_checklist = %s
              AND cod_empresa = %s
        """, (id_checklist, cod_empresa))
        checklist = cur.fetchone()

        if not checklist:
            flash("Checklist não encontrado.", "error")
            return redirect(url_for("vistorias.configurar_checklists"))

        cur.execute("""
            SELECT *
            FROM vistorias_checklist_itens
            WHERE id_checklist = %s
              AND ativo = TRUE
            ORDER BY sequencia
        """, (id_checklist,))
        itens = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "editar_checklist.html",
        nome_empresa=session.get("nome_empresa"),
        checklist=checklist,
        itens=itens,
        url_voltar=url_for("vistorias.configurar_checklists"),
        texto_voltar="← Voltar",
    )


# ---------------------------------------
# ADICIONAR ITEM
# ---------------------------------------
@vistorias_bp.route("/checklists/<int:id_checklist>/itens/adicionar", methods=["POST"])
@permissao_obrigatoria("VISTORIAS", "CONFIGURAR_CHECKLISTS", redirecionar_para="vistorias.menu_vistorias")
def adicionar_item_checklist(id_checklist):
    tipo_linha = (request.form.get("tipo_linha") or "ITEM").strip().upper()
    codigo_item = (request.form.get("codigo_item") or "").strip()
    descricao = (request.form.get("descricao") or "").strip()
    pontos_txt = (request.form.get("pontos_possiveis") or "0").replace(",", ".")

    if tipo_linha not in ["GRUPO", "ITEM"]:
        tipo_linha = "ITEM"

    try:
        pontos = float(pontos_txt)
    except ValueError:
        pontos = 0

    if not descricao:
        flash("Informe a descrição.", "error")
        return redirect(url_for("vistorias.editar_checklist", id_checklist=id_checklist))

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT COALESCE(MAX(sequencia), 0) + 1
            FROM vistorias_checklist_itens
            WHERE id_checklist = %s
        """, (id_checklist,))
        sequencia = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO vistorias_checklist_itens (
                id_checklist,
                sequencia,
                tipo_linha,
                codigo_item,
                descricao,
                pontos_possiveis,
                ativo,
                criado_em,
                atualizado_em
            )
            VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW(), NOW())
        """, (id_checklist, sequencia, tipo_linha, codigo_item, descricao, pontos))

        conn.commit()

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao adicionar item: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("vistorias.editar_checklist", id_checklist=id_checklist))


# ---------------------------------------
# SALVAR ITENS
# ---------------------------------------
@vistorias_bp.route("/checklists/<int:id_checklist>/itens/salvar", methods=["POST"])
@permissao_obrigatoria("VISTORIAS", "CONFIGURAR_CHECKLISTS", redirecionar_para="vistorias.menu_vistorias")
def salvar_itens_checklist(id_checklist):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT id_item
            FROM vistorias_checklist_itens
            WHERE id_checklist = %s
              AND ativo = TRUE
        """, (id_checklist,))
        itens = cur.fetchall() or []

        for item in itens:
            id_item = item["id_item"]

            sequencia = request.form.get(f"sequencia_{id_item}") or 0
            tipo_linha = request.form.get(f"tipo_linha_{id_item}") or "ITEM"
            descricao = request.form.get(f"descricao_{id_item}") or ""

            pontos = (request.form.get(f"pontos_possiveis_{id_item}") or "0").replace(",", ".")

            # 🔥 REGRA NOVA
            if tipo_linha == "GRUPO":
                codigo_item = request.form.get(f"codigo_item_{id_item}") or ""
            else:
                codigo_item = ""


            cur.execute("""
                UPDATE vistorias_checklist_itens
                SET
                    sequencia = %s,
                    tipo_linha = %s,
                    codigo_item = %s,
                    descricao = %s,
                    pontos_possiveis = %s,
                    atualizado_em = NOW()
                WHERE id_item = %s
                  AND id_checklist = %s
            """, (
                sequencia,
                tipo_linha,
                codigo_item,
                descricao,
                pontos,
                id_item,
                id_checklist,
            ))

        conn.commit()
        flash("Checklist salvo com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao salvar itens: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("vistorias.editar_checklist", id_checklist=id_checklist))


# ---------------------------------------
# EXECUTAR VISTORIAS - INÍCIO
# ---------------------------------------
@vistorias_bp.route("/executar", methods=["GET", "POST"])
@permissao_obrigatoria("VISTORIAS", "EXECUTAR_VISTORIAS", redirecionar_para="vistorias.menu_vistorias")
def executar_vistorias():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa")

    hoje = date.today()
    ano_sel = (request.args.get("ano") or str(hoje.year)).strip()
    mes_sel = (request.args.get("mes") or str(hoje.month)).strip().zfill(2)

    data_ini = f"{ano_sel}-{mes_sel}-01"

    if mes_sel == "12":
        data_fim = f"{int(ano_sel) + 1}-01-01"
    else:
        data_fim = f"{ano_sel}-{str(int(mes_sel) + 1).zfill(2)}-01"

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = TRUE
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall() or []

        cur.execute("""
            SELECT id_checklist, codigo_checklist, descricao, versao
            FROM vistorias_checklists
            WHERE cod_empresa = %s
              AND status = 'ATIVO'
            ORDER BY codigo_checklist, versao DESC
        """, (cod_empresa,))
        checklists = cur.fetchall() or []

        if request.method == "POST":
            cod_filial = int(request.form.get("cod_filial") or 0)
            id_checklist = int(request.form.get("id_checklist") or 0)
            data_vistoria = request.form.get("data_vistoria")

            if not cod_filial or not id_checklist or not data_vistoria:
                flash("Informe filial, checklist e data.", "error")
                return redirect(url_for("vistorias.executar_vistorias"))

            nome_executor = (
                session.get("nome_usuario")
                or session.get("usuario")
                or f"Usuário {session.get('id_usuario')}"
            )

            cur.execute("""
                INSERT INTO vistorias_execucoes (
                    cod_empresa,
                    id_checklist,
                    cod_filial,
                    data_vistoria,
                    status,
                    id_usuario_executor,
                    nome_executor,
                    criado_em,
                    atualizado_em
                )
                VALUES (%s, %s, %s, %s, 'ABERTA', %s, %s, NOW(), NOW())
                RETURNING id_execucao
            """, (
                cod_empresa,
                id_checklist,
                cod_filial,
                data_vistoria,
                session.get("id_usuario"),
                nome_executor
            ))

            id_execucao = cur.fetchone()["id_execucao"]

            cur.execute("""
                INSERT INTO vistorias_execucao_itens (
                    id_execucao,
                    id_item,
                    sequencia,
                    tipo_linha,
                    codigo_item,
                    descricao,
                    pontos_possiveis,
                    pontuacao,
                    criado_em,
                    atualizado_em
                )
                SELECT
                    %s,
                    id_item,
                    sequencia,
                    tipo_linha,
                    codigo_item,
                    descricao,
                    pontos_possiveis,
                    0,
                    NOW(),
                    NOW()
                FROM vistorias_checklist_itens
                WHERE id_checklist = %s
                  AND ativo = TRUE
                ORDER BY sequencia
            """, (id_execucao, id_checklist))

            conn.commit()

            return redirect(url_for("vistorias.preencher_vistoria", id_execucao=id_execucao))

        cur.execute("""
            SELECT
                e.id_execucao,
                e.data_vistoria,
                e.status,
                COALESCE(e.nota, 0) AS nota,
                e.nome_executor,
                f.cod_filial,
                f.nome_filial,
                c.codigo_checklist,
                c.descricao AS checklist_descricao,
                c.versao
            FROM vistorias_execucoes e
            LEFT JOIN filiais f
              ON f.cod_empresa = e.cod_empresa
             AND f.cod_filial = e.cod_filial
            LEFT JOIN vistorias_checklists c
              ON c.id_checklist = e.id_checklist
            WHERE e.cod_empresa = %s
              AND e.data_vistoria >= %s
              AND e.data_vistoria < %s
            ORDER BY e.data_vistoria DESC, e.id_execucao DESC
        """, (cod_empresa, data_ini, data_fim))

        vistorias_mes = cur.fetchall() or []

        for v in vistorias_mes:
            v["pode_editar"] = (
                pode_editar_vistoria_data(v["data_vistoria"])
                and v["status"] != "FINALIZADA"
            )
            v["pode_alterar_status"] = pode_editar_vistoria_data(v["data_vistoria"])
            v["pode_excluir"] = float(v["nota"] or 0) == 0

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao iniciar vistoria: {e}", "error")
        filiais = []
        checklists = []
        vistorias_mes = []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "executar_vistorias.html",
        nome_empresa=nome_empresa,
        filiais=filiais,
        checklists=checklists,
        vistorias_mes=vistorias_mes,
        ano_sel=ano_sel,
        mes_sel=mes_sel,
        url_voltar=url_for("vistorias.menu_vistorias"),
        texto_voltar="← Voltar",
    )

# ---------------------------------------
# EXCLUIR VISTORIA EM EXECUÇÃO
# ---------------------------------------
@vistorias_bp.route("/execucao/<int:id_execucao>/excluir", methods=["POST"])
@permissao_obrigatoria(
    "VISTORIAS",
    "EXECUTAR_VISTORIAS",
    redirecionar_para="vistorias.menu_vistorias",
)
def excluir_vistoria(id_execucao):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT
                e.id_execucao,
                COALESCE(e.nota, 0) AS nota,
                COUNT(*) FILTER (
                    WHERE i.tipo_linha = 'ITEM'
                      AND COALESCE(i.atendido, '') = 'SIM'
                ) AS qtde_marcados
            FROM vistorias_execucoes e
            LEFT JOIN vistorias_execucao_itens i
              ON i.id_execucao = e.id_execucao
            WHERE e.id_execucao = %s
              AND e.cod_empresa = %s
            GROUP BY e.id_execucao, e.nota
        """, (id_execucao, cod_empresa))

        row = cur.fetchone()

        if not row:
            flash("Vistoria não encontrada.", "error")
            return redirect(url_for("vistorias.executar_vistorias"))

        if tipo_global != "superusuario":
            if int(row["qtde_marcados"] or 0) > 0 or float(row["nota"] or 0) != 0:
                flash("Só é possível excluir vistoria sem itens atendidos e com nota zero.", "error")
                return redirect(url_for("vistorias.executar_vistorias"))

        cur.execute("""
            DELETE FROM vistorias_execucoes
            WHERE id_execucao = %s
              AND cod_empresa = %s
        """, (id_execucao, cod_empresa))

        conn.commit()
        flash("Vistoria excluída com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao excluir vistoria: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("vistorias.executar_vistorias"))

# ---------------------------------------
# PREENCHER VISTORIA
# ---------------------------------------
@vistorias_bp.route("/execucao/<int:id_execucao>", methods=["GET", "POST"])
@permissao_obrigatoria("VISTORIAS", "EXECUTAR_VISTORIAS", redirecionar_para="vistorias.menu_vistorias")
def preencher_vistoria(id_execucao):
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            cur.execute("""
                SELECT id_execucao_item, tipo_linha, pontos_possiveis
                FROM vistorias_execucao_itens
                WHERE id_execucao = %s
            """, (id_execucao,))
            itens = cur.fetchall() or []

            total_possivel = 0
            total_obtido = 0
            updates = []

            for item in itens:
                id_item = item["id_execucao_item"]
                tipo = item["tipo_linha"]
                pontos = float(item["pontos_possiveis"] or 0)

                atendido = "SIM" if request.form.get(f"atendido_{id_item}") == "SIM" else "NAO"
                observacao = request.form.get(f"observacao_{id_item}") or ""

                pontuacao = 0

                if tipo == "ITEM":
                    pontuacao = pontos if atendido == "SIM" else 0
                    total_possivel += pontos
                    total_obtido += pontuacao

                updates.append((atendido, observacao, pontuacao, id_item))

            if updates:
                execute_batch(cur, """
                    UPDATE vistorias_execucao_itens
                    SET atendido = %s,
                        observacao = %s,
                        pontuacao = %s,
                        atualizado_em = NOW()
                    WHERE id_execucao_item = %s
                """, updates, page_size=100)

            nota = 0
            if total_possivel > 0:
                nota = (total_obtido / total_possivel) * 10

            cur.execute("""
                UPDATE vistorias_execucoes
                SET pontuacao_possivel = %s,
                    pontuacao_obtida = %s,
                    nota = %s,
                    atualizado_em = NOW()
                WHERE id_execucao = %s
                  AND cod_empresa = %s
            """, (total_possivel, total_obtido, nota, id_execucao, cod_empresa))

            conn.commit()
            flash("Vistoria salva com sucesso.", "success")

            return redirect(url_for("vistorias.preencher_vistoria", id_execucao=id_execucao))

        cur.execute("""
            SELECT
                e.*,
                f.nome_filial,
                c.codigo_checklist,
                c.descricao AS checklist_descricao,
                c.versao
            FROM vistorias_execucoes e
            LEFT JOIN filiais f
              ON f.cod_empresa = e.cod_empresa
             AND f.cod_filial = e.cod_filial
            LEFT JOIN vistorias_checklists c
              ON c.id_checklist = e.id_checklist
            WHERE e.id_execucao = %s
              AND e.cod_empresa = %s
        """, (id_execucao, cod_empresa))
        execucao = cur.fetchone()

        cur.execute("""
            SELECT *
            FROM vistorias_execucao_itens
            WHERE id_execucao = %s
            ORDER BY sequencia
        """, (id_execucao,))
        itens = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "preencher_vistoria.html",
        execucao=execucao,
        itens=itens,
        url_voltar=url_for("vistorias.executar_vistorias"),
        texto_voltar="← Voltar",
    )

# ---------------------------------------
# EXCLUIR ITEM DO CHECKLIST
# ---------------------------------------
@vistorias_bp.route("/checklists/<int:id_checklist>/itens/<int:id_item>/excluir", methods=["POST"])
@permissao_obrigatoria(
    "VISTORIAS",
    "CONFIGURAR_CHECKLISTS",
    redirecionar_para="vistorias.menu_vistorias",
)
def excluir_item_checklist(id_checklist, id_item):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE vistorias_checklist_itens
            SET ativo = FALSE,
                atualizado_em = NOW()
            WHERE id_checklist = %s
              AND id_item = %s
        """, (id_checklist, id_item))

        conn.commit()
        flash("Linha excluída com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao excluir linha: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("vistorias.editar_checklist", id_checklist=id_checklist))

@vistorias_bp.route("/execucao/<int:id_execucao>/alterar-status", methods=["POST"])
@permissao_obrigatoria(
    "VISTORIAS",
    "EXECUTAR_VISTORIAS",
    redirecionar_para="vistorias.menu_vistorias",
)
def alterar_status_vistoria(id_execucao):
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT data_vistoria, status
            FROM vistorias_execucoes
            WHERE id_execucao = %s
              AND cod_empresa = %s
        """, (id_execucao, cod_empresa))

        row = cur.fetchone()

        if not row:
            flash("Vistoria não encontrada.", "error")
            return redirect(url_for("vistorias.executar_vistorias"))

        if tipo_global != "superusuario":
            if not pode_editar_vistoria_data(row["data_vistoria"]):
                flash("Não é permitido alterar o status desta vistoria.", "error")
                return redirect(url_for("vistorias.executar_vistorias"))

        novo_status = "FINALIZADA" if request.form.get("finalizada") == "on" else "ABERTA"

        cur.execute("""
            UPDATE vistorias_execucoes
            SET status = %s,
                atualizado_em = NOW()
            WHERE id_execucao = %s
              AND cod_empresa = %s
        """, (novo_status, id_execucao, cod_empresa))

        conn.commit()
        flash("Status da vistoria atualizado.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao alterar status: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("vistorias.executar_vistorias"))

