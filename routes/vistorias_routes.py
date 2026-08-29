from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify
from psycopg2.extras import RealDictCursor, execute_batch
from datetime import date

from db import get_connection
from security_helpers import permissao_obrigatoria, usuario_tem_permissao

# Combos de período das telas de vistoria (ano e mês são escolha, não digitação).
NOMES_MESES = [
    (1, "Janeiro"), (2, "Fevereiro"), (3, "Março"), (4, "Abril"),
    (5, "Maio"), (6, "Junho"), (7, "Julho"), (8, "Agosto"),
    (9, "Setembro"), (10, "Outubro"), (11, "Novembro"), (12, "Dezembro"),
]


def anos_com_vistorias(cur, cod_empresa, ano_sel):
    """Anos que o combo oferece: os que têm vistoria, mais o ano corrente
    e o que estiver selecionado — senão o filtro do usuário sumiria da lista."""
    cur.execute("""
        SELECT DISTINCT EXTRACT(YEAR FROM data_vistoria)::int AS ano
        FROM vistorias_execucoes
        WHERE cod_empresa = %s
    """, (cod_empresa,))

    anos = {int(r["ano"]) for r in cur.fetchall() or []}
    anos.add(date.today().year)

    try:
        anos.add(int(ano_sel))
    except (TypeError, ValueError):
        pass

    return sorted(anos, reverse=True)

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
        pode_programar_vistorias = True
        pode_executar_vistorias = True
        pode_consultar_vistorias = True
    else:
        pode_configurar_checklists = usuario_tem_permissao(id_usuario, cod_empresa, "VISTORIAS", "CONFIGURAR_CHECKLISTS")
        pode_programar_vistorias = usuario_tem_permissao(id_usuario, cod_empresa, "VISTORIAS", "PROGRAMAR_VISTORIAS")
        pode_executar_vistorias = usuario_tem_permissao(id_usuario, cod_empresa, "VISTORIAS", "EXECUTAR_VISTORIAS")
        pode_consultar_vistorias = usuario_tem_permissao(id_usuario, cod_empresa, "VISTORIAS", "CONSULTAR_VISTORIAS")

    return render_template(
        "menu_vistorias.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema"),
        texto_voltar="← Voltar",
        pode_configurar_checklists=pode_configurar_checklists,
        pode_programar_vistorias=pode_programar_vistorias,
        pode_executar_vistorias=pode_executar_vistorias,
        pode_consultar_vistorias=pode_consultar_vistorias,        
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
# SALVAR ITEM (AUTOMÁTICO, VIA AJAX)
# ---------------------------------------
@vistorias_bp.route("/checklists/<int:id_checklist>/itens/<int:id_item>/salvar-ajax", methods=["POST"])
def salvar_item_checklist_ajax(id_checklist, id_item):
    """Grava uma linha do checklist sozinha, sem recarregar a tela.

    A tela de edição salva a cada alteração de campo; o botão "Salvar
    Alterações" continua existindo e grava tudo de uma vez pelo caminho
    de sempre. As regras (código só em GRUPO, pontos com vírgula) são as
    mesmas de `salvar_itens_checklist` — se divergirem, o mesmo campo
    passa a valer coisas diferentes conforme quem gravou.
    """
    if "id_usuario" not in session:
        return jsonify({"ok": False, "erro": "Sessão expirada"}), 401

    if "cod_empresa" not in session:
        return jsonify({"ok": False, "erro": "Empresa não selecionada"}), 401

    cod_empresa = str(session["cod_empresa"]).strip()

    if str(session.get("tipo_global") or "").strip().lower() != "superusuario":
        if not usuario_tem_permissao(session["id_usuario"], cod_empresa, "VISTORIAS", "CONFIGURAR_CHECKLISTS"):
            return jsonify({"ok": False, "erro": "Sem permissão para configurar checklists"}), 403

    dados = request.get_json(silent=True) or {}

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # O checklist tem que ser da empresa da sessão, e o item, dele.
        cur.execute("""
            SELECT i.id_item, i.tipo_linha
            FROM vistorias_checklist_itens i
            JOIN vistorias_checklists c
              ON c.id_checklist = i.id_checklist
            WHERE i.id_item = %s
              AND i.id_checklist = %s
              AND i.ativo = TRUE
              AND c.cod_empresa = %s
        """, (id_item, id_checklist, cod_empresa))
        item = cur.fetchone()

        if not item:
            return jsonify({"ok": False, "erro": "Item não encontrado"}), 404

        tipo_linha = (dados.get("tipo_linha") or item["tipo_linha"] or "ITEM").strip().upper()
        if tipo_linha not in ("ITEM", "GRUPO"):
            tipo_linha = "ITEM"

        sequencia = dados.get("sequencia")
        try:
            sequencia = int(sequencia)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "erro": "Sequência inválida"}), 400

        descricao = (dados.get("descricao") or "").strip()

        pontos = str(dados.get("pontos_possiveis") or "0").replace(",", ".").strip() or "0"
        try:
            pontos = float(pontos)
        except ValueError:
            return jsonify({"ok": False, "erro": "Pontos inválidos"}), 400

        # Mesma regra do salvamento em lote: código só existe em GRUPO.
        codigo_item = (dados.get("codigo_item") or "").strip() if tipo_linha == "GRUPO" else ""

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
        """, (sequencia, tipo_linha, codigo_item, descricao, pontos, id_item, id_checklist))

        conn.commit()

        return jsonify({
            "ok": True,
            "id_item": id_item,
            "codigo_item": codigo_item,
            "pontos_possiveis": pontos,
        })

    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 500

    finally:
        cur.close()
        conn.close()


# ---------------------------------------
# EXECUTAR VISTORIAS - INÍCIO
# ---------------------------------------
def cod_filiais_vistorias_usuario(cur, cod_empresa):
    """Filiais que o usuário enxerga em Executar Vistorias.

    Superusuário vê todas; os demais só as filiais em `usuarios_filiais`.
    Programar Vistorias não usa esta trava — quem programa enxerga a empresa
    inteira.
    """
    if str(session.get("tipo_global") or "").strip().lower() == "superusuario":
        cur.execute("""
            SELECT cod_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = TRUE
        """, (cod_empresa,))
        return {int(r["cod_filial"]) for r in cur.fetchall() or []}

    cur.execute("""
        SELECT cod_filial
        FROM usuarios_filiais
        WHERE id_usuario = %s
          AND cod_empresa = %s
          AND ativo = TRUE
    """, (session.get("id_usuario"), cod_empresa))
    return {int(r["cod_filial"]) for r in cur.fetchall() or []}


# ---------------------------------------
# PROGRAMAR VISTORIAS - criar e acompanhar
# ---------------------------------------
@vistorias_bp.route("/programar", methods=["GET", "POST"])
@permissao_obrigatoria("VISTORIAS", "PROGRAMAR_VISTORIAS", redirecionar_para="vistorias.menu_vistorias")
def programar_vistorias():
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
            # A filial é escolha múltipla (checkboxes no estilo do filtro do
            # Excel): a mesma vistoria é programada para cada filial marcada.
            escolhidas = request.form.getlist("cod_filial")
            id_checklist = int(request.form.get("id_checklist") or 0)
            data_vistoria = request.form.get("data_vistoria")

            cod_filiais = [int(f) for f in escolhidas if str(f).strip()]

            if not cod_filiais or not id_checklist or not data_vistoria:
                flash("Informe filial, checklist e data.", "error")
                return redirect(url_for("vistorias.programar_vistorias", ano=ano_sel, mes=mes_sel))

            criadas = 0
            repetidas = 0

            for cod_filial in cod_filiais:
                # Não duplica: mesma filial, mesmo checklist, mesma data.
                cur.execute("""
                    SELECT 1
                    FROM vistorias_execucoes
                    WHERE cod_empresa = %s
                      AND cod_filial = %s
                      AND id_checklist = %s
                      AND data_vistoria = %s
                    LIMIT 1
                """, (cod_empresa, cod_filial, id_checklist, data_vistoria))

                if cur.fetchone():
                    repetidas += 1
                    continue

                # Quem programa não é quem executa: o executor só é conhecido
                # quando alguém da filial abre a vistoria.
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
                    VALUES (%s, %s, %s, %s, 'ABERTA', NULL, NULL, NOW(), NOW())
                    RETURNING id_execucao
                """, (cod_empresa, id_checklist, cod_filial, data_vistoria))

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

                criadas += 1

            conn.commit()

            if criadas:
                flash(
                    f"{criadas} vistoria(s) programada(s)."
                    + (f" {repetidas} já existia(m) e foram ignoradas." if repetidas else ""),
                    "success",
                )
            else:
                flash("Nenhuma vistoria criada: já existiam para essa data e checklist.", "error")

            return redirect(url_for(
                "vistorias.programar_vistorias",
                ano=data_vistoria[:4],
                mes=data_vistoria[5:7],
            ))

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
            v["pode_excluir"] = float(v["nota"] or 0) == 0

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao programar vistoria: {e}", "error")
        filiais = []
        checklists = []
        vistorias_mes = []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "programar_vistorias.html",
        nome_empresa=nome_empresa,
        filiais=filiais,
        checklists=checklists,
        vistorias_mes=vistorias_mes,
        ano_sel=ano_sel,
        mes_sel=mes_sel,
        hoje=hoje.isoformat(),
        url_voltar=url_for("vistorias.menu_vistorias"),
        texto_voltar="← Voltar",
    )


# ---------------------------------------
# EXECUTAR VISTORIAS - só as filiais do usuário
# ---------------------------------------
@vistorias_bp.route("/executar")
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

    # A tela mostra o mês inteiro; estes dois filtros só recortam a lista.
    status_sel = (request.args.get("status") or "TODAS").strip().upper()

    if status_sel not in ("TODAS", "ABERTA", "FINALIZADA"):
        status_sel = "TODAS"

    filial_sel = (request.args.get("cod_filial") or "").strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cod_filiais = cod_filiais_vistorias_usuario(cur, cod_empresa)

        # O combo de filial só oferece as filiais que o usuário enxerga.
        if cod_filiais:
            cur.execute("""
                SELECT cod_filial, nome_filial
                FROM filiais
                WHERE cod_empresa = %s
                  AND cod_filial = ANY(%s)
                ORDER BY cod_filial
            """, (cod_empresa, list(cod_filiais)))
            filiais = cur.fetchall() or []
        else:
            filiais = []

        if filial_sel and int(filial_sel) in cod_filiais:
            cod_filiais_consulta = [int(filial_sel)]
        else:
            filial_sel = ""
            cod_filiais_consulta = list(cod_filiais)

        if not cod_filiais:
            vistorias_mes = []
        else:
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
                  AND e.cod_filial = ANY(%s)
                  AND (%s = 'TODAS' OR e.status = %s)
                ORDER BY e.data_vistoria DESC, e.id_execucao DESC
            """, (cod_empresa, data_ini, data_fim,
                  cod_filiais_consulta, status_sel, status_sel))

            vistorias_mes = cur.fetchall() or []

        for v in vistorias_mes:
            v["pode_editar"] = (
                pode_editar_vistoria_data(v["data_vistoria"])
                and v["status"] != "FINALIZADA"
            )
            v["pode_alterar_status"] = pode_editar_vistoria_data(v["data_vistoria"])

    finally:
        cur.close()
        conn.close()

    return render_template(
        "executar_vistorias.html",
        nome_empresa=nome_empresa,
        vistorias_mes=vistorias_mes,
        ano_sel=ano_sel,
        mes_sel=mes_sel,
        status_sel=status_sel,
        filial_sel=filial_sel,
        filiais=filiais,
        sem_filial=not cod_filiais,
        url_voltar=url_for("vistorias.menu_vistorias"),
        texto_voltar="← Voltar",
    )

# ---------------------------------------
# EXCLUIR VISTORIA EM EXECUÇÃO
# ---------------------------------------
@vistorias_bp.route("/execucao/<int:id_execucao>/excluir", methods=["POST"])
@permissao_obrigatoria(
    "VISTORIAS",
    "PROGRAMAR_VISTORIAS",
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
            return redirect(url_for("vistorias.programar_vistorias"))

        if tipo_global != "superusuario":
            if int(row["qtde_marcados"] or 0) > 0 or float(row["nota"] or 0) != 0:
                flash("Só é possível excluir vistoria sem itens atendidos e com nota zero.", "error")
                return redirect(url_for("vistorias.programar_vistorias"))

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

    return redirect(url_for("vistorias.programar_vistorias"))

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
        # O executor só abre vistoria de filial dele.
        cur.execute("""
            SELECT cod_filial
            FROM vistorias_execucoes
            WHERE id_execucao = %s
              AND cod_empresa = %s
        """, (id_execucao, cod_empresa))
        dono = cur.fetchone()

        if not dono or int(dono["cod_filial"]) not in cod_filiais_vistorias_usuario(cur, cod_empresa):
            flash("Esta vistoria não é de uma filial sua.", "error")
            return redirect(url_for("vistorias.executar_vistorias"))

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

            # Executor é quem preenche — fica gravado no primeiro save.
            nome_executor = (
                session.get("nome_usuario")
                or session.get("usuario")
                or f"Usuário {session.get('id_usuario')}"
            )

            cur.execute("""
                UPDATE vistorias_execucoes
                SET pontuacao_possivel = %s,
                    pontuacao_obtida = %s,
                    nota = %s,
                    id_usuario_executor = COALESCE(id_usuario_executor, %s),
                    nome_executor = COALESCE(nome_executor, %s),
                    atualizado_em = NOW()
                WHERE id_execucao = %s
                  AND cod_empresa = %s
            """, (total_possivel, total_obtido, nota,
                  session.get("id_usuario"), nome_executor,
                  id_execucao, cod_empresa))

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
            SELECT data_vistoria, status, cod_filial
            FROM vistorias_execucoes
            WHERE id_execucao = %s
              AND cod_empresa = %s
        """, (id_execucao, cod_empresa))

        row = cur.fetchone()

        if not row:
            flash("Vistoria não encontrada.", "error")
            return redirect(url_for("vistorias.executar_vistorias"))

        if int(row["cod_filial"]) not in cod_filiais_vistorias_usuario(cur, cod_empresa):
            flash("Esta vistoria não é de uma filial sua.", "error")
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

# ---------------------------------------
# CONSULTAR VISTORIAS
# ---------------------------------------
@vistorias_bp.route("/consultar")
@permissao_obrigatoria("VISTORIAS", "CONSULTAR_VISTORIAS", redirecionar_para="vistorias.menu_vistorias")
def consultar_vistorias():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa")

    hoje = date.today()

    ano_sel = (request.args.get("ano") or str(hoje.year)).strip()
    mes_sel = (request.args.get("mes") or "").strip()

    if mes_sel:
        mes_sel = mes_sel.zfill(2)
        data_ini = f"{ano_sel}-{mes_sel}-01"

        if mes_sel == "12":
            data_fim = f"{int(ano_sel)+1}-01-01"
        else:
            data_fim = f"{ano_sel}-{str(int(mes_sel)+1).zfill(2)}-01"
    else:
        data_ini = f"{ano_sel}-01-01"
        data_fim = f"{int(ano_sel)+1}-01-01"

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Consulta enxerga só as filiais do usuário, igual a Executar.
        cod_filiais = cod_filiais_vistorias_usuario(cur, cod_empresa)
        anos = anos_com_vistorias(cur, cod_empresa, ano_sel)

        if not cod_filiais:
            vistorias = []
        else:
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
                    c.descricao AS checklist_descricao
                FROM vistorias_execucoes e
                LEFT JOIN filiais f
                  ON f.cod_empresa = e.cod_empresa
                 AND f.cod_filial = e.cod_filial
                LEFT JOIN vistorias_checklists c
                  ON c.id_checklist = e.id_checklist
                WHERE e.cod_empresa = %s
                  AND e.data_vistoria >= %s
                  AND e.data_vistoria < %s
                  AND e.cod_filial = ANY(%s)
                ORDER BY e.data_vistoria DESC, e.id_execucao DESC
            """, (cod_empresa, data_ini, data_fim, list(cod_filiais)))

            vistorias = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "consultar_vistorias.html",
        nome_empresa=nome_empresa,
        vistorias=vistorias,
        sem_filial=not cod_filiais,
        ano_sel=ano_sel,
        mes_sel=mes_sel,
        anos=anos,
        nomes_meses=NOMES_MESES,
        url_voltar=url_for("vistorias.menu_vistorias"),
        texto_voltar="← Voltar",
    )

# ---------------------------------------
# VISUALIZAR VISTORIA - SOMENTE LEITURA
# ---------------------------------------
@vistorias_bp.route("/execucao/<int:id_execucao>/visualizar")
@permissao_obrigatoria(
    "VISTORIAS",
    "CONSULTAR_VISTORIAS",
    redirecionar_para="vistorias.menu_vistorias",
)
def visualizar_vistoria(id_execucao):
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

        if not execucao:
            flash("Vistoria não encontrada.", "error")
            return redirect(url_for("vistorias.consultar_vistorias"))

        if int(execucao["cod_filial"]) not in cod_filiais_vistorias_usuario(cur, cod_empresa):
            flash("Esta vistoria não é de uma filial sua.", "error")
            return redirect(url_for("vistorias.consultar_vistorias"))

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
        "visualizar_vistoria.html",
        execucao=execucao,
        itens=itens,
        url_voltar=url_for("vistorias.consultar_vistorias"),
        texto_voltar="← Voltar",
    )

@vistorias_bp.route("/execucao/item/salvar-ajax", methods=["POST"])
def salvar_item_vistoria_ajax():
    if "id_usuario" not in session:
        return jsonify({"ok": False, "erro": "Sessão expirada"}), 401

    if "cod_empresa" not in session:
        return jsonify({"ok": False, "erro": "Empresa não selecionada"}), 401

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global != "superusuario":
        if not usuario_tem_permissao(id_usuario, cod_empresa, "VISTORIAS", "EXECUTAR_VISTORIAS"):
            return jsonify({"ok": False, "erro": "Sem permissão para executar vistorias"}), 403

    dados = request.get_json(silent=True) or {}

    id_execucao_item = dados.get("id_execucao_item")
    atendido = dados.get("atendido") or "NAO"
    observacao = dados.get("observacao") or ""

    if not id_execucao_item:
        return jsonify({"ok": False, "erro": "Item não informado"}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT
                i.id_execucao_item,
                i.id_execucao,
                i.tipo_linha,
                i.pontos_possiveis,
                e.data_vistoria,
                e.status,
                e.cod_filial
            FROM vistorias_execucao_itens i
            JOIN vistorias_execucoes e
              ON e.id_execucao = i.id_execucao
            WHERE i.id_execucao_item = %s
              AND e.cod_empresa = %s
        """, (id_execucao_item, cod_empresa))

        item = cur.fetchone()

        if not item:
            return jsonify({"ok": False, "erro": "Item não encontrado"}), 404

        if item["status"] == "FINALIZADA":
            return jsonify({"ok": False, "erro": "Vistoria finalizada"}), 403

        if int(item["cod_filial"]) not in cod_filiais_vistorias_usuario(cur, cod_empresa):
            return jsonify({"ok": False, "erro": "Vistoria de outra filial"}), 403

        id_execucao = item["id_execucao"]
        pontos = float(item["pontos_possiveis"] or 0)
        pontuacao = pontos if atendido == "SIM" else 0

        cur.execute("""
            UPDATE vistorias_execucao_itens
               SET atendido = %s,
                   observacao = %s,
                   pontuacao = %s,
                   atualizado_em = NOW()
             WHERE id_execucao_item = %s
        """, (atendido, observacao, pontuacao, id_execucao_item))

        cur.execute("""
            SELECT
                COALESCE(SUM(pontos_possiveis), 0) AS total_possivel,
                COALESCE(SUM(pontuacao), 0) AS total_obtido
            FROM vistorias_execucao_itens
            WHERE id_execucao = %s
              AND tipo_linha = 'ITEM'
        """, (id_execucao,))

        totais = cur.fetchone()

        total_possivel = float(totais["total_possivel"] or 0)
        total_obtido = float(totais["total_obtido"] or 0)
        nota = (total_obtido / total_possivel) * 10 if total_possivel > 0 else 0

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

        return jsonify({"ok": True, "nota": nota, "pontuacao": pontuacao})

    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 500

    finally:
        cur.close()
        conn.close()