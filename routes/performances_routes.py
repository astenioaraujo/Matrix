from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify
from psycopg2.extras import RealDictCursor
from db import get_connection
from security_helpers import permissao_obrigatoria, usuario_tem_permissao
from datetime import date

performances_bp = Blueprint("performances", __name__)


# ---------------------------------------
# MENU PERFORMANCES
# ---------------------------------------
@performances_bp.route("/menu")
@permissao_obrigatoria(
    "PERFORMANCES",
    "MENU",
    redirecionar_para="sistema.selecionar_sistema",
)
def menu_performances():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_executar_avaliacoes = True
        pode_consultar_avaliacoes = True
        pode_configurar_avaliacoes = True
    else:
        pode_executar_avaliacoes = usuario_tem_permissao(
            id_usuario,
            cod_empresa,
            "PERFORMANCES",
            "EXECUTAR_AVALIACOES",
        )

        pode_consultar_avaliacoes = usuario_tem_permissao(
            id_usuario,
            cod_empresa,
            "PERFORMANCES",
            "CONSULTAR_AVALIACOES",
        )

        pode_configurar_avaliacoes = usuario_tem_permissao(
            id_usuario,
            cod_empresa,
            "PERFORMANCES",
            "CONFIGURAR_AVALIACOES",
        )

    return render_template(
        "menu_performances.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema"),
        texto_voltar="← Voltar",
        pode_executar_avaliacoes=pode_executar_avaliacoes,
        pode_consultar_avaliacoes=pode_consultar_avaliacoes,
        pode_configurar_avaliacoes=pode_configurar_avaliacoes,
    )

# ---------------------------------------
# NOVA AVALIAÇÃO
# ---------------------------------------

@performances_bp.route("/avaliacoes/nova", methods=["GET", "POST"])
@permissao_obrigatoria(
    "PERFORMANCES",
    "CONFIGURAR_AVALIACOES",
    redirecionar_para="performances.menu_performances",
)
def nova_avaliacao():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    if request.method == "POST":
        codigo_avaliacao = (request.form.get("codigo_avaliacao") or "").strip().upper()
        descricao = (request.form.get("descricao") or "").strip()

        if not codigo_avaliacao or not descricao:
            flash("Informe o código e a descrição da avaliação.", "error")
            return redirect(url_for("performances.nova_avaliacao"))

        conn = get_connection()
        cur = conn.cursor()

        try:
            cur.execute("""
                INSERT INTO performances_avaliacoes (
                    cod_empresa,
                    codigo_avaliacao,
                    descricao,
                    versao,
                    status,
                    criado_em,
                    atualizado_em
                )
                VALUES (%s, %s, %s, 1, 'ATIVO', NOW(), NOW())
                RETURNING id_avaliacao
            """, (cod_empresa, codigo_avaliacao, descricao))

            id_avaliacao = cur.fetchone()[0]
            conn.commit()

            return redirect(url_for("performances.editar_avaliacao", id_avaliacao=id_avaliacao))

        except Exception as e:
            conn.rollback()
            flash(f"Erro ao criar avaliação: {e}", "error")

        finally:
            cur.close()
            conn.close()

    return render_template(
        "nova_avaliacao.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("performances.configurar_avaliacoes"),
        texto_voltar="← Voltar",
    )

#--------------------------------------------------------------
# EXECUTAR AVALIACOES
#--------------------------------------------------------------

@performances_bp.route("/avaliacoes/executar", methods=["GET", "POST"])
@permissao_obrigatoria(
    "PERFORMANCES",
    "EXECUTAR_AVALIACOES",
    redirecionar_para="performances.menu_performances",
)
def executar_avaliacoes():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa")

    hoje = date.today()
    ano_sel = (request.args.get("ano") or str(hoje.year)).strip()
    mes_sel = (request.args.get("mes") or str(hoje.month)).strip().zfill(2)

    filial_sel = (request.args.get("filial") or "").strip()
    filial_execucao = filial_sel
    data_hoje = hoje.isoformat()

    data_ini = f"{ano_sel}-{mes_sel}-01"

    if mes_sel == "12":
        data_fim = f"{int(ano_sel) + 1}-01-01"
    else:
        data_fim = f"{ano_sel}-{str(int(mes_sel) + 1).zfill(2)}-01"

    filtro_filial_sql = ""
    params_filial = []

    if filial_sel:
        filtro_filial_sql = " AND e.cod_filial = %s "
        params_filial.append(int(filial_sel))

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
            SELECT id, codigo, descricao
            FROM cargos
            WHERE cod_empresa = %s
              AND ativo = TRUE
            ORDER BY descricao
        """, (cod_empresa,))
        cargos = cur.fetchall() or []

        funcionarios = []

        if filial_sel:
            cur.execute("""
                SELECT
                    f.id,
                    f.nome
                FROM funcionarios f
                WHERE f.cod_empresa = %s
                  AND f.cod_filial = %s
                  AND f.ativo = TRUE

                  AND NOT EXISTS (
                      SELECT 1
                      FROM performances_execucoes e
                      WHERE e.cod_empresa = f.cod_empresa
                        AND e.cod_filial = f.cod_filial
                        AND e.id_funcionario = f.id
                        AND e.data_avaliacao >= %s
                        AND e.data_avaliacao < %s
                  )

                ORDER BY f.nome
            """, (
                cod_empresa,
                int(filial_sel),
                data_ini,
                data_fim
            ))

            funcionarios = cur.fetchall() or []

        cur.execute("""
            SELECT id_avaliacao, codigo_avaliacao, descricao, versao
            FROM performances_avaliacoes
            WHERE cod_empresa = %s
              AND status = 'ATIVO'
            ORDER BY codigo_avaliacao, versao DESC
        """, (cod_empresa,))
        avaliacoes = cur.fetchall() or []

        if request.method == "POST":
            cod_filial = int(filial_sel or 0)
            id_avaliacao = int(request.form.get("id_avaliacao") or 0)
            id_funcionario = request.form.get("id_funcionario") or ""
            id_cargo = request.form.get("id_cargo") or ""
            data_avaliacao = request.form.get("data_avaliacao")

            if not cod_filial:
                flash("Selecione uma filial no filtro antes de iniciar a avaliação.", "error")
                return redirect(url_for("performances.executar_avaliacoes"))

            if not id_avaliacao or not data_avaliacao:
                flash("Informe questionário e data.", "error")
                return redirect(url_for(
                    "performances.executar_avaliacoes",
                    ano=ano_sel,
                    mes=mes_sel,
                    filial=filial_sel,
                ))

            nome_executor = (
                session.get("nome_usuario")
                or session.get("usuario")
                or f"Usuário {session.get('id_usuario')}"
            )

            # ---------------------------------------
            # CRIAÇÃO EM BLOCO POR CARGO
            # ---------------------------------------
            if id_cargo:
                cur.execute("""
                    SELECT
                        f.id,
                        f.nome
                    FROM funcionarios f
                    WHERE f.cod_empresa = %s
                      AND f.cod_filial = %s
                      AND f.id_cargo = %s
                      AND f.ativo = TRUE

                      AND NOT EXISTS (
                          SELECT 1
                          FROM performances_execucoes e
                          WHERE e.cod_empresa = f.cod_empresa
                            AND e.cod_filial = f.cod_filial
                            AND e.id_funcionario = f.id
                            AND e.data_avaliacao >= %s
                            AND e.data_avaliacao < %s
                      )

                    ORDER BY f.nome
                """, (
                    cod_empresa,
                    cod_filial,
                    int(id_cargo),
                    data_ini,
                    data_fim,
                ))

                funcionarios_bloco = cur.fetchall() or []

                if not funcionarios_bloco:
                    flash("Nenhum funcionário ativo desse cargo disponível para avaliação neste mês.", "error")
                    return redirect(url_for(
                        "performances.executar_avaliacoes",
                        ano=ano_sel,
                        mes=mes_sel,
                        filial=filial_sel,
                    ))

                primeira_execucao = None
                total_criados = 0

                for func in funcionarios_bloco:
                    cur.execute("""
                        INSERT INTO performances_execucoes (
                            cod_empresa,
                            id_avaliacao,
                            cod_filial,
                            data_avaliacao,
                            id_funcionario,
                            nome_avaliado,
                            status,
                            id_usuario_executor,
                            nome_executor,
                            criado_em,
                            atualizado_em
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, 'ABERTA', %s, %s, NOW(), NOW())
                        RETURNING id_execucao
                    """, (
                        cod_empresa,
                        id_avaliacao,
                        cod_filial,
                        data_avaliacao,
                        func["id"],
                        func["nome"],
                        session.get("id_usuario"),
                        nome_executor,
                    ))

                    id_execucao = cur.fetchone()["id_execucao"]

                    if primeira_execucao is None:
                        primeira_execucao = id_execucao

                    cur.execute("""
                        INSERT INTO performances_execucao_itens (
                            id_execucao,
                            id_item,
                            sequencia,
                            titulo,
                            detalhamento,
                            nota_item,
                            criado_em,
                            atualizado_em
                        )
                        SELECT
                            %s,
                            id_item,
                            sequencia,
                            titulo,
                            detalhamento,
                            0,
                            NOW(),
                            NOW()
                        FROM performances_avaliacao_itens
                        WHERE id_avaliacao = %s
                          AND ativo = TRUE
                        ORDER BY sequencia
                    """, (id_execucao, id_avaliacao))

                    total_criados += 1

                conn.commit()
                flash(f"{total_criados} avaliações criadas com sucesso.", "success")

                return redirect(url_for(
                    "performances.executar_avaliacoes",
                    ano=ano_sel,
                    mes=mes_sel,
                    filial=filial_sel,
                ))

            # ---------------------------------------
            # CRIAÇÃO INDIVIDUAL
            # ---------------------------------------
            if not id_funcionario:
                flash("Selecione um avaliado ou escolha um cargo para criar em bloco.", "error")
                return redirect(url_for(
                    "performances.executar_avaliacoes",
                    ano=ano_sel,
                    mes=mes_sel,
                    filial=filial_sel,
                ))

            cur.execute("""
                SELECT nome
                FROM funcionarios
                WHERE id = %s
                  AND cod_empresa = %s
                  AND cod_filial = %s
                  AND ativo = TRUE
            """, (id_funcionario, cod_empresa, cod_filial))

            row_func = cur.fetchone()

            if not row_func:
                flash("Funcionário não encontrado para esta filial.", "error")
                return redirect(url_for(
                    "performances.executar_avaliacoes",
                    ano=ano_sel,
                    mes=mes_sel,
                    filial=filial_sel,
                ))

            nome_avaliado = row_func["nome"]

            cur.execute("""
                INSERT INTO performances_execucoes (
                    cod_empresa,
                    id_avaliacao,
                    cod_filial,
                    data_avaliacao,
                    id_funcionario,
                    nome_avaliado,
                    status,
                    id_usuario_executor,
                    nome_executor,
                    criado_em,
                    atualizado_em
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'ABERTA', %s, %s, NOW(), NOW())
                RETURNING id_execucao
            """, (
                cod_empresa,
                id_avaliacao,
                cod_filial,
                data_avaliacao,
                id_funcionario,
                nome_avaliado,
                session.get("id_usuario"),
                nome_executor,
            ))

            id_execucao = cur.fetchone()["id_execucao"]

            cur.execute("""
                INSERT INTO performances_execucao_itens (
                    id_execucao,
                    id_item,
                    sequencia,
                    titulo,
                    detalhamento,
                    nota_item,
                    criado_em,
                    atualizado_em
                )
                SELECT
                    %s,
                    id_item,
                    sequencia,
                    titulo,
                    detalhamento,
                    0,
                    NOW(),
                    NOW()
                FROM performances_avaliacao_itens
                WHERE id_avaliacao = %s
                  AND ativo = TRUE
                ORDER BY sequencia
            """, (id_execucao, id_avaliacao))

            conn.commit()

            return redirect(url_for("performances.preencher_avaliacao", id_execucao=id_execucao))

        sql = f"""
            SELECT
                e.id_execucao,
                e.data_avaliacao,
                e.status,
                e.nome_avaliado,
                COALESCE(e.nota, 0) AS nota,
                e.nome_executor,
                f.cod_filial,
                f.nome_filial,
                a.codigo_avaliacao,
                a.descricao AS avaliacao_descricao,
                a.versao
            FROM performances_execucoes e
            LEFT JOIN filiais f
              ON f.cod_empresa = e.cod_empresa
             AND f.cod_filial = e.cod_filial
            LEFT JOIN performances_avaliacoes a
              ON a.id_avaliacao = e.id_avaliacao
            WHERE e.cod_empresa = %s
              AND e.data_avaliacao >= %s
              AND e.data_avaliacao < %s
              {filtro_filial_sql}
            ORDER BY e.data_avaliacao DESC, e.id_execucao DESC
        """

        params = [cod_empresa, data_ini, data_fim] + params_filial
        cur.execute(sql, params)

        avaliacoes_mes = cur.fetchall() or []

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao executar avaliação: {e}", "error")
        filiais = []
        cargos = []
        avaliacoes = []
        avaliacoes_mes = []
        funcionarios = []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "executar_avaliacoes.html",
        nome_empresa=nome_empresa,
        filiais=filiais,
        cargos=cargos,
        funcionarios=funcionarios,
        avaliacoes=avaliacoes,
        avaliacoes_mes=avaliacoes_mes,
        ano_sel=ano_sel,
        mes_sel=mes_sel,
        filial_sel=filial_sel,
        filial_execucao=filial_execucao,
        data_hoje=data_hoje,
        url_voltar=url_for("performances.menu_performances"),
        texto_voltar="← Voltar",
    )
# ---------------------------------------
# EDITAR AVALIAÇÃO
# ---------------------------------------
@performances_bp.route("/avaliacoes/<int:id_avaliacao>/editar")
@permissao_obrigatoria(
    "PERFORMANCES",
    "CONFIGURAR_AVALIACOES",
    redirecionar_para="performances.menu_performances",
)
def editar_avaliacao(id_avaliacao):
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
            FROM performances_avaliacoes
            WHERE id_avaliacao = %s
              AND cod_empresa = %s
        """, (id_avaliacao, cod_empresa))

        avaliacao = cur.fetchone()

        if not avaliacao:
            flash("Avaliação não encontrada.", "error")
            return redirect(url_for("performances.configurar_avaliacoes"))

        cur.execute("""
            SELECT *
            FROM performances_avaliacao_itens
            WHERE id_avaliacao = %s
              AND ativo = TRUE
            ORDER BY sequencia
        """, (id_avaliacao,))

        itens = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "editar_avaliacao.html",
        nome_empresa=session.get("nome_empresa"),
        avaliacao=avaliacao,
        itens=itens,
        url_voltar=url_for("performances.configurar_avaliacoes"),
        texto_voltar="← Voltar",
    )
# ---------------------------------------
# CONSULTAR AVALIAÇÕES
# ---------------------------------------
@performances_bp.route("/avaliacoes/consultar")
@permissao_obrigatoria(
    "PERFORMANCES",
    "CONSULTAR_AVALIACOES",
    redirecionar_para="performances.menu_performances",
)
def consultar_avaliacoes():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    return render_template(
        "consultar_avaliacoes.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("performances.menu_performances"),
        texto_voltar="← Voltar",
    )


# ---------------------------------------
# CONFIGURAR AVALIAÇÕES - LISTA
# ---------------------------------------
@performances_bp.route("/avaliacoes/configurar")
@permissao_obrigatoria(
    "PERFORMANCES",
    "CONFIGURAR_AVALIACOES",
    redirecionar_para="performances.menu_performances",
)
def configurar_avaliacoes():
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
                id_avaliacao,
                codigo_avaliacao,
                descricao,
                versao,
                status,
                criado_em,
                atualizado_em
            FROM performances_avaliacoes
            WHERE cod_empresa = %s
            ORDER BY codigo_avaliacao, versao DESC
        """, (cod_empresa,))

        avaliacoes = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "configurar_avaliacoes.html",
        nome_empresa=session.get("nome_empresa"),
        avaliacoes=avaliacoes,
        url_voltar=url_for("performances.menu_performances"),
        texto_voltar="← Voltar",
    )

# ---------------------------------------
# ADICIONAR ITEM DA AVALIAÇÃO
# ---------------------------------------
@performances_bp.route("/avaliacoes/<int:id_avaliacao>/itens/adicionar", methods=["POST"])
@permissao_obrigatoria(
    "PERFORMANCES",
    "CONFIGURAR_AVALIACOES",
    redirecionar_para="performances.menu_performances",
)
def adicionar_item_avaliacao(id_avaliacao):
    sequencia = request.form.get("sequencia") or 0
    titulo = (request.form.get("titulo") or "").strip()
    detalhamento = (request.form.get("detalhamento") or "").strip()

    if not titulo:
        flash("Informe a pergunta.", "error")
        return redirect(url_for("performances.editar_avaliacao", id_avaliacao=id_avaliacao))

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO performances_avaliacao_itens (
                id_avaliacao,
                sequencia,
                titulo,
                detalhamento,
                ativo,
                criado_em,
                atualizado_em
            )
            VALUES (%s, %s, %s, %s, TRUE, NOW(), NOW())
        """, (id_avaliacao, sequencia, titulo, detalhamento))

        conn.commit()
        flash("Pergunta adicionada com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao adicionar pergunta: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("performances.editar_avaliacao", id_avaliacao=id_avaliacao))


# ---------------------------------------
# SALVAR ITENS DA AVALIAÇÃO
# ---------------------------------------
@performances_bp.route("/avaliacoes/<int:id_avaliacao>/itens/salvar", methods=["POST"])
@permissao_obrigatoria(
    "PERFORMANCES",
    "CONFIGURAR_AVALIACOES",
    redirecionar_para="performances.menu_performances",
)
def salvar_itens_avaliacao(id_avaliacao):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT id_item
            FROM performances_avaliacao_itens
            WHERE id_avaliacao = %s
              AND ativo = TRUE
        """, (id_avaliacao,))

        itens = cur.fetchall() or []

        for item in itens:
            id_item = item["id_item"]

            sequencia = request.form.get(f"sequencia_{id_item}") or 0
            titulo = request.form.get(f"titulo_{id_item}") or ""
            detalhamento = request.form.get(f"detalhamento_{id_item}") or ""

            cur.execute("""
                UPDATE performances_avaliacao_itens
                SET
                    sequencia = %s,
                    titulo = %s,
                    detalhamento = %s,
                    atualizado_em = NOW()
                WHERE id_item = %s
                  AND id_avaliacao = %s
            """, (
                sequencia,
                titulo.strip(),
                detalhamento.strip(),
                id_item,
                id_avaliacao,
            ))

        conn.commit()
        flash("Perguntas salvas com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao salvar perguntas: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("performances.editar_avaliacao", id_avaliacao=id_avaliacao))


# ---------------------------------------
# EXCLUIR ITEM DA AVALIAÇÃO
# ---------------------------------------
@performances_bp.route("/avaliacoes/<int:id_avaliacao>/itens/<int:id_item>/excluir", methods=["POST"])
@permissao_obrigatoria(
    "PERFORMANCES",
    "CONFIGURAR_AVALIACOES",
    redirecionar_para="performances.menu_performances",
)
def excluir_item_avaliacao(id_avaliacao, id_item):
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE performances_avaliacao_itens
            SET ativo = FALSE,
                atualizado_em = NOW()
            WHERE id_avaliacao = %s
              AND id_item = %s
        """, (id_avaliacao, id_item))

        conn.commit()
        flash("Pergunta excluída com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao excluir pergunta: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("performances.editar_avaliacao", id_avaliacao=id_avaliacao))

#-----------------------------------------------
# FUNCIONARIOS POR FILIAL
#-----------------------------------------------


@performances_bp.route("/funcionarios-por-filial/<int:cod_filial>")
@permissao_obrigatoria(
    "PERFORMANCES",
    "EXECUTAR_AVALIACOES",
    redirecionar_para="performances.menu_performances",
)
def funcionarios_por_filial(cod_filial):
    if "cod_empresa" not in session:
        return jsonify({"ok": False, "erro": "Empresa não selecionada", "funcionarios": []}), 401

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT
                id,
                nome
            FROM funcionarios
            WHERE cod_empresa = %s
              AND cod_filial = %s
              AND ativo = TRUE
            ORDER BY nome
        """, (cod_empresa, cod_filial))

        funcionarios = cur.fetchall() or []

        return jsonify({
            "ok": True,
            "funcionarios": funcionarios
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "erro": str(e),
            "funcionarios": []
        }), 500

    finally:
        cur.close()
        conn.close()

# ---------------------------------------
# PREENCHER AVALIAÇÃO
# ---------------------------------------
@performances_bp.route("/avaliacoes/execucao/<int:id_execucao>", methods=["GET", "POST"])
@permissao_obrigatoria(
    "PERFORMANCES",
    "EXECUTAR_AVALIACOES",
    redirecionar_para="performances.menu_performances",
)
def preencher_avaliacao(id_execucao):
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            cur.execute("""
                SELECT id_execucao_item
                FROM performances_execucao_itens
                WHERE id_execucao = %s
            """, (id_execucao,))
            itens = cur.fetchall() or []

            total_obtido = 0
            total_possivel = 0

            for item in itens:
                id_item = item["id_execucao_item"]

                nota_item = request.form.get(f"nota_item_{id_item}") or "0"
                observacao = request.form.get(f"observacao_{id_item}") or ""

                try:
                    nota_item = float(nota_item)
                except ValueError:
                    nota_item = 0

                if nota_item < 0:
                    nota_item = 0
                if nota_item > 5:
                    nota_item = 5

                total_obtido += nota_item
                total_possivel += 5

                cur.execute("""
                    UPDATE performances_execucao_itens
                    SET
                        nota_item = %s,
                        observacao = %s,
                        atualizado_em = NOW()
                    WHERE id_execucao_item = %s
                      AND id_execucao = %s
                """, (
                    nota_item,
                    observacao.strip(),
                    id_item,
                    id_execucao,
                ))

            nota_final = 0
            percentual = 0

            if total_possivel > 0:
                percentual = (total_obtido / total_possivel) * 100
                nota_final = (total_obtido / total_possivel) * 10

            cur.execute("""
                UPDATE performances_execucoes
                SET
                    pontuacao_possivel = %s,
                    pontuacao_obtida = %s,
                    nota = %s,
                    atualizado_em = NOW()
                WHERE id_execucao = %s
                  AND cod_empresa = %s
            """, (
                total_possivel,
                total_obtido,
                nota_final,
                id_execucao,
                cod_empresa,
            ))

            conn.commit()
            flash("Avaliação salva com sucesso.", "success")

            return redirect(url_for("performances.preencher_avaliacao", id_execucao=id_execucao))

        cur.execute("""
            SELECT
                e.*,
                f.nome_filial,
                a.codigo_avaliacao,
                a.descricao AS avaliacao_descricao,
                a.versao
            FROM performances_execucoes e
            LEFT JOIN filiais f
              ON f.cod_empresa = e.cod_empresa
             AND f.cod_filial = e.cod_filial
            LEFT JOIN performances_avaliacoes a
              ON a.id_avaliacao = e.id_avaliacao
            WHERE e.id_execucao = %s
              AND e.cod_empresa = %s
        """, (id_execucao, cod_empresa))

        execucao = cur.fetchone()

        if not execucao:
            flash("Avaliação não encontrada.", "error")
            return redirect(url_for("performances.executar_avaliacoes"))

        cur.execute("""
            SELECT *
            FROM performances_execucao_itens
            WHERE id_execucao = %s
            ORDER BY sequencia
        """, (id_execucao,))

        itens = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "preencher_avaliacao.html",
        execucao=execucao,
        itens=itens,
        url_voltar=url_for("performances.executar_avaliacoes"),
        texto_voltar="← Voltar",
    )

# ---------------------------------------
# SALVAR ITEM DA AVALIAÇÃO VIA AJAX
# ---------------------------------------
@performances_bp.route("/avaliacoes/item/salvar-ajax", methods=["POST"])
@permissao_obrigatoria(
    "PERFORMANCES",
    "EXECUTAR_AVALIACOES",
    redirecionar_para="performances.menu_performances",
)
def salvar_item_avaliacao_ajax():
    if "cod_empresa" not in session:
        return jsonify({"ok": False, "erro": "Empresa não selecionada"}), 401

    cod_empresa = str(session["cod_empresa"]).strip()

    dados = request.get_json(silent=True) or {}

    id_execucao_item = dados.get("id_execucao_item")
    nota_item = dados.get("nota_item")

    if not id_execucao_item:
        return jsonify({"ok": False, "erro": "Item não informado"}), 400

    try:
        nota_item = float(nota_item or 0)
    except ValueError:
        nota_item = 0

    if nota_item < 0:
        nota_item = 0

    if nota_item > 5:
        nota_item = 5

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT i.id_execucao
            FROM performances_execucao_itens i
            JOIN performances_execucoes e
              ON e.id_execucao = i.id_execucao
            WHERE i.id_execucao_item = %s
              AND e.cod_empresa = %s
        """, (id_execucao_item, cod_empresa))

        row = cur.fetchone()

        if not row:
            return jsonify({"ok": False, "erro": "Item não encontrado"}), 404

        id_execucao = row["id_execucao"]

        cur.execute("""
            UPDATE performances_execucao_itens
            SET nota_item = %s,
                atualizado_em = NOW()
            WHERE id_execucao_item = %s
        """, (nota_item, id_execucao_item))

        cur.execute("""
            SELECT
                COALESCE(SUM(nota_item), 0) AS total_obtido,
                COUNT(*) * 5 AS total_possivel
            FROM performances_execucao_itens
            WHERE id_execucao = %s
        """, (id_execucao,))

        totais = cur.fetchone()

        total_obtido = float(totais["total_obtido"] or 0)
        total_possivel = float(totais["total_possivel"] or 0)

        nota_final = 0
        atendido = 0

        if total_possivel > 0:
            atendido = (total_obtido / total_possivel) * 100
            nota_final = (total_obtido / total_possivel) * 10

        cur.execute("""
            UPDATE performances_execucoes
            SET pontuacao_possivel = %s,
                pontuacao_obtida = %s,
                nota = %s,
                atualizado_em = NOW()
            WHERE id_execucao = %s
              AND cod_empresa = %s
        """, (
            total_possivel,
            total_obtido,
            nota_final,
            id_execucao,
            cod_empresa,
        ))

        conn.commit()

        return jsonify({
            "ok": True,
            "nota_item": nota_item,
            "pontos": total_obtido,
            "atendido": atendido,
            "nota_final": nota_final,
        })

    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 500

    finally:
        cur.close()
        conn.close()