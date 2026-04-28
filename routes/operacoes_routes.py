from flask import Blueprint, render_template, session, redirect, url_for, flash, request
from psycopg2.extras import RealDictCursor
from datetime import datetime, date, timedelta

from db import get_connection
from security_helpers import (
    permissao_obrigatoria,
    usuario_tem_permissao,
    usuario_filiais_ativas,
)

operacoes_bp = Blueprint("operacoes", __name__)

def parse_valor_br(valor_txt):
    valor_txt = (valor_txt or "").strip()

    if not valor_txt:
        return 0.0

    valor_txt = valor_txt.replace(".", "").replace(",", ".")

    try:
        return float(valor_txt)
    except ValueError:
        return 0.0

def formatar_numero_br(valor):
    try:
        return f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "0,00"

#------------------------------------------
# MENU OPERACOES
#------------------------------------------
@operacoes_bp.route("/menu")
@permissao_obrigatoria("OPERACOES", "MENU")
def menu_operacoes():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()


    if tipo_global == "superusuario":
        permissoes = {
            "pode_informar_medicoes": True,
            "pode_consultar_medicoes": True,
            "pode_informar_preco_compra": True,
            "pode_consultar_preco_compra": True,
            "pode_informar_compras": True,
            "pode_informar_descarregos": True,
            "pode_consultar_estoques": True,
            "pode_consultar_vendas": True,
            "pode_configuracoes": True,
        }
    else:
        permissoes = {
            "pode_informar_medicoes": usuario_tem_permissao(
                id_usuario, cod_empresa, "OPERACOES", "INFORMAR_MEDICOES"
            ),
            "pode_consultar_medicoes": usuario_tem_permissao(
                id_usuario, cod_empresa, "OPERACOES", "CONSULTAR_MEDICOES"
            ),
            "pode_informar_preco_compra": usuario_tem_permissao(
                id_usuario, cod_empresa, "OPERACOES", "INFORMAR_PRECO_COMPRA"
            ),
            "pode_consultar_preco_compra": usuario_tem_permissao(
                id_usuario, cod_empresa, "OPERACOES", "CONSULTAR_PRECO_COMPRA"
            ),
            "pode_informar_compras": usuario_tem_permissao(
                id_usuario, cod_empresa, "OPERACOES", "INFORMAR_COMPRAS"
            ),
            "pode_informar_descarregos": usuario_tem_permissao(
                id_usuario, cod_empresa, "OPERACOES", "INFORMAR_DESCARREGOS"
            ),
            "pode_consultar_estoques": usuario_tem_permissao(
                id_usuario, cod_empresa, "OPERACOES", "CONSULTAR_ESTOQUES"
            ),
            "pode_consultar_vendas": usuario_tem_permissao(
                id_usuario, cod_empresa, "OPERACOES", "CONSULTAR_VENDAS"
            ),
            "pode_configuracoes": usuario_tem_permissao(
                id_usuario, cod_empresa, "OPERACOES", "CONFIGURACOES"
            ),
        }

    return render_template(
        "menu_operacoes.html",
        cod_empresa=session.get("cod_empresa", ""),
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("sistema.selecionar_sistema"),
        texto_voltar="← Voltar",
        **permissoes
    )

# ---------------------------------------
# INFORMAR MEDICOES
# ---------------------------------------
@operacoes_bp.route("/medicoes/informar", methods=["GET", "POST"])
@permissao_obrigatoria(
    "OPERACOES",
    "INFORMAR_MEDICOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def informar_medicoes():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    hoje = date.today()

    # BLOQUEIO REMOVIDO
    bloqueado_horario = False

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if tipo_global == "superusuario":
            cur.execute(
                """
                SELECT cod_filial, nome_filial
                FROM filiais
                WHERE cod_empresa = %s
                  AND ativo = TRUE
                ORDER BY cod_filial
                """,
                (cod_empresa,),
            )
            filiais = cur.fetchall() or []
        else:
            filiais_permitidas = [int(x) for x in usuario_filiais_ativas(id_usuario, cod_empresa)]

            if not filiais_permitidas:
                flash("Você não possui filiais habilitadas.", "error")
                return redirect(url_for("operacoes.menu_operacoes"))

            cur.execute(
                """
                SELECT cod_filial, nome_filial
                FROM filiais
                WHERE cod_empresa = %s
                  AND ativo = TRUE
                  AND cod_filial = ANY(%s)
                ORDER BY cod_filial
                """,
                (cod_empresa, filiais_permitidas),
            )
            filiais = cur.fetchall() or []

        filial_sel_txt = (request.values.get("cod_filial") or "").strip()
        filial_sel = int(filial_sel_txt) if filial_sel_txt.isdigit() else None

        if filial_sel is None and len(filiais) == 1:
            filial_sel = int(filiais[0]["cod_filial"])

        codigos_filiais_permitidas = [int(f["cod_filial"]) for f in filiais]

        if filial_sel is not None and filial_sel not in codigos_filiais_permitidas and tipo_global != "superusuario":
            flash("Filial não permitida para este usuário.", "error")
            return redirect(url_for("operacoes.informar_medicoes"))

        produtos = []
        medicoes_existentes = {}

        if filial_sel is not None:
            cur.execute(
                """
                SELECT
                    c.cod_produto,
                    c.descricao,
                    COALESCE(ct.capacidade_tanque, 0) AS capacidade_tanque
                FROM combustiveis c
                JOIN capacidade_tanques ct
                  ON ct.cod_empresa = c.cod_empresa
                 AND ct.cod_filial = %s
                 AND ct.cod_produto = c.cod_produto
                WHERE c.cod_empresa = %s
                  AND COALESCE(ct.capacidade_tanque, 0) > 0
                ORDER BY c.cod_produto
                """,
                (filial_sel, cod_empresa),
            )
            produtos = cur.fetchall() or []

            cur.execute(
                """
                SELECT
                    cod_produto,
                    quantidade_medida,
                    quantidade_descarregada,
                    quantidade_vendida
                FROM medicoes
                WHERE cod_empresa = %s
                  AND cod_filial = %s
                  AND data_medicao = %s
                """,
                (cod_empresa, filial_sel, hoje),
            )
            rows = cur.fetchall() or []

            for r in rows:
                medicoes_existentes[str(r["cod_produto"]).strip()] = {
                    "quantidade_medida": r["quantidade_medida"],
                    "quantidade_descarregada": r["quantidade_descarregada"],
                    "quantidade_vendida": r["quantidade_vendida"],
                }

        if request.method == "POST":
            try:
                cod_filial = int(request.form.get("cod_filial") or 0)
            except ValueError:
                return {"ok": False, "erro": "Filial inválida."}, 400

            if not cod_filial:
                flash("Selecione a filial.", "error")
                return redirect(url_for("operacoes.informar_medicoes"))

            if tipo_global != "superusuario" and cod_filial not in codigos_filiais_permitidas:
                flash("Filial não permitida para este usuário.", "error")
                return redirect(url_for("operacoes.informar_medicoes"))

            cur.execute(
                """
                SELECT
                    c.cod_produto,
                    COALESCE(ct.capacidade_tanque, 0) AS capacidade_tanque
                FROM combustiveis c
                JOIN capacidade_tanques ct
                  ON ct.cod_empresa = c.cod_empresa
                 AND ct.cod_filial = %s
                 AND ct.cod_produto = c.cod_produto
                WHERE c.cod_empresa = %s
                  AND COALESCE(ct.capacidade_tanque, 0) > 0
                ORDER BY c.cod_produto
                """,
                (cod_filial, cod_empresa),
            )
            produtos_salvar = cur.fetchall() or []

            for p in produtos_salvar:
                cod_produto = str(p["cod_produto"]).strip()

                valor_txt = (request.form.get(f"med_{cod_produto}") or "0").strip()
                valor_txt = valor_txt.replace(".", "").replace(",", ".")

                try:
                    quantidade = float(valor_txt) if valor_txt else 0.0
                except ValueError:
                    quantidade = 0.0

                cur.execute(
                    """
                    INSERT INTO medicoes (
                        cod_empresa,
                        cod_filial,
                        data_medicao,
                        cod_produto,
                        quantidade_medida,
                        criado_em,
                        atualizado_em
                    )
                    VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (cod_empresa, cod_filial, data_medicao, cod_produto)
                    DO UPDATE SET
                        quantidade_medida = EXCLUDED.quantidade_medida,
                        atualizado_em = NOW()
                    """,
                    (cod_empresa, cod_filial, hoje, cod_produto, quantidade),
                )

            conn.commit()
            flash("Medições salvas com sucesso.", "success")
            return redirect(url_for("operacoes.informar_medicoes", cod_filial=cod_filial))

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao processar medições: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return render_template(
        "informar_medicoes.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        hoje=hoje.strftime("%d/%m/%Y"),
        hoje_iso=hoje.isoformat(),
        bloqueado_horario=bloqueado_horario,
        filiais=filiais,
        filial_sel=filial_sel,
        produtos=produtos,
        medicoes_existentes=medicoes_existentes,
        url_voltar=url_for("operacoes.menu_operacoes"),
        texto_voltar="← Voltar",
    )

#------------------------------------------
# CONFIGURACOES DE OPERACOES
#------------------------------------------
@operacoes_bp.route("/configuracoes")
@permissao_obrigatoria(
    "OPERACOES",
    "CONFIGURACOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def menu_configuracoes():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_capacidade_tanques = True
    else:
        pode_capacidade_tanques = usuario_tem_permissao(
            id_usuario, cod_empresa, "OPERACOES", "CAPACIDADE_TANQUES"
        )

    return render_template(
        "menu_configuracoes_operacoes.html",
        cod_empresa=session.get("cod_empresa", ""),
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("operacoes.menu_operacoes"),
        texto_voltar="← Voltar",
        pode_capacidade_tanques=pode_capacidade_tanques,
    )

# ---------------------------------------
# CAPACIDADE DE TANQUES
# ---------------------------------------

@operacoes_bp.route("/configuracoes/capacidade-tanques")
@permissao_obrigatoria(
    "OPERACOES",
    "CONFIGURACOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def capacidade_tanques():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT
                ct.cod_filial,
                f.nome_filial,
                ct.cod_produto,
                c.descricao,
                ct.capacidade_tanque
            FROM capacidade_tanques ct
            LEFT JOIN filiais f
              ON f.cod_empresa = ct.cod_empresa
             AND f.cod_filial = ct.cod_filial
            LEFT JOIN combustiveis c
              ON c.cod_empresa = ct.cod_empresa
             AND c.cod_produto = ct.cod_produto
            WHERE ct.cod_empresa = %s
            ORDER BY ct.cod_filial, ct.cod_produto
        """, (cod_empresa,))

        linhas = cur.fetchall() or []

        # AGRUPAR POR FILIAL
        filiais = {}
        for l in linhas:
            chave = l["cod_filial"]

            if chave not in filiais:
                filiais[chave] = {
                    "nome_filial": l["nome_filial"],
                    "itens": []
                }

            filiais[chave]["itens"].append(l)

    finally:
        cur.close()
        conn.close()

    return render_template(
        "capacidade_tanques.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        filiais=filiais,
        url_voltar=url_for("operacoes.menu_operacoes"),
        texto_voltar="← Voltar",
    )

# ---------------------------------------
# CONSULTAR MEDICOES
# ---------------------------------------

@operacoes_bp.route("/medicoes/consultar")
@permissao_obrigatoria(
    "OPERACOES",
    "CONSULTAR_MEDICOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def consultar_medicoes():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    data_sel = (request.args.get("data") or "").strip()
    if not data_sel:
        data_sel = date.today().isoformat()

    filial_sel_txt = (request.args.get("cod_filial") or "").strip()
    filial_sel = int(filial_sel_txt) if filial_sel_txt.isdigit() else None

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if tipo_global == "superusuario":
            cur.execute("""
                SELECT cod_filial, nome_filial
                FROM filiais
                WHERE cod_empresa = %s
                  AND ativo = TRUE
                ORDER BY cod_filial
            """, (cod_empresa,))
            filiais = cur.fetchall() or []
        else:
            filiais_permitidas = [
                int(x) for x in usuario_filiais_ativas(id_usuario, cod_empresa)
            ]

            if not filiais_permitidas:
                flash("Você não possui filiais habilitadas.", "error")
                return redirect(url_for("operacoes.menu_operacoes"))

            cur.execute("""
                SELECT cod_filial, nome_filial
                FROM filiais
                WHERE cod_empresa = %s
                  AND ativo = TRUE
                  AND cod_filial = ANY(%s)
                ORDER BY cod_filial
            """, (cod_empresa, filiais_permitidas))
            filiais = cur.fetchall() or []

        codigos_filiais_permitidas = [int(f["cod_filial"]) for f in filiais]

        if filial_sel is not None and tipo_global != "superusuario" and filial_sel not in codigos_filiais_permitidas:
            flash("Filial não permitida para este usuário.", "error")
            return redirect(url_for("operacoes.consultar_medicoes"))

        filtros = ["m.cod_empresa = %s"]
        params = [cod_empresa]

        if data_sel:
            filtros.append("m.data_medicao = %s")
            params.append(data_sel)

        if filial_sel is not None:
            filtros.append("m.cod_filial = %s")
            params.append(filial_sel)
        elif tipo_global != "superusuario":
            filtros.append("m.cod_filial = ANY(%s)")
            params.append(codigos_filiais_permitidas)

        where_sql = " AND ".join(filtros)

        cur.execute(f"""
            SELECT
                m.cod_filial,
                f.nome_filial,
                m.data_medicao,
                m.cod_produto,
                c.descricao,
                COALESCE(ct.capacidade_tanque, 0) AS capacidade_tanque,
                m.quantidade_medida,
                m.quantidade_descarregada,
                m.quantidade_vendida
            FROM medicoes m
            LEFT JOIN filiais f
              ON f.cod_empresa = m.cod_empresa
             AND f.cod_filial = m.cod_filial
            LEFT JOIN combustiveis c
              ON c.cod_empresa = m.cod_empresa
             AND c.cod_produto = m.cod_produto
            LEFT JOIN capacidade_tanques ct
              ON ct.cod_empresa = m.cod_empresa
             AND ct.cod_filial = m.cod_filial
             AND ct.cod_produto = m.cod_produto
            WHERE {where_sql}
              AND COALESCE(ct.capacidade_tanque, 0) > 0
            ORDER BY m.data_medicao DESC, m.cod_filial, m.cod_produto
        """, params)

        linhas = cur.fetchall() or []

        # FILIAIS SEM MEDIÇÃO NA DATA
        filtros_alerta = ["f.cod_empresa = %s", "f.ativo = TRUE"]
        params_where_alerta = [cod_empresa]

        if filial_sel is not None:
            filtros_alerta.append("f.cod_filial = %s")
            params_where_alerta.append(filial_sel)
        elif tipo_global != "superusuario":
            filtros_alerta.append("f.cod_filial = ANY(%s)")
            params_where_alerta.append(codigos_filiais_permitidas)

        where_alerta = " AND ".join(filtros_alerta)

        params_alerta = [data_sel] + params_where_alerta

        cur.execute(f"""
            SELECT
                f.cod_filial,
                f.nome_filial
            FROM filiais f
            LEFT JOIN medicoes m
            ON m.cod_empresa = f.cod_empresa
            AND m.cod_filial = f.cod_filial
            AND m.data_medicao = %s
            WHERE {where_alerta}
            GROUP BY f.cod_filial, f.nome_filial
            HAVING COALESCE(SUM(COALESCE(m.quantidade_medida, 0)), 0) = 0
            ORDER BY f.cod_filial
        """, params_alerta)

        filiais_sem_medicao = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "consultar_medicoes.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        filiais=filiais,
        filial_sel=filial_sel,
        data_sel=data_sel,
        linhas=linhas,
        filiais_sem_medicao=filiais_sem_medicao,
        url_voltar=url_for("operacoes.menu_operacoes"),
        texto_voltar="← Voltar",
    )

# --------------------------------
# INFORMAR PREÇOS
# --------------------------------
@operacoes_bp.route("/precos-compra/informar")
@permissao_obrigatoria(
    "OPERACOES",
    "INFORMAR_PRECO_COMPRA",
    redirecionar_para="operacoes.menu_operacoes",
)
def informar_preco_compra():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")
    data_sel = (request.args.get("data") or "").strip()

    if not data_sel:
        data_sel = date.today().isoformat()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT cod_produto, descricao
            FROM combustiveis
            WHERE cod_empresa = %s
            ORDER BY cod_produto
        """, (cod_empresa,))
        produtos = cur.fetchall() or []

        cur.execute("""
            SELECT cod_produto, preco_compra
            FROM precos_compra
            WHERE cod_empresa = %s
              AND data_preco = %s
        """, (cod_empresa, data_sel))
        rows = cur.fetchall() or []

        precos_existentes = {
            str(r["cod_produto"]).strip(): r["preco_compra"]
            for r in rows
        }

    finally:
        cur.close()
        conn.close()

    return render_template(
        "informar_preco_compra.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        data_sel=data_sel,
        produtos=produtos,
        precos_existentes=precos_existentes,
        url_voltar=url_for("operacoes.menu_operacoes"),
        texto_voltar="← Voltar",
    )


# --------------------------------
# CONSULTAR PREÇOS DE COMPRA
# --------------------------------

@operacoes_bp.route("/precos-compra/consultar")
@permissao_obrigatoria(
    "OPERACOES",
    "CONSULTAR_PRECO_COMPRA",
    redirecionar_para="operacoes.menu_operacoes",
)
def consultar_preco_compra():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")
    data_ini = (request.args.get("data_ini") or "").strip()

    if not data_ini:
        data_ini = date.today().replace(day=1).isoformat()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT cod_produto, descricao
            FROM combustiveis
            WHERE cod_empresa = %s
            ORDER BY cod_produto
        """, (cod_empresa,))
        produtos = cur.fetchall() or []

        cur.execute("""
            SELECT
                data_preco,
                cod_produto,
                preco_compra
            FROM precos_compra
            WHERE cod_empresa = %s
              AND data_preco >= %s
            ORDER BY data_preco DESC, cod_produto
        """, (cod_empresa, data_ini))
        rows = cur.fetchall() or []

        linhas_por_data = {}
        for r in rows:
            d = r["data_preco"]
            if d not in linhas_por_data:
                linhas_por_data[d] = {}
            linhas_por_data[d][str(r["cod_produto"]).strip()] = r["preco_compra"]

        datas = sorted(linhas_por_data.keys(), reverse=True)

    finally:
        cur.close()
        conn.close()

    return render_template(
        "consultar_preco_compra.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        data_ini=data_ini,
        produtos=produtos,
        datas=datas,
        linhas_por_data=linhas_por_data,
        url_voltar=url_for("operacoes.menu_operacoes"),
        texto_voltar="← Voltar",
    )

# ---------------------------------------
# FORNECEDORES DE COMBUSTÍVEIS
# ---------------------------------------

@operacoes_bp.route("/configuracoes/fornecedores-combustiveis", methods=["GET", "POST"])
@permissao_obrigatoria(
    "OPERACOES",
    "CONFIGURACOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def fornecedores_combustiveis():
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
            id_fornecedor = (request.form.get("id_fornecedor") or "").strip()
            nome_fornecedor = (request.form.get("nome_fornecedor") or "").strip()
            cidade_base = (request.form.get("cidade_base") or "").strip()
            ativo = True if request.form.get("ativo") == "on" else False

            if not nome_fornecedor:
                flash("Informe o nome do fornecedor.", "error")
                return redirect(url_for("operacoes.fornecedores_combustiveis"))

            if id_fornecedor:
                cur.execute("""
                    UPDATE fornecedores_combustiveis
                    SET nome_fornecedor = %s,
                        cidade_base = %s,
                        ativo = %s,
                        atualizado_em = NOW()
                    WHERE id_fornecedor = %s
                      AND cod_empresa = %s
                """, (
                    nome_fornecedor,
                    cidade_base,
                    ativo,
                    int(id_fornecedor),
                    cod_empresa
                ))
            else:
                cur.execute("""
                    INSERT INTO fornecedores_combustiveis (
                        cod_empresa,
                        nome_fornecedor,
                        cidade_base,
                        ativo,
                        criado_em,
                        atualizado_em
                    )
                    VALUES (%s, %s, %s, %s, NOW(), NOW())
                """, (
                    cod_empresa,
                    nome_fornecedor,
                    cidade_base,
                    ativo
                ))

            conn.commit()
            flash("Fornecedor salvo com sucesso.", "success")
            return redirect(url_for("operacoes.fornecedores_combustiveis"))

        cur.execute("""
            SELECT
                id_fornecedor,
                nome_fornecedor,
                cidade_base,
                ativo
            FROM fornecedores_combustiveis
            WHERE cod_empresa = %s
            ORDER BY nome_fornecedor
        """, (cod_empresa,))

        fornecedores = cur.fetchall() or []

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao processar fornecedores: {e}", "error")
        fornecedores = []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "fornecedores_combustiveis.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        fornecedores=fornecedores,
        url_voltar=url_for("operacoes.menu_configuracoes"),
        texto_voltar="← Voltar",
    )


@operacoes_bp.route("/configuracoes/fornecedores-combustiveis/excluir/<int:id_fornecedor>", methods=["POST"])
@permissao_obrigatoria(
    "OPERACOES",
    "CONFIGURACOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def excluir_fornecedor_combustivel(id_fornecedor):
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            DELETE FROM fornecedores_combustiveis
            WHERE id_fornecedor = %s
              AND cod_empresa = %s
        """, (id_fornecedor, cod_empresa))

        conn.commit()
        flash("Fornecedor excluído com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Não foi possível excluir. Use inativar se ele já estiver vinculado a compras. Erro: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("operacoes.fornecedores_combustiveis"))

# ---------------------------------------
# INFORMAR COMPRAS DE COMBUSTÍVEIS
# ---------------------------------------

@operacoes_bp.route("/compras-combustiveis/informar", methods=["GET", "POST"])
@permissao_obrigatoria(
    "OPERACOES",
    "INFORMAR_MEDICOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def informar_compras_combustiveis():

    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    data_sel = (request.values.get("data") or "").strip()
    if not data_sel:
        data_sel = (date.today() - timedelta(days=1)).isoformat()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    resumo_compras = []
    total_geral_qtd = 0
    total_geral_valor = 0
    preco_medio_geral = 0

    try:
        if tipo_global == "superusuario":
            cur.execute("""
                SELECT cod_filial, nome_filial
                FROM filiais
                WHERE cod_empresa = %s
                  AND ativo = TRUE
                ORDER BY cod_filial
            """, (cod_empresa,))
            filiais = cur.fetchall() or []
        else:
            filiais_permitidas = [
                int(x) for x in usuario_filiais_ativas(id_usuario, cod_empresa)
            ]

            if not filiais_permitidas:
                flash("Você não possui filiais habilitadas.", "error")
                return redirect(url_for("operacoes.menu_operacoes"))

            cur.execute("""
                SELECT cod_filial, nome_filial
                FROM filiais
                WHERE cod_empresa = %s
                  AND ativo = TRUE
                  AND cod_filial = ANY(%s)
                ORDER BY cod_filial
            """, (cod_empresa, filiais_permitidas))
            filiais = cur.fetchall() or []

        codigos_filiais = [int(f["cod_filial"]) for f in filiais]

        cur.execute("""
            SELECT cod_produto, descricao
            FROM combustiveis
            WHERE cod_empresa = %s
            ORDER BY cod_produto
        """, (cod_empresa,))
        produtos = cur.fetchall() or []

        cur.execute("""
            SELECT id_fornecedor, nome_fornecedor, cidade_base
            FROM fornecedores_combustiveis
            WHERE cod_empresa = %s
              AND ativo = TRUE
            ORDER BY nome_fornecedor
        """, (cod_empresa,))
        fornecedores = cur.fetchall() or []

        if request.method == "POST":
            id_compra_txt = (request.form.get("id_compra") or "").strip()
            cod_filial = int(request.form.get("cod_filial") or 0)
            cod_produto = (request.form.get("cod_produto") or "").strip()
            id_fornecedor = int(request.form.get("id_fornecedor") or 0)

            quantidade_comprada = parse_valor_br(request.form.get("quantidade_comprada"))
            preco_unitario = parse_valor_br(request.form.get("preco_unitario"))
            valor_comprado = quantidade_comprada * preco_unitario

            if cod_filial not in codigos_filiais:
                flash("Filial não permitida para este usuário.", "error")
                return redirect(url_for("operacoes.informar_compras_combustiveis", data=data_sel))

            if not cod_produto or not id_fornecedor:
                flash("Informe produto e fornecedor.", "error")
                return redirect(url_for("operacoes.informar_compras_combustiveis", data=data_sel))

            if id_compra_txt and id_compra_txt.isdigit():
                id_compra = int(id_compra_txt)

                cur.execute("""
                    UPDATE compras_combustiveis
                    SET cod_filial = %s,
                        cod_produto = %s,
                        id_fornecedor = %s,
                        quantidade_comprada = %s,
                        preco_unitario = %s,
                        valor_comprado = %s,
                        atualizado_em = NOW()
                    WHERE id_compra = %s
                      AND cod_empresa = %s
                """, (
                    cod_filial,
                    cod_produto,
                    id_fornecedor,
                    quantidade_comprada,
                    preco_unitario,
                    valor_comprado,
                    id_compra,
                    cod_empresa
                ))

            else:
                cur.execute("""
                    INSERT INTO compras_combustiveis (
                        cod_empresa,
                        data_compra,
                        cod_filial,
                        cod_produto,
                        id_fornecedor,
                        quantidade_comprada,
                        preco_unitario,
                        valor_comprado,
                        status,
                        criado_em,
                        atualizado_em
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'ABERTA', NOW(), NOW())
                    ON CONFLICT (cod_empresa, data_compra, cod_filial, cod_produto, id_fornecedor)
                    DO UPDATE SET
                        quantidade_comprada = EXCLUDED.quantidade_comprada,
                        preco_unitario = EXCLUDED.preco_unitario,
                        valor_comprado = EXCLUDED.valor_comprado,
                        atualizado_em = NOW()
                """, (
                    cod_empresa,
                    data_sel,
                    cod_filial,
                    cod_produto,
                    id_fornecedor,
                    quantidade_comprada,
                    preco_unitario,
                    valor_comprado
                ))

            conn.commit()
            flash("Compra salva com sucesso.", "success")
            return redirect(url_for("operacoes.informar_compras_combustiveis", data=data_sel))

        cur.execute("""
            SELECT
                cc.id_compra,
                cc.data_compra,
                cc.cod_filial,
                f.nome_filial,
                cc.cod_produto,
                c.descricao AS produto,
                cc.quantidade_comprada,
                cc.preco_unitario,
                cc.valor_comprado,
                cc.id_fornecedor,
                fc.nome_fornecedor,
                fc.cidade_base,
                COALESCE(cc.status, 'ABERTA') AS status,
                COALESCE(SUM(d.quantidade_descarregada), 0) AS total_descarregado
            FROM compras_combustiveis cc
            LEFT JOIN filiais f
              ON f.cod_empresa = cc.cod_empresa
             AND f.cod_filial = cc.cod_filial
            LEFT JOIN combustiveis c
              ON c.cod_empresa = cc.cod_empresa
             AND c.cod_produto = cc.cod_produto
            LEFT JOIN fornecedores_combustiveis fc
              ON fc.id_fornecedor = cc.id_fornecedor
            LEFT JOIN descarregos_combustiveis d
              ON d.cod_empresa = cc.cod_empresa
             AND d.id_compra = cc.id_compra
            WHERE cc.cod_empresa = %s
              AND cc.data_compra = %s
            GROUP BY
                cc.id_compra,
                cc.data_compra,
                cc.cod_filial,
                f.nome_filial,
                cc.cod_produto,
                c.descricao,
                cc.quantidade_comprada,
                cc.preco_unitario,
                cc.valor_comprado,
                cc.id_fornecedor,
                fc.nome_fornecedor,
                fc.cidade_base,
                cc.status
            ORDER BY cc.cod_filial, cc.cod_produto
        """, (cod_empresa, data_sel))

        compras = cur.fetchall() or []

        resumo_produtos = {}

        for c in compras:
            cod_produto = str(c["cod_produto"] or "").strip()
            produto = c["produto"] or cod_produto

            qtd = float(c["quantidade_comprada"] or 0)
            valor = float(c["valor_comprado"] or 0)

            if cod_produto not in resumo_produtos:
                resumo_produtos[cod_produto] = {
                    "cod_produto": cod_produto,
                    "produto": produto,
                    "quantidade": 0,
                    "valor": 0,
                    "preco_medio": 0,
                }

            resumo_produtos[cod_produto]["quantidade"] += qtd
            resumo_produtos[cod_produto]["valor"] += valor

            total_geral_qtd += qtd
            total_geral_valor += valor

        for r in resumo_produtos.values():
            if r["quantidade"] > 0:
                r["preco_medio"] = r["valor"] / r["quantidade"]

        resumo_compras = list(resumo_produtos.values())

        if total_geral_qtd > 0:
            preco_medio_geral = total_geral_valor / total_geral_qtd

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao processar compras: {e}", "error")
        compras = []
        filiais = []
        produtos = []
        fornecedores = []
        resumo_compras = []
        total_geral_qtd = 0
        total_geral_valor = 0
        preco_medio_geral = 0

    finally:
        cur.close()
        conn.close()

    return render_template(
        "informar_compras_combustiveis.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        data_sel=data_sel,
        filiais=filiais,
        produtos=produtos,
        fornecedores=fornecedores,
        compras=compras,
        resumo_compras=resumo_compras,
        total_geral_qtd=total_geral_qtd,
        total_geral_valor=total_geral_valor,
        preco_medio_geral=preco_medio_geral,
        url_voltar=url_for("operacoes.menu_operacoes"),
        texto_voltar="← Voltar",
    )
# ---------------------------------------
# EXCLUIR COMPRA DE COMBUSTÍVEL
# ---------------------------------------

@operacoes_bp.route("/compras-combustiveis/excluir/<int:id_compra>", methods=["POST"])
@permissao_obrigatoria(
    "OPERACOES",
    "INFORMAR_MEDICOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def excluir_compra_combustivel(id_compra):
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    data_sel = (request.form.get("data") or date.today().isoformat()).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            DELETE FROM compras_combustiveis
            WHERE id_compra = %s
              AND cod_empresa = %s
        """, (id_compra, cod_empresa))

        conn.commit()
        flash("Compra excluída com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao excluir compra: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("operacoes.informar_compras_combustiveis", data=data_sel))


# ---------------------------------------
# INFORMAR DESCARREGOS DE COMBUSTÍVEIS
# ---------------------------------------

@operacoes_bp.route("/descarregos-combustiveis/informar", methods=["GET", "POST"])
@permissao_obrigatoria(
    "OPERACOES",
    "INFORMAR_MEDICOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def informar_descarregos_combustiveis():

    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    data_sel = (request.values.get("data") or "").strip()
    if not data_sel:
        data_sel = (date.today() - timedelta(days=1)).isoformat()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # -----------------------------------
        # FILIAIS
        # -----------------------------------
        if tipo_global == "superusuario":
            cur.execute("""
                SELECT cod_filial, nome_filial
                FROM filiais
                WHERE cod_empresa = %s AND ativo = TRUE
                ORDER BY cod_filial
            """, (cod_empresa,))
            filiais = cur.fetchall() or []
        else:
            filiais_permitidas = [int(x) for x in usuario_filiais_ativas(id_usuario, cod_empresa)]

            if not filiais_permitidas:
                flash("Você não possui filiais habilitadas.", "error")
                return redirect(url_for("operacoes.menu_operacoes"))

            cur.execute("""
                SELECT cod_filial, nome_filial
                FROM filiais
                WHERE cod_empresa = %s
                  AND ativo = TRUE
                  AND cod_filial = ANY(%s)
                ORDER BY cod_filial
            """, (cod_empresa, filiais_permitidas))
            filiais = cur.fetchall() or []

        codigos_filiais = [int(f["cod_filial"]) for f in filiais]

        # -----------------------------------
        # COMPRAS COM SALDO
        # -----------------------------------
        cur.execute("""
            SELECT
                cc.id_compra,
                cc.data_compra,
                cc.cod_filial,
                f.nome_filial,
                cc.cod_produto,
                c.descricao AS produto,
                cc.quantidade_comprada,
                COALESCE(SUM(d.quantidade_descarregada), 0) AS total_descarregado,
                (cc.quantidade_comprada - COALESCE(SUM(d.quantidade_descarregada), 0)) AS saldo
            FROM compras_combustiveis cc
            LEFT JOIN filiais f ON f.cod_empresa = cc.cod_empresa AND f.cod_filial = cc.cod_filial
            LEFT JOIN combustiveis c ON c.cod_empresa = cc.cod_empresa AND c.cod_produto = cc.cod_produto
            LEFT JOIN descarregos_combustiveis d ON d.id_compra = cc.id_compra

            WHERE cc.cod_empresa = %s
              AND cc.cod_filial = ANY(%s)
              AND cc.status = 'ABERTA'

            GROUP BY
                cc.id_compra, cc.data_compra, cc.cod_filial,
                f.nome_filial, cc.cod_produto, c.descricao,
                cc.quantidade_comprada

            HAVING (cc.quantidade_comprada - COALESCE(SUM(d.quantidade_descarregada), 0)) > 0

            ORDER BY cc.data_compra DESC
        """, (cod_empresa, codigos_filiais))

        compras = cur.fetchall() or []

        # -----------------------------------
        # POST (SALVAR DESCARREGO)
        # -----------------------------------
        if request.method == "POST":

            id_compra_txt = (request.form.get("id_compra") or "").strip()
            cod_filial_descarga_txt = (request.form.get("cod_filial_descarga") or "").strip()
            quantidade_txt = (request.form.get("quantidade_descarregada") or "0").strip()

            if not id_compra_txt.isdigit():
                flash("Selecione uma compra válida.", "error")
                return redirect(url_for("operacoes.informar_descarregos_combustiveis", data=data_sel))

            if not cod_filial_descarga_txt.isdigit():
                flash("Selecione a filial de descarga.", "error")
                return redirect(url_for("operacoes.informar_descarregos_combustiveis", data=data_sel))

            id_compra = int(id_compra_txt)
            cod_filial_descarga = int(cod_filial_descarga_txt)
            quantidade_descarregada = parse_valor_br(quantidade_txt)

            if quantidade_descarregada <= 0:
                flash("Informe uma quantidade maior que zero.", "error")
                return redirect(url_for("operacoes.informar_descarregos_combustiveis", data=data_sel))

            if cod_filial_descarga not in codigos_filiais:
                flash("Filial de descarga não permitida.", "error")
                return redirect(url_for("operacoes.informar_descarregos_combustiveis", data=data_sel))

            # Buscar dados da compra
            cur.execute("""
                SELECT
                    id_compra,
                    cod_produto,
                    quantidade_comprada
                FROM compras_combustiveis
                WHERE id_compra = %s
                  AND cod_empresa = %s
            """, (id_compra, cod_empresa))

            compra = cur.fetchone()

            if not compra:
                flash("Compra não encontrada.", "error")
                return redirect(url_for("operacoes.informar_descarregos_combustiveis", data=data_sel))

            cod_produto = str(compra["cod_produto"]).strip()

            # INSERT REAL NA TABELA DE DESCARREGOS
            cur.execute("""
                INSERT INTO descarregos_combustiveis (
                    cod_empresa,
                    data_descarrego,
                    id_compra,
                    cod_filial_descarga,
                    cod_produto,
                    quantidade_descarregada,
                    criado_em,
                    atualizado_em
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING id_descarrego
            """, (
                cod_empresa,
                data_sel,
                id_compra,
                cod_filial_descarga,
                cod_produto,
                quantidade_descarregada
            ))

            novo = cur.fetchone()
            id_descarrego_gerado = novo["id_descarrego"]

            # Recalcular saldo da compra
            cur.execute("""
                SELECT
                    cc.quantidade_comprada,
                    COALESCE(SUM(d.quantidade_descarregada), 0) AS total_descarregado
                FROM compras_combustiveis cc
                LEFT JOIN descarregos_combustiveis d
                  ON d.id_compra = cc.id_compra
                 AND d.cod_empresa = cc.cod_empresa
                WHERE cc.id_compra = %s
                  AND cc.cod_empresa = %s
                GROUP BY cc.quantidade_comprada
            """, (id_compra, cod_empresa))

            row = cur.fetchone()

            if row:
                saldo_final = float(row["quantidade_comprada"] or 0) - float(row["total_descarregado"] or 0)

                if saldo_final <= 0:
                    cur.execute("""
                        UPDATE compras_combustiveis
                        SET status = 'FECHADA',
                            atualizado_em = NOW()
                        WHERE id_compra = %s
                          AND cod_empresa = %s
                    """, (id_compra, cod_empresa))

            conn.commit()

            flash(f"Descarrego {id_descarrego_gerado} salvo com sucesso.", "success")
            return redirect(url_for("operacoes.informar_descarregos_combustiveis", data=data_sel))

        # -----------------------------------
        # LISTA DESCARREGOS
        # -----------------------------------
        cur.execute("""
            SELECT
                d.id_descarrego,
                d.data_descarrego,
                d.id_compra,
                d.cod_filial_descarga,
                fd.nome_filial AS nome_filial_descarga,
                d.cod_produto,
                c.descricao AS produto,
                d.quantidade_descarregada,

                cc.data_compra,
                cc.cod_filial AS cod_filial_compra,
                fc.nome_filial AS nome_filial_compra,

                fo.nome_fornecedor,
                fo.cidade_base

            FROM descarregos_combustiveis d

            LEFT JOIN compras_combustiveis cc
              ON cc.cod_empresa = d.cod_empresa
             AND cc.id_compra = d.id_compra

            LEFT JOIN filiais fd
              ON fd.cod_empresa = d.cod_empresa
             AND fd.cod_filial = d.cod_filial_descarga

            LEFT JOIN filiais fc
              ON fc.cod_empresa = cc.cod_empresa
             AND fc.cod_filial = cc.cod_filial

            LEFT JOIN combustiveis c
              ON c.cod_empresa = d.cod_empresa
             AND c.cod_produto = d.cod_produto

            LEFT JOIN fornecedores_combustiveis fo
              ON fo.cod_empresa = cc.cod_empresa
             AND fo.id_fornecedor = cc.id_fornecedor

            WHERE d.cod_empresa = %s
              AND d.data_descarrego = %s

            ORDER BY d.id_descarrego
        """, (cod_empresa, data_sel))

        descarregos = cur.fetchall() or []

    except Exception as e:
        conn.rollback()
        flash(f"Erro: {e}", "error")
        filiais = []
        compras = []
        descarregos = []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "informar_descarregos_combustiveis.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        data_sel=data_sel,
        filiais=filiais,
        compras=compras,
        descarregos=descarregos,
        url_voltar=url_for("operacoes.menu_operacoes"),
        texto_voltar="← Voltar",
    )


# ---------------------------------------
# EXCLUIR DESCARREGO DE COMBUSTÍVEL
# ---------------------------------------

@operacoes_bp.route("/descarregos-combustiveis/excluir/<int:id_descarrego>", methods=["POST"])
@permissao_obrigatoria(
    "OPERACOES",
    "INFORMAR_MEDICOES",
    redirecionar_para="operacoes.menu_operacoes",
)

def excluir_descarrego_combustivel(id_descarrego):
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    data_sel = (request.form.get("data") or date.today().isoformat()).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT id_compra
            FROM descarregos_combustiveis
            WHERE id_descarrego = %s
              AND cod_empresa = %s
        """, (id_descarrego, cod_empresa))

        row = cur.fetchone()

        if not row:
            flash("Descarrego não encontrado.", "error")
            return redirect(url_for("operacoes.informar_descarregos_combustiveis", data=data_sel))

        id_compra = row["id_compra"]

        cur.execute("""
            DELETE FROM descarregos_combustiveis
            WHERE id_descarrego = %s
              AND cod_empresa = %s
        """, (id_descarrego, cod_empresa))

        cur.execute("""
            UPDATE compras_combustiveis
            SET status = 'ABERTA',
                atualizado_em = NOW()
            WHERE id_compra = %s
              AND cod_empresa = %s
        """, (id_compra, cod_empresa))

        conn.commit()
        flash("Descarrego excluído e compra reaberta com sucesso.", "success")

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao excluir descarrego: {e}", "error")

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("operacoes.informar_descarregos_combustiveis", data=data_sel))

# ---------------------------------------
# CONSULTAR ESTOQUES
# ---------------------------------------

@operacoes_bp.route("/estoques/consultar")
@permissao_obrigatoria(
    "OPERACOES",
    "CONSULTAR_MEDICOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def consultar_estoques():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    data_sel = (request.args.get("data") or "").strip()

    if not data_sel:
        data_sel = (date.today() - timedelta(days=1)).isoformat()

    data_base = date.fromisoformat(data_sel)
    data_anterior = data_base - timedelta(days=1)

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if tipo_global == "superusuario":
            filtro_filiais_sql = ""
            params_filiais = []
        else:
            filiais_permitidas = [
                int(x) for x in usuario_filiais_ativas(id_usuario, cod_empresa)
            ]

            if not filiais_permitidas:
                flash("Você não possui filiais habilitadas.", "error")
                return redirect(url_for("operacoes.menu_operacoes"))

            filtro_filiais_sql = "AND f.cod_filial = ANY(%s)"
            params_filiais = [filiais_permitidas]

        sql = f"""
            WITH base AS (
                SELECT
                    f.cod_filial,
                    f.nome_filial,
                    c.cod_produto,
                    c.descricao AS produto,
                    COALESCE(ct.capacidade_tanque, 0) AS capacidade_tanque
                FROM filiais f
                JOIN capacidade_tanques ct
                  ON ct.cod_empresa = f.cod_empresa
                 AND ct.cod_filial = f.cod_filial
                JOIN combustiveis c
                  ON c.cod_empresa = ct.cod_empresa
                 AND c.cod_produto = ct.cod_produto
                WHERE f.cod_empresa = %s
                  AND f.ativo = TRUE
                  AND COALESCE(ct.capacidade_tanque, 0) > 0
                  {filtro_filiais_sql}
            ),

            medicao_anterior AS (
                SELECT
                    cod_filial,
                    cod_produto,
                    SUM(COALESCE(quantidade_medida, 0)) AS medicao_anterior
                FROM medicoes
                WHERE cod_empresa = %s
                  AND data_medicao = %s
                GROUP BY cod_filial, cod_produto
            ),

            vendas AS (
                SELECT
                    cod_filial,
                    cod_produto,
                    SUM(COALESCE(quantidade_vendida, 0)) AS vendas
                FROM medicoes
                WHERE cod_empresa = %s
                  AND data_medicao = %s
                GROUP BY cod_filial, cod_produto
            ),



            compras_dia AS (
                SELECT
                    cod_filial,
                    cod_produto,
                    SUM(COALESCE(quantidade_comprada, 0)) AS compras
                FROM compras_combustiveis
                WHERE cod_empresa = %s
                  AND data_compra = %s
                GROUP BY cod_filial, cod_produto
            ),

            transito AS (
                SELECT
                    cc.cod_filial,
                    cc.cod_produto,
                    SUM(
                        COALESCE(cc.quantidade_comprada, 0)
                        - COALESCE(d.total_descarregado, 0)
                    ) AS estoque_transito
                FROM compras_combustiveis cc
                LEFT JOIN (
                    SELECT
                        id_compra,
                        cod_empresa,
                        SUM(COALESCE(quantidade_descarregada, 0)) AS total_descarregado
                    FROM descarregos_combustiveis
                    WHERE cod_empresa = %s
                    GROUP BY id_compra, cod_empresa
                ) d
                  ON d.cod_empresa = cc.cod_empresa
                 AND d.id_compra = cc.id_compra
                WHERE cc.cod_empresa = %s
                  AND cc.data_compra < %s
                  AND COALESCE(cc.status, 'ABERTA') = 'ABERTA'
                GROUP BY cc.cod_filial, cc.cod_produto
            ),

            descarregos AS (
                SELECT
                    cod_filial_descarga AS cod_filial,
                    cod_produto,
                    SUM(COALESCE(quantidade_descarregada, 0)) AS descarregos
                FROM descarregos_combustiveis
                WHERE cod_empresa = %s
                  AND data_descarrego = %s
                GROUP BY cod_filial_descarga, cod_produto
            ),

            emprestimos AS (
                SELECT
                    cc.cod_filial,
                    d.cod_produto,
                    SUM(COALESCE(d.quantidade_descarregada, 0)) * -1 AS emprestimos
                FROM descarregos_combustiveis d
                JOIN compras_combustiveis cc
                  ON cc.cod_empresa = d.cod_empresa
                 AND cc.id_compra = d.id_compra
                WHERE d.cod_empresa = %s
                  AND d.data_descarrego = %s
                  AND cc.cod_filial <> d.cod_filial_descarga
                GROUP BY cc.cod_filial, d.cod_produto
            ),

            medicao_atual AS (
                SELECT
                    cod_filial,
                    cod_produto,
                    SUM(COALESCE(quantidade_medida, 0)) AS medicao_atual
                FROM medicoes
                WHERE cod_empresa = %s
                  AND data_medicao = %s
                GROUP BY cod_filial, cod_produto
            )

            SELECT
                b.cod_filial,
                b.nome_filial,
                b.cod_produto,
                b.produto,
                b.capacidade_tanque,

                COALESCE(ma.medicao_anterior, 0) AS medicao_anterior,
                COALESCE(v.vendas, 0) AS vendas,
                COALESCE(cd.compras, 0) AS compras,
                COALESCE(t.estoque_transito, 0) AS estoque_transito,
                COALESCE(ds.descarregos, 0) AS descarregos,
                COALESCE(e.emprestimos, 0) AS emprestimos,

                (
                    COALESCE(ma.medicao_anterior, 0)
                    - COALESCE(v.vendas, 0)
                    + COALESCE(cd.compras, 0)
                    + COALESCE(t.estoque_transito, 0)
                    + COALESCE(ds.descarregos, 0)
                    + COALESCE(e.emprestimos, 0)
                ) AS estoque_calculado,

                COALESCE(mat.medicao_atual, 0) AS medicao_atual

            FROM base b
            LEFT JOIN medicao_anterior ma
              ON ma.cod_filial = b.cod_filial
             AND ma.cod_produto = b.cod_produto
            LEFT JOIN vendas v
              ON v.cod_filial = b.cod_filial
             AND v.cod_produto = b.cod_produto
            LEFT JOIN compras_dia cd
              ON cd.cod_filial = b.cod_filial
             AND cd.cod_produto = b.cod_produto
            LEFT JOIN transito t
              ON t.cod_filial = b.cod_filial
             AND t.cod_produto = b.cod_produto
            LEFT JOIN descarregos ds
              ON ds.cod_filial = b.cod_filial
             AND ds.cod_produto = b.cod_produto
            LEFT JOIN emprestimos e
              ON e.cod_filial = b.cod_filial
             AND e.cod_produto = b.cod_produto
            LEFT JOIN medicao_atual mat
              ON mat.cod_filial = b.cod_filial
             AND mat.cod_produto = b.cod_produto

            ORDER BY b.cod_filial, b.cod_produto
        """

        params = (
            [cod_empresa]
            + params_filiais
            + [
                cod_empresa, data_anterior,
                cod_empresa, data_anterior,
                cod_empresa, data_sel,
                cod_empresa,
                cod_empresa, data_sel,
                cod_empresa, data_sel,
                cod_empresa, data_sel,
                cod_empresa, data_sel,
            ]
        )

        cur.execute(sql, params)
        linhas = cur.fetchall() or []

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao consultar estoques: {e}", "error")
        linhas = []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "consultar_estoques.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        data_sel=data_sel,
        data_anterior=data_anterior,
        linhas=linhas,
        url_voltar=url_for("operacoes.menu_operacoes"),
        texto_voltar="← Voltar",
    )
# ---------------------------------------
# CONSULTAR VENDAS
# ---------------------------------------

@operacoes_bp.route("/vendas/consultar")
@permissao_obrigatoria(
    "OPERACOES",
    "CONSULTAR_MEDICOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def consultar_vendas():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    nome_empresa = session.get("nome_empresa", "")
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    data_sel = (request.args.get("data") or "").strip()
    if not data_sel:
        data_sel = (date.today() - timedelta(days=1)).isoformat()
    data_medicao = (date.fromisoformat(data_sel) + timedelta(days=1)).isoformat()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if tipo_global == "superusuario":
            filtro_filiais_sql = ""
            params_filiais = []
        else:
            filiais_permitidas = [int(x) for x in usuario_filiais_ativas(id_usuario, cod_empresa)]

            if not filiais_permitidas:
                flash("Você não possui filiais habilitadas.", "error")
                return redirect(url_for("operacoes.menu_operacoes"))

            filtro_filiais_sql = "AND f.cod_filial = ANY(%s)"
            params_filiais = [filiais_permitidas]

        sql = f"""
            WITH base AS (
                SELECT
                    f.cod_filial,
                    f.nome_filial,
                    c.cod_produto,
                    c.descricao AS produto
                FROM filiais f
                JOIN capacidade_tanques ct
                  ON ct.cod_empresa = f.cod_empresa
                 AND ct.cod_filial = f.cod_filial
                JOIN combustiveis c
                  ON c.cod_empresa = ct.cod_empresa
                 AND c.cod_produto = ct.cod_produto
                WHERE f.cod_empresa = %s
                  AND f.ativo = TRUE
                  AND COALESCE(ct.capacidade_tanque, 0) > 0
                  {filtro_filiais_sql}
            ),
            vendas AS (
                SELECT
                    cod_filial,
                    cod_produto,
                    SUM(COALESCE(quantidade_vendida, 0)) AS quantidade_vendida
                FROM medicoes
                WHERE cod_empresa = %s
                  AND data_medicao = %s
                GROUP BY cod_filial, cod_produto
            )
            SELECT
                b.cod_filial,
                b.nome_filial,
                b.cod_produto,
                b.produto,
                COALESCE(v.quantidade_vendida, 0) AS quantidade_vendida,
                0::numeric AS quantidade_vendida_sistema
            FROM base b
            LEFT JOIN vendas v
              ON v.cod_filial = b.cod_filial
             AND v.cod_produto = b.cod_produto
            ORDER BY b.cod_filial, b.cod_produto
        """

        params = [cod_empresa] + params_filiais + [cod_empresa, data_medicao]

        cur.execute(sql, params)
        linhas = cur.fetchall() or []

        # Busca vendas do sistema separadamente, sem quebrar a tela.
        try:
            cur.execute("""
                SELECT
                    cod_filial,

                    CASE
                        WHEN codigo_produto::text IN ('1', '9', '543') THEN 'C1'
                        WHEN codigo_produto::text = '2' THEN 'C2'
                        WHEN codigo_produto::text IN ('3', '7') THEN 'C3'
                        WHEN codigo_produto::text = '5' THEN 'C4'
                        WHEN codigo_produto::text IN ('4', '8', '10') THEN 'C5'
                        ELSE NULL
                    END AS cod_produto,

                    SUM(COALESCE(quantidade, 0)) AS quantidade_vendida_sistema

                FROM vendas_diarias
                WHERE cod_empresa = %s
                  AND data = %s
                GROUP BY
                    cod_filial,
                    CASE
                        WHEN codigo_produto::text IN ('1', '9', '543') THEN 'C1'
                        WHEN codigo_produto::text = '2' THEN 'C2'
                        WHEN codigo_produto::text IN ('3', '7') THEN 'C3'
                        WHEN codigo_produto::text = '5' THEN 'C4'
                        WHEN codigo_produto::text IN ('4', '8', '10') THEN 'C5'
                        ELSE NULL
                    END
            """, (cod_empresa, data_sel))

            vendas_sistema_rows = cur.fetchall() or []

            vendas_sistema = {}
            for r in vendas_sistema_rows:
                if not r["cod_produto"]:
                    continue

                chave = (
                    int(r["cod_filial"]),
                    str(r["cod_produto"]).strip()
                )
                vendas_sistema[chave] = r["quantidade_vendida_sistema"] or 0

            for l in linhas:
                chave = (
                    int(l["cod_filial"]),
                    str(l["cod_produto"]).strip()
                )
                l["quantidade_vendida_sistema"] = vendas_sistema.get(chave, 0)

        except Exception as e:
            print("Erro ao buscar vendas do sistema:", e)

            for l in linhas:
                l["quantidade_vendida_sistema"] = 0

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao consultar vendas: {e}", "error")
        linhas = []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "consultar_vendas.html",
        cod_empresa=cod_empresa,
        nome_empresa=nome_empresa,
        data_sel=data_sel,
        linhas=linhas,
        url_voltar=url_for("operacoes.menu_operacoes"),
        texto_voltar="← Voltar",
    )
# --------------------------------
# AJAX - SALVAR PRECOS
# --------------------------------
@operacoes_bp.route("/precos-compra/ajax-salvar", methods=["POST"])
@permissao_obrigatoria(
    "OPERACOES",
    "INFORMAR_PRECO_COMPRA",
    redirecionar_para="operacoes.menu_operacoes",
)
def ajax_salvar_preco_compra():
    if "id_usuario" not in session:
        return {"ok": False, "erro": "Sessão expirada"}, 401

    if "cod_empresa" not in session:
        return {"ok": False, "erro": "Empresa não selecionada"}, 400

    cod_empresa = str(session["cod_empresa"]).strip()

    data_preco = (request.form.get("data_preco") or "").strip()
    cod_produto = str(request.form.get("cod_produto") or "").strip()
    valor_txt = (request.form.get("valor") or "").strip()

    if not data_preco or not cod_produto:
        return {"ok": False, "erro": "Dados incompletos."}, 400

    valor_txt = valor_txt.replace(".", "").replace(",", ".")
    try:
        preco = float(valor_txt) if valor_txt else 0.0
    except ValueError:
        return {"ok": False, "erro": "Valor inválido."}, 400

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO precos_compra (
                cod_empresa,
                data_preco,
                cod_produto,
                preco_compra,
                criado_em,
                atualizado_em
            )
            VALUES (%s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (cod_empresa, data_preco, cod_produto)
            DO UPDATE SET
                preco_compra = EXCLUDED.preco_compra,
                atualizado_em = NOW()
        """, (cod_empresa, data_preco, cod_produto, preco))

        conn.commit()
        return {"ok": True}
    except Exception as e:
        conn.rollback()
        return {"ok": False, "erro": str(e)}, 500
    finally:
        cur.close()
        conn.close()

# --------------------------------
# AJAX - SALVAR MEDICOES
# --------------------------------

@operacoes_bp.route("/medicoes/ajax-salvar", methods=["POST"])
@permissao_obrigatoria(
    "OPERACOES",
    "INFORMAR_MEDICOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def ajax_salvar_medicao():
    if "id_usuario" not in session:
        return {"ok": False, "erro": "Sessão expirada"}, 401

    if "cod_empresa" not in session:
        return {"ok": False, "erro": "Empresa não selecionada"}, 400

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    cod_filial_txt = (request.form.get("cod_filial") or "").strip()
    if not cod_filial_txt.isdigit():
        return {"ok": False, "erro": "Selecione a filial."}, 400

    cod_filial = int(cod_filial_txt)
    cod_produto = str(request.form.get("cod_produto") or "").strip()
    campo = str(request.form.get("campo") or "").strip()
    valor_txt = (request.form.get("valor") or "").strip()

    if not cod_filial or not cod_produto or not campo:
        return {"ok": False, "erro": "Dados incompletos."}, 400

    campos_permitidos = {
        "quantidade_medida",
        "quantidade_descarregada",
        "quantidade_vendida",
    }

    if campo not in campos_permitidos:
        return {"ok": False, "erro": "Campo inválido."}, 400

    if tipo_global != "superusuario":
        filiais_permitidas = [int(x) for x in usuario_filiais_ativas(id_usuario, cod_empresa)]

        if cod_filial not in filiais_permitidas:
            return {"ok": False, "erro": "Filial não permitida para este usuário."}, 403

    valor_txt = valor_txt.replace(".", "").replace(",", ".")

    try:
        quantidade = float(valor_txt) if valor_txt else 0.0
    except ValueError:
        quantidade = 0.0

    hoje = date.today()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO medicoes (
                cod_empresa,
                cod_filial,
                data_medicao,
                cod_produto,
                quantidade_medida,
                quantidade_descarregada,
                quantidade_vendida,
                criado_em,
                atualizado_em
            )
            VALUES (
                %s, %s, %s, %s,
                CASE WHEN %s = 'quantidade_medida' THEN %s ELSE 0 END,
                CASE WHEN %s = 'quantidade_descarregada' THEN %s ELSE 0 END,
                CASE WHEN %s = 'quantidade_vendida' THEN %s ELSE 0 END,
                NOW(), NOW()
            )
            ON CONFLICT (cod_empresa, cod_filial, data_medicao, cod_produto)
            DO UPDATE SET
                quantidade_medida = CASE
                    WHEN %s = 'quantidade_medida' THEN %s
                    ELSE medicoes.quantidade_medida
                END,
                quantidade_descarregada = CASE
                    WHEN %s = 'quantidade_descarregada' THEN %s
                    ELSE medicoes.quantidade_descarregada
                END,
                quantidade_vendida = CASE
                    WHEN %s = 'quantidade_vendida' THEN %s
                    ELSE medicoes.quantidade_vendida
                END,
                atualizado_em = NOW()
            """,
            (
                cod_empresa, cod_filial, hoje, cod_produto,
                campo, quantidade,
                campo, quantidade,
                campo, quantidade,
                campo, quantidade,
                campo, quantidade,
                campo, quantidade,
            ),
        )

        conn.commit()
        return {"ok": True}

    except Exception as e:
        conn.rollback()
        return {"ok": False, "erro": str(e)}, 500

    finally:
        cur.close()
        conn.close()

# --------------------------------
# AJAX - BUSCAR PREÇO DE COMPRA
# --------------------------------
@operacoes_bp.route("/precos-compra/ajax-buscar", methods=["GET"])
@permissao_obrigatoria(
    "OPERACOES",
    "INFORMAR_MEDICOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def ajax_buscar_preco_compra():
    if "cod_empresa" not in session:
        return {"ok": False, "erro": "Empresa não selecionada"}, 400

    cod_empresa = str(session["cod_empresa"]).strip()
    data_preco = (request.args.get("data") or "").strip()
    cod_produto = (request.args.get("cod_produto") or "").strip()

    if not data_preco or not cod_produto:
        return {"ok": False, "preco": 0}, 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT preco_compra
            FROM precos_compra
            WHERE cod_empresa = %s
              AND data_preco = %s
              AND cod_produto = %s
            LIMIT 1
        """, (cod_empresa, data_preco, cod_produto))

        row = cur.fetchone()

        if not row:
            return {"ok": True, "preco": 0}

        return {"ok": True, "preco": float(row["preco_compra"] or 0)}

    finally:
        cur.close()
        conn.close()

# --------------------------------
# CAPACIDADE TANQUES
# --------------------------------       

@operacoes_bp.route("/configuracoes/capacidade-tanques/ajax-salvar", methods=["POST"])
@permissao_obrigatoria(
    "OPERACOES",
    "CONFIGURACOES",
    redirecionar_para="operacoes.menu_operacoes",
)
def ajax_salvar_capacidade():
    cod_empresa = str(session["cod_empresa"]).strip()

    try:
        cod_filial = int(request.form.get("cod_filial"))
        cod_produto = str(request.form.get("cod_produto")).strip()
        valor_txt = (request.form.get("capacidade") or "").strip()

        valor_txt = valor_txt.replace(".", "").replace(",", ".")
        capacidade = float(valor_txt) if valor_txt else 0.0

    except:
        return {"ok": False, "erro": "Dados inválidos"}, 400

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO capacidade_tanques (
                cod_empresa,
                cod_filial,
                cod_produto,
                capacidade_tanque,
                criado_em,
                atualizado_em
            )
            VALUES (%s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (cod_empresa, cod_filial, cod_produto)
            DO UPDATE SET
                capacidade_tanque = EXCLUDED.capacidade_tanque,
                atualizado_em = NOW()
        """, (cod_empresa, cod_filial, cod_produto, capacidade))

        conn.commit()
        return {"ok": True}

    except Exception as e:
        conn.rollback()
        return {"ok": False, "erro": str(e)}, 500

    finally:
        cur.close()
        conn.close()