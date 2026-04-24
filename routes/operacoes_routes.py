from flask import Blueprint, render_template, session, redirect, url_for, flash, request
from psycopg2.extras import RealDictCursor
from datetime import datetime, date

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
        pode_informar = True
        pode_consultar = True
        pode_configuracoes = True
        pode_informar_preco = True
        pode_consultar_preco = True
    else:
        pode_informar = usuario_tem_permissao(
            id_usuario, cod_empresa, "OPERACOES", "INFORMAR_MEDICOES"
        )
        pode_consultar = usuario_tem_permissao(
            id_usuario, cod_empresa, "OPERACOES", "CONSULTAR_MEDICOES"
        )
        pode_configuracoes = usuario_tem_permissao(
            id_usuario, cod_empresa, "OPERACOES", "CONFIGURACOES"
        )
        pode_informar_preco = usuario_tem_permissao(
            id_usuario, cod_empresa, "OPERACOES", "INFORMAR_PRECO_COMPRA"
        )
        pode_consultar_preco = usuario_tem_permissao(
            id_usuario, cod_empresa, "OPERACOES", "CONSULTAR_PRECO_COMPRA"
        )

    return render_template(
        "menu_operacoes.html",
        cod_empresa=session.get("cod_empresa", ""),
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("sistema.selecionar_sistema"),
        texto_voltar="← Voltar",
        pode_informar=pode_informar,
        pode_consultar=pode_consultar,
        pode_configuracoes=pode_configuracoes,
        pode_informar_preco=pode_informar_preco,
        pode_consultar_preco=pode_consultar_preco,
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

        filiais_sem_medicao = []

        if filial_sel is None:
            filiais_com_medicao = {
                int(l["cod_filial"])
                for l in linhas
                if l["cod_filial"] is not None
            }

            for f in filiais:
                cod_filial_f = int(f["cod_filial"])
                if cod_filial_f not in filiais_com_medicao:
                    filiais_sem_medicao.append({
                        "cod_filial": cod_filial_f,
                        "nome_filial": f["nome_filial"]
                    })

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