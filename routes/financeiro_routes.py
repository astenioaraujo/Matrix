from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify, flash
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict, OrderedDict
import math
import io
from psycopg2.extras import RealDictCursor, execute_batch
from db import get_connection
from security_helpers import usuario_tem_permissao, permissao_obrigatoria
from services.dashboard_service import montar_dashboard
from services.estoques_service import totais_estoque_rs
from services.bloqueios_service import datas_bloqueio_pendentes
from utils.formatters import formatar_numero_br, formatar_int

financeiro_bp = Blueprint("financeiro", __name__)

def eh_ajax():
    return request.headers.get("X-Requested-With") == "XMLHttpRequest"


def filial_para_dict(f):
    return {
        "cod_filial": f[0],
        "nome_filial": f[1],
        "nome_filial_importacao": f[2] or "",
        "ativo": bool(f[3]),
        "qtde_lancamentos": int(f[4] or 0),
    }

#-----------------------------------------------
# FUNÇÕES AUXILIARES
#-----------------------------------------------

def buscar_filiais_empresa(cur, cod_empresa):
    cur.execute("""
        SELECT
            f.cod_filial,
            f.nome_filial,
            f.nome_filial_importacao,
            f.ativo,
            COALESCE(l.qtde_lancamentos, 0) AS qtde_lancamentos
        FROM filiais f
        LEFT JOIN (
            SELECT
                cod_empresa,
                cod_filial,
                COUNT(*) AS qtde_lancamentos
            FROM lancamentos
            WHERE cod_empresa = %s
            GROUP BY cod_empresa, cod_filial
        ) l
            ON l.cod_empresa = f.cod_empresa
           AND l.cod_filial = f.cod_filial
        WHERE f.cod_empresa = %s
        ORDER BY f.cod_filial
    """, (cod_empresa, cod_empresa))
    return cur.fetchall()

def conta_para_ordenacao(valor):
    texto = str(valor or "").strip()
    try:
        return (0, int(texto))
    except Exception:
        return (1, texto.upper())

def obter_dados_matricial(cod_empresa, ano_sel="", mes_sel="", filial_sel=""):
    conn = get_connection()
    cur = conn.cursor()

    try:
        where = ["l.cod_empresa = %s"]
        params = [cod_empresa]

        if ano_sel:
            where.append("CAST(l.ano AS TEXT) = %s")
            params.append(str(ano_sel))

        if mes_sel:
            where.append("CAST(l.mes AS TEXT) = %s")
            params.append(str(mes_sel))

        if filial_sel:
            where.append("CAST(l.cod_filial AS TEXT) = %s")
            params.append(str(filial_sel))

        where_sql = " AND ".join(where)

        where_filiais = ["cod_empresa = %s", "ativo = TRUE"]
        params_filiais = [cod_empresa]

        if filial_sel:
            where_filiais.append("CAST(cod_filial AS TEXT) = %s")
            params_filiais.append(str(filial_sel))

        where_filiais_sql = " AND ".join(where_filiais)

        cur.execute(f"""
            SELECT
                cod_filial,
                nome_filial
            FROM filiais
            WHERE {where_filiais_sql}
            ORDER BY cod_filial
        """, params_filiais)
        filiais_colunas = cur.fetchall()

        cur.execute(f"""
            SELECT
                l.grupo,
                l.conta,
                COALESCE(NULLIF(TRIM(l.descricao_conta), ''), 'SEM DESCRIÇÃO') AS descricao_conta,
                l.cod_filial,
                COALESCE(SUM(l.valor), 0) AS total_valor
            FROM lancamentos l
            WHERE {where_sql}
              AND l.grupo IS NOT NULL
              AND l.conta IS NOT NULL
            GROUP BY
                l.grupo,
                l.conta,
                COALESCE(NULLIF(TRIM(l.descricao_conta), ''), 'SEM DESCRIÇÃO'),
                l.cod_filial
            ORDER BY
                l.grupo,
                l.conta,
                descricao_conta,
                l.cod_filial
        """, params)
        dados = cur.fetchall()

        cur.execute("""
            SELECT DISTINCT ano
            FROM lancamentos
            WHERE cod_empresa = %s
              AND ano IS NOT NULL
            ORDER BY ano
        """, (cod_empresa,))
        anos = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT mes
            FROM lancamentos
            WHERE cod_empresa = %s
              AND mes IS NOT NULL
            ORDER BY mes
        """, (cod_empresa,))
        meses = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = TRUE
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    filiais_ids = [f[0] for f in filiais_colunas]

    mapa = {}
    for grupo, conta, descricao_conta, cod_filial, total_valor in dados:
        chave = (grupo, conta, descricao_conta)

        if chave not in mapa:
            mapa[chave] = {
                "grupo": grupo,
                "conta": conta,
                "descricao_conta": descricao_conta,
                "por_filial": {fid: 0.0 for fid in filiais_ids},
                "total": 0.0
            }

        valor = float(total_valor or 0)
        mapa[chave]["por_filial"][cod_filial] = valor
        mapa[chave]["total"] += valor

    linhas_matriciais = []
    for item in mapa.values():
        valores = [item["por_filial"].get(fid, 0.0) for fid in filiais_ids]
        linhas_matriciais.append({
            "grupo": item["grupo"],
            "conta": item["conta"],
            "descricao_conta": item["descricao_conta"],
            "valores": valores,
            "total": item["total"]
        })

    linhas_matriciais.sort(
        key=lambda x: (
            int(x["grupo"]) if str(x["grupo"]).isdigit() else 999,
            conta_para_ordenacao(x["conta"]),
            str(x["descricao_conta"]).upper()
        )
    )

    grupos_tmp = {}
    for linha in linhas_matriciais:
        grupo = linha["grupo"]

        if grupo not in grupos_tmp:
            grupos_tmp[grupo] = {
                "grupo": grupo,
                "linhas": [],
                "totais_filiais": [0.0 for _ in filiais_ids],
                "total_geral": 0.0
            }

        grupos_tmp[grupo]["linhas"].append(linha)
        grupos_tmp[grupo]["total_geral"] += linha["total"]

        for i, valor in enumerate(linha["valores"]):
            grupos_tmp[grupo]["totais_filiais"][i] += valor

    grupos_ordenados = sorted(
        grupos_tmp.values(),
        key=lambda g: int(g["grupo"]) if str(g["grupo"]).isdigit() else 999
    )

    total_geral_filiais = [0.0 for _ in filiais_ids]
    total_geral = 0.0

    for grupo in grupos_ordenados:
        for i, valor in enumerate(grupo["totais_filiais"]):
            total_geral_filiais[i] += valor
        total_geral += grupo["total_geral"]

    return {
        "filiais_colunas": filiais_colunas,
        "linhas_matriciais": linhas_matriciais,
        "grupos_ordenados": grupos_ordenados,
        "total_geral_filiais": total_geral_filiais,
        "total_geral": total_geral,
        "anos": anos,
        "meses": meses,
        "filiais": filiais
    }


MESES_NOMES = [
    (1, "Jan"), (2, "Fev"), (3, "Mar"), (4, "Abr"),
    (5, "Mai"), (6, "Jun"), (7, "Jul"), (8, "Ago"),
    (9, "Set"), (10, "Out"), (11, "Nov"), (12, "Dez"),
]


def obter_dados_matricial_anual(cod_empresa, ano_sel="", filial_sel=""):
    """Mesma matriz da Consulta Matricial, mas as colunas sao os 12 meses do
    ano — de um posto ou de todos somados. Nada de subtotal persistido: tudo
    e somado aqui, a partir de `lancamentos`."""
    conn = get_connection()
    cur = conn.cursor()

    try:
        where = ["l.cod_empresa = %s"]
        params = [cod_empresa]

        if ano_sel:
            where.append("CAST(l.ano AS TEXT) = %s")
            params.append(str(ano_sel))

        if filial_sel:
            where.append("CAST(l.cod_filial AS TEXT) = %s")
            params.append(str(filial_sel))

        where_sql = " AND ".join(where)

        cur.execute(f"""
            SELECT
                l.grupo,
                l.conta,
                COALESCE(NULLIF(TRIM(l.descricao_conta), ''), 'SEM DESCRIÇÃO') AS descricao_conta,
                l.mes,
                COALESCE(SUM(l.valor), 0) AS total_valor
            FROM lancamentos l
            WHERE {where_sql}
              AND l.grupo IS NOT NULL
              AND l.conta IS NOT NULL
              AND l.mes IS NOT NULL
            GROUP BY
                l.grupo,
                l.conta,
                COALESCE(NULLIF(TRIM(l.descricao_conta), ''), 'SEM DESCRIÇÃO'),
                l.mes
        """, params)
        dados = cur.fetchall()

        cur.execute("""
            SELECT DISTINCT ano
            FROM lancamentos
            WHERE cod_empresa = %s
              AND ano IS NOT NULL
            ORDER BY ano
        """, (cod_empresa,))
        anos = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = TRUE
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    meses_colunas = list(MESES_NOMES)
    meses_ids = [m[0] for m in meses_colunas]

    mapa = {}
    for grupo, conta, descricao_conta, mes, total_valor in dados:
        try:
            mes_int = int(mes)
        except (TypeError, ValueError):
            continue

        if mes_int not in meses_ids:
            continue

        chave = (grupo, conta, descricao_conta)

        if chave not in mapa:
            mapa[chave] = {
                "grupo": grupo,
                "conta": conta,
                "descricao_conta": descricao_conta,
                "por_mes": {mid: 0.0 for mid in meses_ids},
                "total": 0.0
            }

        valor = float(total_valor or 0)
        mapa[chave]["por_mes"][mes_int] += valor
        mapa[chave]["total"] += valor

    linhas_matriciais = []
    for item in mapa.values():
        linhas_matriciais.append({
            "grupo": item["grupo"],
            "conta": item["conta"],
            "descricao_conta": item["descricao_conta"],
            "valores": [item["por_mes"].get(mid, 0.0) for mid in meses_ids],
            "total": item["total"]
        })

    linhas_matriciais.sort(
        key=lambda x: (
            int(x["grupo"]) if str(x["grupo"]).isdigit() else 999,
            conta_para_ordenacao(x["conta"]),
            str(x["descricao_conta"]).upper()
        )
    )

    grupos_tmp = {}
    for linha in linhas_matriciais:
        grupo = linha["grupo"]

        if grupo not in grupos_tmp:
            grupos_tmp[grupo] = {
                "grupo": grupo,
                "linhas": [],
                "totais_meses": [0.0 for _ in meses_ids],
                "total_geral": 0.0
            }

        grupos_tmp[grupo]["linhas"].append(linha)
        grupos_tmp[grupo]["total_geral"] += linha["total"]

        for i, valor in enumerate(linha["valores"]):
            grupos_tmp[grupo]["totais_meses"][i] += valor

    grupos_ordenados = sorted(
        grupos_tmp.values(),
        key=lambda g: int(g["grupo"]) if str(g["grupo"]).isdigit() else 999
    )

    total_geral_meses = [0.0 for _ in meses_ids]
    total_geral = 0.0

    for grupo in grupos_ordenados:
        for i, valor in enumerate(grupo["totais_meses"]):
            total_geral_meses[i] += valor
        total_geral += grupo["total_geral"]

    return {
        "meses_colunas": meses_colunas,
        "linhas_matriciais": linhas_matriciais,
        "grupos_ordenados": grupos_ordenados,
        "total_geral_meses": total_geral_meses,
        "total_geral": total_geral,
        "anos": anos,
        "filiais": filiais
    }


def validar_data_contrato(valor, nome_campo):
    if not valor:
        return None

    try:
        data = datetime.strptime(valor, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"{nome_campo} inválida.")

    if data.year < 2000:
        raise ValueError(f"{nome_campo} deve ter ano igual ou superior a 2000.")

    if data.year > 2100:
        raise ValueError(f"{nome_campo} deve ter ano até 2100.")

    return data

def converter_numero_br(valor):
    if valor is None:
        return 0

    valor = str(valor).strip()

    if valor == "":
        return 0

    valor = valor.replace(".", "").replace(",", ".")

    try:
        return float(valor)
    except ValueError:
        return 0

# =========================
# MENU FINANCEIRO
# =========================
@financeiro_bp.route("/menu")
def menu_empresa():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":

        pode_saldos = True
        pode_caixas = True
        pode_fluxo_caixa = True
        pode_cadastros = True
        pode_emprestimos_financiamentos = True
        pode_cr_fiado = True
        pode_credito = True

    else:

        # Caixas
        pode_caixas = usuario_tem_permissao(
            id_usuario, cod_empresa, "FINANCEIRO", "MENU_CAIXAS"
        )

        # Saldos
        pode_saldos = usuario_tem_permissao(
            id_usuario,
            cod_empresa,
            "FINANCEIRO",
            "MENU_SALDOS"
        )

        # Cadastros
        pode_cadastros = usuario_tem_permissao(
            id_usuario,
            cod_empresa,
            "FINANCEIRO",
            "CADASTROS"
        )

        # Fluxo de Caixa
        # Como todos os itens internos possuem permissões próprias,
        # basta ter acesso ao menu financeiro.
        pode_fluxo_caixa = usuario_tem_permissao(
            id_usuario,
            cod_empresa,
            "FINANCEIRO",
            "MENU_FLUXO_CAIXA"
        )

        # Empréstimos e Financiamentos
        pode_cadastro_emprestimos = usuario_tem_permissao(
            id_usuario,
            cod_empresa,
            "FINANCEIRO",
            "CADASTRO_EMPRESTIMOS_FINANCIAMENTOS"
        )

        pode_consulta_emprestimos = usuario_tem_permissao(
            id_usuario,
            cod_empresa,
            "FINANCEIRO",
            "CONSULTA_EMPRESTIMOS_FINANCIAMENTOS"
        )

        pode_emprestimos_financiamentos = (
            pode_cadastro_emprestimos
            or pode_consulta_emprestimos
        )

        pode_cr_fiado = (
            usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "CR_MENU") or
            usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "CR_FIADO_MENU") or
            usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "CR_CARTOES_MENU")
        )

        # Crédito abre com qualquer uma das duas pontas do workflow.
        pode_credito = (
            usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "CREDITO_CADASTRAR") or
            usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "CREDITO_APROVAR")
        )

    linhas, totais = montar_dashboard(cod_empresa)

    return render_template(
        "menu_financeiro.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        linhas_dashboard=linhas,
        totais_dashboard=totais,
        formatar_numero_br=formatar_numero_br,
        formatar_int=formatar_int,
        ano_atual=datetime.now().year,
        url_voltar=url_for("sistema.selecionar_sistema"),

        pode_caixas=pode_caixas,
        pode_saldos=pode_saldos,
        pode_fluxo_caixa=pode_fluxo_caixa,
        pode_cadastros=pode_cadastros,
        pode_emprestimos_financiamentos=pode_emprestimos_financiamentos,
        pode_cr_fiado=pode_cr_fiado,
        pode_credito=pode_credito,
    )
# =========================
# CADASTROS
# =========================
@financeiro_bp.route("/cadastros")
def menu_cadastros():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    empresa_ativa = session.get("cod_empresa")
    nome_empresa_ativa = session.get("nome_empresa")

    return render_template(
        "menu_cadastros.html",
        empresa_ativa=empresa_ativa,
        nome_empresa_ativa=nome_empresa_ativa,
        url_voltar=url_for("financeiro.menu_empresa")
    )

# =========================
# FILIAIS
# =========================
@financeiro_bp.route("/filiais", methods=["GET", "POST"])
def cadastrar_filiais():
    if "cod_empresa" not in session:
        if eh_ajax():
            return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
        return redirect(url_for("auth.index"))

    mensagem = ""
    erro = ""
    filial_edicao = None
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        if request.method == "POST":
            acao = (request.form.get("acao") or "").strip().lower()

            try:
                cod_filial_raw = (request.form.get("cod_filial") or "").strip()
                nome_filial = (request.form.get("nome_filial") or "").strip()
                nome_filial_importacao = (request.form.get("nome_filial_importacao") or "").strip()
                ativo = True if request.form.get("ativo") == "on" else False

                if acao == "novo":
                    if not nome_filial:
                        raise ValueError("Informe o nome da filial.")

                    cur.execute("""
                        SELECT COALESCE(MAX(cod_filial), 0) + 1
                        FROM filiais
                        WHERE cod_empresa = %s
                    """, (cod_empresa,))
                    proximo_codigo = cur.fetchone()[0]

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
                        proximo_codigo,
                        nome_filial,
                        nome_filial_importacao if nome_filial_importacao else None,
                        ativo
                    ))

                    conn.commit()
                    mensagem = f"Filial incluída com sucesso. Código gerado: {proximo_codigo}"

                    if eh_ajax():
                        filiais = buscar_filiais_empresa(cur, cod_empresa)
                        filial_nova = next((f for f in filiais if int(f[0]) == int(proximo_codigo)), None)

                        return jsonify({
                            "ok": True,
                            "mensagem": mensagem,
                            "acao": "novo",
                            "filial": filial_para_dict(filial_nova) if filial_nova else None
                        })

                elif acao == "alterar":
                    if not cod_filial_raw:
                        raise ValueError("Selecione uma filial para alterar.")
                    if not nome_filial:
                        raise ValueError("Informe o nome da filial.")

                    cod_filial = int(cod_filial_raw)

                    cur.execute("""
                        UPDATE filiais
                        SET nome_filial = %s,
                            nome_filial_importacao = %s,
                            ativo = %s
                        WHERE cod_empresa = %s
                          AND cod_filial = %s
                    """, (
                        nome_filial,
                        nome_filial_importacao if nome_filial_importacao else None,
                        ativo,
                        cod_empresa,
                        cod_filial
                    ))

                    if cur.rowcount == 0:
                        raise ValueError("Filial não encontrada para alteração.")

                    conn.commit()
                    mensagem = "Filial alterada com sucesso."

                    if eh_ajax():
                        filiais = buscar_filiais_empresa(cur, cod_empresa)
                        filial_alt = next((f for f in filiais if int(f[0]) == int(cod_filial)), None)

                        return jsonify({
                            "ok": True,
                            "mensagem": mensagem,
                            "acao": "alterar",
                            "filial": filial_para_dict(filial_alt) if filial_alt else None
                        })

                elif acao == "excluir":
                    if not cod_filial_raw:
                        raise ValueError("Selecione uma filial para excluir.")

                    cod_filial = int(cod_filial_raw)

                    cur.execute("""
                        SELECT COUNT(*)
                        FROM lancamentos
                        WHERE cod_empresa = %s
                          AND cod_filial = %s
                    """, (cod_empresa, cod_filial))
                    qtde_lanc = cur.fetchone()[0]

                    if qtde_lanc > 0:
                        raise ValueError("Esta filial possui lançamentos e não pode ser excluída.")

                    cur.execute("""
                        DELETE FROM filiais
                        WHERE cod_empresa = %s
                          AND cod_filial = %s
                    """, (cod_empresa, cod_filial))

                    if cur.rowcount == 0:
                        raise ValueError("Filial não encontrada para exclusão.")

                    conn.commit()
                    mensagem = "Filial excluída com sucesso."

                    if eh_ajax():
                        return jsonify({
                            "ok": True,
                            "mensagem": mensagem,
                            "acao": "excluir",
                            "cod_filial": cod_filial
                        })

                elif acao == "carregar":
                    if not cod_filial_raw:
                        raise ValueError("Selecione uma filial para carregar.")

                    cod_filial = int(cod_filial_raw)

                    cur.execute("""
                        SELECT
                            cod_filial,
                            nome_filial,
                            nome_filial_importacao,
                            ativo
                        FROM filiais
                        WHERE cod_empresa = %s
                          AND cod_filial = %s
                    """, (cod_empresa, cod_filial))
                    filial_edicao = cur.fetchone()

                    if not filial_edicao:
                        raise ValueError("Filial não encontrada.")

                    if eh_ajax():
                        return jsonify({
                            "ok": True,
                            "acao": "carregar",
                            "filial": {
                                "cod_filial": filial_edicao[0],
                                "nome_filial": filial_edicao[1],
                                "nome_filial_importacao": filial_edicao[2] or "",
                                "ativo": bool(filial_edicao[3]),
                            }
                        })

            except Exception as e:
                conn.rollback()
                erro = str(e)

                if eh_ajax():
                    return jsonify({"ok": False, "erro": erro}), 400

        filiais = buscar_filiais_empresa(cur, cod_empresa)

    finally:
        cur.close()
        conn.close()

    return render_template(
        "filiais.html",
        filiais=filiais,
        filial_edicao=filial_edicao,
        mensagem=mensagem,
        erro=erro,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("sistema.selecionar_sistema"),
        url_menu_modulo=url_for("financeiro.menu_empresa"),
        texto_menu_modulo="Menu do Financeiro"
    )

# =========================
# GRUPOS GERENCIAIS
# =========================
@financeiro_bp.route("/grupos_gerenciais")
def grupos_gerenciais():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT cod_grupo, abreviatura, descricao
            FROM grupos_gerenciais
            ORDER BY cod_grupo
        """)
        grupos = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return render_template(
        "grupos_gerenciais.html",
        grupos=grupos,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_cadastros"),
        texto_voltar="← Voltar"
    )


# =========================
# CONTAS GERENCIAIS
# =========================
@financeiro_bp.route("/contas_gerenciais", methods=["GET", "POST"])
def contas_gerenciais():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    mensagem = ""
    erro = ""
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        if request.method == "POST":
            try:
                descricoes = request.form.getlist("descricao[]")
                grupos = request.form.getlist("cod_grupo[]")
                contas_post = request.form.getlist("cod_conta[]")

                if not (len(descricoes) == len(grupos) == len(contas_post)):
                    raise ValueError("Os dados enviados estão inconsistentes.")

                # checkbox só vem no POST quando marcado; a lista de marcados
                # identifica cada conta por "grupo-conta"
                projetar_marcados = set(request.form.getlist("projetar[]"))

                for i in range(len(descricoes)):
                    cod_grupo = int(grupos[i])
                    cod_conta = int(contas_post[i])
                    descricao = (descricoes[i] or "").strip()

                    if descricao == "":
                        descricao = None

                    projetar = f"{cod_grupo}-{cod_conta}" in projetar_marcados

                    cur.execute("""
                        UPDATE contas_gerenciais
                        SET descricao = %s,
                            projetar = %s
                        WHERE cod_empresa = %s
                          AND cod_grupo = %s
                          AND cod_conta = %s
                    """, (descricao, projetar, cod_empresa, cod_grupo, cod_conta))

                conn.commit()
                mensagem = "Contas atualizadas com sucesso."

            except Exception as e:
                conn.rollback()
                erro = str(e)

        cur.execute("""
            SELECT
                c.cod_grupo,
                g.abreviatura AS nome_grupo,
                c.cod_conta,
                c.descricao,
                c.projetar
            FROM contas_gerenciais c
            LEFT JOIN grupos_gerenciais g
                ON c.cod_grupo = g.cod_grupo
            WHERE c.cod_empresa = %s
            ORDER BY c.cod_grupo, c.cod_conta
        """, (cod_empresa,))
        contas = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return render_template(
        "contas_gerenciais.html",
        contas=contas,
        mensagem=mensagem,
        erro=erro,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_cadastros"),
        texto_voltar="← Voltar"
    )


# =========================
# CLASSIFICAÇÕES AUTOMÁTICAS
# =========================
@financeiro_bp.route("/classificacoes_automaticas", methods=["GET", "POST"])
def classificacoes_automaticas():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    mensagem = ""
    erro = ""
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        if request.method == "POST":
            acao = (request.form.get("acao") or "").strip().lower()

            try:
                if acao == "salvar":
                    ids = request.form.getlist("id_classificacao[]")
                    textos = request.form.getlist("texto[]")
                    grupos = request.form.getlist("cod_grupo[]")
                    contas = request.form.getlist("cod_conta[]")
                    complementos = request.form.getlist("complemento[]")

                    if not (len(ids) == len(textos) == len(grupos) == len(contas) == len(complementos)):
                        raise ValueError("Os dados enviados estão inconsistentes.")

                    for i in range(len(ids)):
                        id_classificacao = (ids[i] or "").strip()
                        texto = (textos[i] or "").strip()
                        grupo_raw = (grupos[i] or "").strip()
                        conta_raw = (contas[i] or "").strip()
                        complemento = (complementos[i] or "").strip()

                        if not texto:
                            continue

                        if not grupo_raw or not conta_raw:
                            raise ValueError(f"Informe grupo e conta na linha {i + 1}.")

                        cod_grupo = int(grupo_raw)
                        cod_conta = int(conta_raw)

                        if cod_grupo < 1 or cod_grupo > 7:
                            raise ValueError(f"Grupo inválido na linha {i + 1}.")
                        if cod_conta < 1 or cod_conta > 15:
                            raise ValueError(f"Conta inválida na linha {i + 1}.")

                        if id_classificacao:
                            cur.execute("""
                                UPDATE classificacoes_automaticas
                                   SET texto = %s,
                                       cod_grupo = %s,
                                       cod_conta = %s,
                                       complemento = %s
                                 WHERE id_classificacao = %s
                                   AND cod_empresa = %s
                            """, (
                                texto,
                                cod_grupo,
                                cod_conta,
                                complemento if complemento else None,
                                int(id_classificacao),
                                cod_empresa
                            ))
                        else:
                            cur.execute("""
                                INSERT INTO classificacoes_automaticas
                                    (cod_empresa, texto, cod_grupo, cod_conta, complemento)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (
                                cod_empresa,
                                texto,
                                cod_grupo,
                                cod_conta,
                                complemento if complemento else None
                            ))

                    conn.commit()
                    mensagem = "Classificações automáticas salvas com sucesso."

                elif acao == "excluir":
                    id_excluir = (request.form.get("id_excluir") or "").strip()
                    if not id_excluir:
                        raise ValueError("Registro não informado para exclusão.")

                    cur.execute("""
                        DELETE FROM classificacoes_automaticas
                        WHERE id_classificacao = %s
                          AND cod_empresa = %s
                    """, (int(id_excluir), cod_empresa))

                    if cur.rowcount == 0:
                        raise ValueError("Registro não encontrado para exclusão.")

                    conn.commit()
                    mensagem = "Registro excluído com sucesso."

            except Exception as e:
                conn.rollback()
                erro = str(e)

        cur.execute("""
            SELECT
                ca.id_classificacao,
                ca.texto,
                ca.cod_grupo,
                ca.cod_conta,
                cg.descricao AS descricao_conta,
                ca.complemento
            FROM classificacoes_automaticas ca
            LEFT JOIN contas_gerenciais cg
                   ON cg.cod_empresa = ca.cod_empresa
                  AND cg.cod_grupo = ca.cod_grupo
                  AND cg.cod_conta = ca.cod_conta
            WHERE ca.cod_empresa = %s
            ORDER BY LOWER(ca.texto)
        """, (cod_empresa,))
        classificacoes = cur.fetchall()

        # opções dos seletores: código + descrição (contas são por empresa)
        cur.execute("""
            SELECT cod_grupo, COALESCE(descricao, abreviatura)
            FROM grupos_gerenciais
            ORDER BY cod_grupo
        """)
        opcoes_grupos = cur.fetchall()

        cur.execute("""
            SELECT cod_grupo, cod_conta, COALESCE(descricao, '')
            FROM contas_gerenciais
            WHERE cod_empresa = %s
            ORDER BY cod_grupo, cod_conta
        """, (cod_empresa,))
        opcoes_contas = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return render_template(
        "classificacoes_automaticas.html",
        classificacoes=classificacoes,
        opcoes_grupos=opcoes_grupos,
        opcoes_contas=opcoes_contas,
        mensagem=mensagem,
        erro=erro,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_cadastros"),
        texto_voltar="← Voltar"
    )


# =========================
# LANÇAMENTOS
# =========================
MESES_LANCAMENTOS = [
    ("1", "01 - Janeiro"), ("2", "02 - Fevereiro"), ("3", "03 - Março"),
    ("4", "04 - Abril"), ("5", "05 - Maio"), ("6", "06 - Junho"),
    ("7", "07 - Julho"), ("8", "08 - Agosto"), ("9", "09 - Setembro"),
    ("10", "10 - Outubro"), ("11", "11 - Novembro"), ("12", "12 - Dezembro"),
]


@financeiro_bp.route("/lancamentos")
def listar_lancamentos():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    page = request.args.get("page", default=1, type=int)
    if page < 1:
        page = 1

    filial_sel = (request.args.get("filial") or "").strip()
    grupo_sel = (request.args.get("grupo") or "").strip()
    conta_sel = (request.args.get("conta") or "").strip()
    ano_sel = (request.args.get("ano") or "").strip()
    mes_sel = (request.args.get("mes") or "").strip()
    busca = (request.args.get("busca") or "").strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Filiais: o filtro é pelo código; o rótulo mostra código + nome.
        cur.execute("""
            SELECT CAST(cod_filial AS TEXT), MAX(nome_filial)
            FROM lancamentos
            WHERE cod_empresa = %s
              AND cod_filial IS NOT NULL
            GROUP BY cod_filial
            ORDER BY
                CASE WHEN CAST(cod_filial AS TEXT) ~ '^[0-9]+$'
                     THEN CAST(CAST(cod_filial AS TEXT) AS INTEGER) END,
                CAST(cod_filial AS TEXT)
        """, (cod_empresa,))
        filiais = cur.fetchall()

        # Grupos e contas saem do cadastro gerencial, com número e descrição.
        cur.execute("""
            SELECT CAST(cod_grupo AS TEXT), abreviatura, descricao
            FROM grupos_gerenciais
            ORDER BY cod_grupo
        """)
        grupos = cur.fetchall()

        cur.execute("""
            SELECT CAST(cod_grupo AS TEXT), CAST(cod_conta AS TEXT),
                   COALESCE(descricao, '')
            FROM contas_gerenciais
            WHERE cod_empresa = %s
            ORDER BY
                CASE WHEN CAST(cod_grupo AS TEXT) ~ '^[0-9]+$'
                     THEN CAST(CAST(cod_grupo AS TEXT) AS INTEGER) END,
                CASE WHEN CAST(cod_conta AS TEXT) ~ '^[0-9]+$'
                     THEN CAST(CAST(cod_conta AS TEXT) AS INTEGER) END,
                CAST(cod_conta AS TEXT)
        """, (cod_empresa,))
        contas = cur.fetchall()

        # Mesmas contas agrupadas por grupo, para os optgroups da edição.
        contas_por_grupo = []
        for gcod, ccod, desc in contas:
            if not contas_por_grupo or contas_por_grupo[-1][0] != gcod:
                contas_por_grupo.append((gcod, []))
            contas_por_grupo[-1][1].append((ccod, desc))

        cur.execute("""
            SELECT DISTINCT ano
            FROM lancamentos
            WHERE cod_empresa = %s
              AND ano IS NOT NULL
            ORDER BY ano DESC
        """, (cod_empresa,))
        anos = [str(r[0]) for r in cur.fetchall()]

        where = ["cod_empresa = %s"]
        params = [cod_empresa]

        if filial_sel:
            where.append("CAST(cod_filial AS TEXT) = %s")
            params.append(filial_sel)

        if grupo_sel:
            where.append("CAST(grupo AS TEXT) = %s")
            params.append(grupo_sel)

        # O código da conta se repete entre grupos, então o filtro vem como
        # "grupo.conta" e amarra os dois.
        if conta_sel:
            if "." in conta_sel:
                g_conta, c_conta = conta_sel.split(".", 1)
                where.append("CAST(grupo AS TEXT) = %s AND CAST(conta AS TEXT) = %s")
                params.extend([g_conta, c_conta])
            else:
                where.append("CAST(conta AS TEXT) = %s")
                params.append(conta_sel)

        if ano_sel:
            where.append("CAST(ano AS TEXT) = %s")
            params.append(ano_sel)

        if mes_sel:
            where.append("CAST(mes AS INTEGER) = %s")
            params.append(int(mes_sel))

        if busca:
            where.append("""
                (
                    UPPER(COALESCE(historico, '')) LIKE UPPER(%s)
                    OR UPPER(COALESCE(descricao_conta, '')) LIKE UPPER(%s)
                    OR UPPER(COALESCE(complemento, '')) LIKE UPPER(%s)
                )
            """)
            termo = f"%{busca}%"
            params.extend([termo, termo, termo])

        where_sql = " AND ".join(where)

        cur.execute(f"""
            SELECT COUNT(*)
            FROM lancamentos
            WHERE {where_sql}
        """, params)
        total_rows = cur.fetchone()[0]

        page_size = 100
        total_pages = max(1, math.ceil(total_rows / page_size))

        if page > total_pages:
            page = total_pages

        offset = (page - 1) * page_size

        cur.execute(f"""
            SELECT
                cod_filial,
                nome_filial,
                conta_banco,
                ano,
                mes,
                data,
                historico,
                valor,
                descricao_conta,
                grupo,
                conta,
                complemento,
                id_lancamento,
                cod_empresa
            FROM lancamentos
            WHERE {where_sql}
            ORDER BY id_lancamento
            LIMIT %s OFFSET %s
        """, params + [page_size, offset])

        colnames = [d[0] for d in cur.description]
        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return render_template(
        "lancamentos.html",
        rows=rows,
        colnames=colnames,
        page=page,
        total_pages=total_pages,
        has_prev=page > 1,
        has_next=page < total_pages,
        total_rows=total_rows,
        page_size=page_size,
        filiais=filiais,
        grupos=grupos,
        contas=contas,
        contas_por_grupo=contas_por_grupo,
        anos=anos,
        meses=MESES_LANCAMENTOS,
        filial_sel=filial_sel,
        grupo_sel=grupo_sel,
        conta_sel=conta_sel,
        ano_sel=ano_sel,
        mes_sel=mes_sel,
        busca=busca,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_fluxo_caixa"),
        texto_voltar="← Voltar"
    )


# =========================
# ATUALIZAR LANÇAMENTO
# =========================
@financeiro_bp.route("/lancamentos/atualizar/<int:id_lancamento>", methods=["POST"])
def atualizar_lancamento(id_lancamento):
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    grupo = (request.form.get("grupo") or "").strip()
    conta = (request.form.get("conta") or "").strip()

    if not grupo or not conta:
        return "Grupo e conta são obrigatórios.", 400

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE lancamentos
            SET grupo = %s,
                conta = %s
            WHERE cod_empresa = %s
              AND id_lancamento = %s
        """, (grupo, conta, cod_empresa, id_lancamento))

        conn.commit()

    finally:
        cur.close()
        conn.close()

    return redirect(url_for(
        "financeiro.listar_lancamentos",
        page=request.form.get("page", 1),
        filial=request.form.get("filial", ""),
        grupo=request.form.get("filtro_grupo", ""),
        conta=request.form.get("filtro_conta", ""),
        ano=request.form.get("filtro_ano", ""),
        mes=request.form.get("filtro_mes", ""),
        busca=request.form.get("busca", "")
    ))


# =========================
# RESULTADO POR MARGEM BRUTA
# =========================
@financeiro_bp.route("/resultado_mb", methods=["GET"])
def resultado_mb():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    ano = request.args.get("ano", type=int)
    mes = request.args.get("mes", type=int)

    hoje = datetime.now()

    # Sugere o mês anterior
    if not ano and not mes:
        if hoje.month == 1:
            ano = hoje.year - 1
            mes = 12
        else:
            ano = hoje.year
            mes = hoje.month - 1
    elif ano and not mes:
        if hoje.month == 1:
            mes = 12
            if ano == hoje.year:
                ano = ano - 1
        else:
            mes = hoje.month - 1
    elif mes and not ano:
        if hoje.month == 1 and mes == 12:
            ano = hoje.year - 1
        else:
            ano = hoje.year

    # Filial selecionada (vazio = todas)
    filial_sel_raw = (request.args.get("filial") or "").strip()
    filiais_sel = [int(filial_sel_raw)] if filial_sel_raw.isdigit() else []

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s AND ativo = true
            ORDER BY cod_filial
        """, (cod_empresa,))
        todas_filiais = cur.fetchall()

        # Filiais exibidas (filtradas ou todas)
        if filiais_sel:
            filiais = [f for f in todas_filiais if f[0] in filiais_sel]
        else:
            filiais = todas_filiais

        def buscar_valores(tabela, filtro_extra):
            where_mes = ""
            params = [cod_empresa, ano]
            if mes:
                where_mes = "AND mes = %s"
                params.append(mes)
            campo_valor = "margem_bruta" if tabela == "vendas_mb_sintetico" else "valor"
            cur.execute(f"""
                SELECT cod_filial, COALESCE(SUM({campo_valor}), 0)
                FROM {tabela}
                WHERE cod_empresa = %s AND ano = %s
                {where_mes} {filtro_extra}
                GROUP BY cod_filial
            """, params)
            return {r[0]: float(r[1]) for r in cur.fetchall()}

        mb   = buscar_valores("vendas_mb_sintetico", "")
        desp = buscar_valores("lancamentos", "AND grupo = '4'")
        inv  = buscar_valores("lancamentos", "AND grupo = '5'")
        div  = buscar_valores("lancamentos", "AND grupo = '6'")

        def montar_linha(nome, base):
            linha = {"nome": nome, "total": 0.0, "por_filial": {}}
            for cod_filial, _ in filiais:
                v = base.get(cod_filial, 0.0)
                linha["por_filial"][cod_filial] = v
                linha["total"] += v
            return linha

        linha_mb   = montar_linha("MB", mb)
        linha_desp = montar_linha("DESPESAS", desp)
        linha_res1 = montar_linha("RESULTADO 1", {
            f[0]: mb.get(f[0], 0.0) + desp.get(f[0], 0.0) for f in filiais
        })
        linha_inv  = montar_linha("INVESTIMENTOS / AMORTIZAÇÕES", inv)
        linha_res2 = montar_linha("RESULTADO 2", {
            f[0]: linha_res1["por_filial"][f[0]] + inv.get(f[0], 0.0) for f in filiais
        })
        linha_div  = montar_linha("ANTECIPAÇÃO DIVIDENDOS", div)
        linha_res3 = montar_linha("RESULTADO 3", {
            f[0]: linha_res2["por_filial"][f[0]] + div.get(f[0], 0.0) for f in filiais
        })

        linhas = [linha_mb, linha_desp, linha_res1,
                  linha_inv, linha_res2, linha_div, linha_res3]

    finally:
        cur.close()
        conn.close()

    return render_template(
        "resultado_mb.html",
        todas_filiais=todas_filiais,
        filiais=filiais,
        filial_sel=filial_sel_raw,
        linhas=linhas,
        ano=ano,
        mes=mes,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_fluxo_caixa")
    )

# =========================
# RESULTADO MB ANUAL POR POSTO
# =========================


@financeiro_bp.route("/resultado_mb_anual", methods=["GET"])
def resultado_mb_anual():
    """Mesmo Resultado por Margem Bruta, virado de lado: as colunas sao os 12
    meses do ano — de um posto ou de todos somados."""
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    hoje = datetime.now()

    ano = request.args.get("ano", type=int)
    filial_sel_raw = (request.args.get("filial") or "").strip()
    filial_sel = int(filial_sel_raw) if filial_sel_raw.isdigit() else None

    conn = get_connection()
    cur = conn.cursor()

    try:
        if not ano:
            cur.execute("""
                SELECT ano FROM lancamentos
                WHERE cod_empresa = %s AND ano IS NOT NULL
                ORDER BY ano DESC
                LIMIT 1
            """, (cod_empresa,))
            row = cur.fetchone()
            ano = int(row[0]) if row else hoje.year

        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s AND ativo = true
            ORDER BY cod_filial
        """, (cod_empresa,))
        todas_filiais = cur.fetchall()

        nome_filial_sel = "Todos os postos"
        for f in todas_filiais:
            if f[0] == filial_sel:
                nome_filial_sel = f"{f[0]} - {f[1]}"

        def buscar_valores(tabela, filtro_extra):
            """Soma por mes. Sem filial escolhida, soma todos os postos."""
            params = [cod_empresa, ano]
            where_filial = ""
            if filial_sel is not None:
                where_filial = "AND cod_filial = %s"
                params.append(filial_sel)
            campo_valor = "margem_bruta" if tabela == "vendas_mb_sintetico" else "valor"
            cur.execute(f"""
                SELECT mes, COALESCE(SUM({campo_valor}), 0)
                FROM {tabela}
                WHERE cod_empresa = %s AND ano = %s
                  AND mes IS NOT NULL
                {where_filial} {filtro_extra}
                GROUP BY mes
            """, params)
            return {int(r[0]): float(r[1]) for r in cur.fetchall()}

        mb   = buscar_valores("vendas_mb_sintetico", "")
        desp = buscar_valores("lancamentos", "AND grupo = '4'")
        inv  = buscar_valores("lancamentos", "AND grupo = '5'")
        div  = buscar_valores("lancamentos", "AND grupo = '6'")

        meses_colunas = list(MESES_NOMES)
        meses_ids = [m[0] for m in meses_colunas]

        def montar_linha(nome, base):
            linha = {"nome": nome, "total": 0.0, "por_mes": {}}
            for mes_id in meses_ids:
                v = base.get(mes_id, 0.0)
                linha["por_mes"][mes_id] = v
                linha["total"] += v
            return linha

        linha_mb   = montar_linha("MB", mb)
        linha_desp = montar_linha("DESPESAS", desp)
        linha_res1 = montar_linha("RESULTADO 1", {
            m: mb.get(m, 0.0) + desp.get(m, 0.0) for m in meses_ids
        })
        linha_inv  = montar_linha("INVESTIMENTOS / AMORTIZAÇÕES", inv)
        linha_res2 = montar_linha("RESULTADO 2", {
            m: linha_res1["por_mes"][m] + inv.get(m, 0.0) for m in meses_ids
        })
        linha_div  = montar_linha("ANTECIPAÇÃO DIVIDENDOS", div)
        linha_res3 = montar_linha("RESULTADO 3", {
            m: linha_res2["por_mes"][m] + div.get(m, 0.0) for m in meses_ids
        })

        linhas = [linha_mb, linha_desp, linha_res1,
                  linha_inv, linha_res2, linha_div, linha_res3]

    finally:
        cur.close()
        conn.close()

    return render_template(
        "resultado_mb_anual.html",
        todas_filiais=todas_filiais,
        meses_colunas=meses_colunas,
        linhas=linhas,
        ano=ano,
        filial_sel=filial_sel_raw,
        nome_filial_sel=nome_filial_sel,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_fluxo_caixa")
    )

# =========================
# MATRICIAL
# =========================


@financeiro_bp.route("/matricial")
def matricial():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    hoje = datetime.now()

    ano_sel = (request.args.get("ano") or "").strip()
    mes_sel = (request.args.get("mes") or "").strip()
    filial_sel = (request.args.get("filial") or "").strip()

    if not ano_sel and not mes_sel:
        conn_def = get_connection()
        cur_def = conn_def.cursor()
        try:
            cur_def.execute("""
                SELECT ano, mes FROM lancamentos
                WHERE cod_empresa = %s
                ORDER BY ano DESC, mes DESC
                LIMIT 1
            """, (cod_empresa,))
            row_def = cur_def.fetchone()
        finally:
            cur_def.close()
            conn_def.close()
        if row_def:
            ano_sel = str(row_def[0])
            mes_sel = str(row_def[1])
        elif hoje.month == 1:
            ano_sel = str(hoje.year - 1)
            mes_sel = "12"
        else:
            ano_sel = str(hoje.year)
            mes_sel = str(hoje.month - 1)

    dados = obter_dados_matricial(
        cod_empresa,
        ano_sel,
        mes_sel,
        filial_sel
    )

    return render_template(
        "matricial.html",
        filiais_colunas=dados["filiais_colunas"],
        linhas_matriciais=dados["linhas_matriciais"],
        grupos_ordenados=dados["grupos_ordenados"],
        total_geral_filiais=dados["total_geral_filiais"],
        total_geral=dados["total_geral"],
        anos=dados["anos"],
        meses=dados["meses"],
        filiais=dados["filiais"],
        ano_sel=ano_sel,
        mes_sel=mes_sel,
        filial_sel=filial_sel,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        formatar_numero_br=formatar_numero_br,
        url_voltar=url_for("financeiro.menu_fluxo_caixa"),
        texto_voltar="← Voltar"
    )

# =========================
# MATRICIAL ANUAL POR POSTO
# =========================


@financeiro_bp.route("/matricial-anual")
def matricial_anual():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    hoje = datetime.now()

    ano_sel = (request.args.get("ano") or "").strip()
    filial_sel = (request.args.get("filial") or "").strip()

    if not ano_sel:
        conn_def = get_connection()
        cur_def = conn_def.cursor()
        try:
            cur_def.execute("""
                SELECT ano FROM lancamentos
                WHERE cod_empresa = %s
                ORDER BY ano DESC
                LIMIT 1
            """, (cod_empresa,))
            row_def = cur_def.fetchone()
        finally:
            cur_def.close()
            conn_def.close()

        ano_sel = str(row_def[0]) if row_def else str(hoje.year)

    dados = obter_dados_matricial_anual(cod_empresa, ano_sel, filial_sel)

    nome_filial_sel = "Todos os postos"
    for f in dados["filiais"]:
        if str(f[0]) == filial_sel:
            nome_filial_sel = f"{f[0]}-{f[1]}"

    return render_template(
        "matricial_anual.html",
        meses_colunas=dados["meses_colunas"],
        linhas_matriciais=dados["linhas_matriciais"],
        grupos_ordenados=dados["grupos_ordenados"],
        total_geral_meses=dados["total_geral_meses"],
        total_geral=dados["total_geral"],
        anos=dados["anos"],
        filiais=dados["filiais"],
        ano_sel=ano_sel,
        filial_sel=filial_sel,
        nome_filial_sel=nome_filial_sel,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        formatar_numero_br=formatar_numero_br,
        url_voltar=url_for("financeiro.menu_fluxo_caixa"),
        texto_voltar="← Voltar"
    )

# =========================
# VARIACOES 
# =========================

@financeiro_bp.route("/variacoes")
def variacoes():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    ano_sel = (request.args.get("ano") or "").strip()
    filial_sel = (request.args.get("filial") or "").strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:

        # captura o parâmetro original
        ano_param = request.args.get("ano", None)

        # lista de anos
        cur.execute("""
            SELECT DISTINCT ano
            FROM lancamentos
            WHERE cod_empresa = %s
              AND ano IS NOT NULL
            ORDER BY ano
        """, (cod_empresa,))
        anos = [r["ano"] for r in cur.fetchall()]

        # definição do ano selecionado
        if ano_param is None:
            # primeira vez: seleciona o último ano existente na base
            ano_sel = str(anos[-1]) if anos else ""
        else:
            # veio da tela (pode ser "" = Todos ou um ano específico)
            ano_sel = ano_param


        # lista de filiais
        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = TRUE
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()

        # contas gerenciais por grupo
        cur.execute("""
            SELECT
                cg.cod_grupo,
                gg.descricao AS nome_grupo,
                cg.cod_conta,
                cg.descricao
            FROM contas_gerenciais cg
            LEFT JOIN grupos_gerenciais gg
              ON gg.cod_grupo = cg.cod_grupo
            WHERE cg.cod_empresa = %s
            ORDER BY cg.cod_grupo, cg.cod_conta
        """, (cod_empresa,))
        contas = cur.fetchall()

        contas_por_grupo = {}
        for c in contas:
            g = c["cod_grupo"]
            if g not in contas_por_grupo:
                contas_por_grupo[g] = {
                    "nome_grupo": c["nome_grupo"] or f"Grupo {g}",
                    "contas": []
                }
            contas_por_grupo[g]["contas"].append({
                "cod_conta": c["cod_conta"],
                "descricao": c["descricao"] or "-"
            })

        # valores dos lançamentos
        params = [cod_empresa]
        sql_ano = ""
        sql_filial = ""

        if ano_sel:
            sql_ano = "AND l.ano >= %s"
            params.append(int(ano_sel))

        if filial_sel:
            sql_filial = "AND l.cod_filial = %s"
            params.append(filial_sel)

        cur.execute(f"""
            SELECT
                l.grupo,
                l.conta,
                l.ano,
                l.mes,
                COALESCE(SUM(l.valor), 0) AS valor
            FROM lancamentos l
            WHERE l.cod_empresa = %s
            {sql_ano}
            {sql_filial}
            AND l.grupo IS NOT NULL
            AND l.conta IS NOT NULL
            GROUP BY l.grupo, l.conta, l.ano, l.mes
            ORDER BY l.ano, l.mes, l.grupo, l.conta
        """, params)

        valores = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    mapa_valores = {}
    for v in valores:
        chave = (
            str(v["grupo"]).strip(),
            str(v["conta"]).strip(),
            int(v["ano"]),
            int(v["mes"])
        )
        mapa_valores[chave] = float(v["valor"] or 0)

    mapa_meses = {
        1: "JAN", 2: "FEV", 3: "MAR", 4: "ABR",
        5: "MAI", 6: "JUN", 7: "JUL", 8: "AGO",
        9: "SET", 10: "OUT", 11: "NOV", 12: "DEZ"
    }
    periodos = sorted(
    {(int(v["ano"]), int(v["mes"])) for v in valores},
    key=lambda x: (x[0], x[1])
    )    

    grupos = []
    for grupo in sorted(contas_por_grupo.keys(), key=lambda x: int(x)):
        info = contas_por_grupo[grupo]
        linhas = []
        ultimo_ano = None

        for ano, num_mes in periodos:
            linha = {
                "ano": ano,
                "mes_num": num_mes,
                "mes_nome": mapa_meses[num_mes],
                "valores": [],
                "total_mes": 0.0,
                "quebra_ano": False
            }

            if ultimo_ano is not None and ano != ultimo_ano:
                linha["quebra_ano"] = True

            ultimo_ano = ano

            for conta in info["contas"]:
                valor = mapa_valores.get(
                    (str(grupo), str(conta["cod_conta"]), ano, num_mes),
                    0.0
                )
                linha["valores"].append(valor)
                linha["total_mes"] += valor

            linhas.append(linha)

        grupos.append({
            "cod_grupo": grupo,
            "nome_grupo": info["nome_grupo"],
            "contas": info["contas"],
            "linhas": linhas
        })
    def cor_excel(valor, vmin, vmax):
        if valor is None:
            return ""

        if vmax == vmin:
            return ""

        ratio = (valor - vmin) / (vmax - vmin)
        ratio = max(0, min(1, ratio))

        if ratio < 0.5:
            r = 255
            g = int(255 * (ratio * 2))
            b = 0
        else:
            r = int(255 * (1 - (ratio - 0.5) * 2))
            g = 255
            b = 0

        return f"background-color: rgb({r},{g},{b});"

    for grupo in grupos:
        colunas = list(zip(*[linha["valores"] for linha in grupo["linhas"]]))
        grupo["min_max"] = []

        for col in colunas:
            valores_validos = [v for v in col if v is not None]

            if valores_validos:
                grupo["min_max"].append({
                    "min": min(valores_validos),
                    "max": max(valores_validos)
                })
            else:
                grupo["min_max"].append({
                    "min": 0,
                    "max": 0
                })
    return render_template(
        "variacoes.html",
        anos=anos,
        ano_sel=ano_sel,
        filiais=filiais,
        filial_sel=filial_sel,
        grupos=grupos,
        nome_empresa=session.get("nome_empresa", ""),
        empresa_ativa=session.get("cod_empresa", ""),
        url_voltar=url_for("financeiro.menu_fluxo_caixa"),
        texto_voltar="← Voltar",
        formatar_numero_br=formatar_numero_br,
        cor_excel=cor_excel
    )

# =========================
# DADOS DETALHADOS
# =========================

@financeiro_bp.route("/dados_detalhados")
def dados_detalhados():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    ano = request.args.get("ano", type=int)
    mes = request.args.get("mes", type=int)
    grupo = request.args.get("grupo", type=int)
    conta_sel = request.args.get("conta", type=int)

    hoje = datetime.now()
    nomes_meses = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                   "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]

    conn = get_connection()
    cur = conn.cursor()

    try:
        # último mês com lançamento (padrão quando sem filtro)
        if not ano and not mes:
            cur.execute("""
                SELECT ano, mes FROM lancamentos
                WHERE cod_empresa = %s
                ORDER BY ano DESC, mes DESC LIMIT 1
            """, (cod_empresa,))
            row_ult = cur.fetchone()
            if row_ult:
                ano, mes = row_ult[0], row_ult[1]
            elif hoje.month == 1:
                ano, mes = hoje.year - 1, 12
            else:
                ano, mes = hoje.year, hoje.month - 1
        elif ano and not mes:
            mes = hoje.month - 1 if hoje.month > 1 else 12
        elif mes and not ano:
            ano = hoje.year

        # anos disponíveis
        cur.execute("""
            SELECT DISTINCT ano FROM lancamentos
            WHERE cod_empresa = %s ORDER BY ano DESC
        """, (cod_empresa,))
        anos_disp = [r[0] for r in cur.fetchall()]

        # grupos com nome
        cur.execute("""
            SELECT DISTINCT l.grupo, COALESCE(cg.descricao, '') AS descricao
            FROM lancamentos l
            LEFT JOIN contas_gerenciais cg
                   ON cg.cod_empresa = l.cod_empresa
                  AND cg.cod_grupo   = l.grupo
                  AND cg.cod_conta   = 0
            WHERE l.cod_empresa = %s
            ORDER BY l.grupo
        """, (cod_empresa,))
        grupos_disp = cur.fetchall()   # [(num, descricao), ...]

        if grupo is None and grupos_disp:
            grupo = grupos_disp[0][0]

        # contas disponíveis para o grupo selecionado
        cur.execute("""
            SELECT DISTINCT l.conta,
                   COALESCE(NULLIF(TRIM(l.descricao_conta),''), 'SEM DESCRIÇÃO') AS nome
            FROM lancamentos l
            WHERE l.cod_empresa = %s AND l.grupo = %s
            ORDER BY l.conta
        """, (cod_empresa, grupo))
        contas_disp = cur.fetchall()   # [(num, nome), ...]

        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s AND ativo = true
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais_rows = cur.fetchall()
        filiais = [r[0] for r in filiais_rows]
        mapa_filiais = {r[0]: r[1] for r in filiais_rows}

        filtros_conta = [conta_sel] if conta_sel else None

        query = """
            SELECT
                conta,
                COALESCE(NULLIF(TRIM(descricao_conta), ''), 'SEM DESCRIÇÃO') AS descricao_conta,
                COALESCE(NULLIF(TRIM(historico), ''), 'SEM HISTÓRICO') AS historico,
                cod_filial,
                COALESCE(SUM(valor), 0) AS total_valor
            FROM lancamentos
            WHERE cod_empresa = %s AND ano = %s AND mes = %s AND grupo = %s
        """
        params = [cod_empresa, ano, mes, grupo]
        if filtros_conta:
            query += " AND conta = %s"
            params.append(conta_sel)
        query += """
            GROUP BY conta,
                COALESCE(NULLIF(TRIM(descricao_conta), ''), 'SEM DESCRIÇÃO'),
                COALESCE(NULLIF(TRIM(historico), ''), 'SEM HISTÓRICO'),
                cod_filial
            ORDER BY conta, descricao_conta, historico, cod_filial
        """
        cur.execute(query, params)
        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    def conta_para_ordenacao(valor):
        texto = str(valor or "").strip()
        try:
            return (0, int(texto))
        except Exception:
            return (1, texto.upper())

    def classe_heatmap(valor, minimo, maximo):
        if maximo <= minimo:
            return "hm-25"

        proporcao = (valor - minimo) / (maximo - minimo)
        proporcao = max(0, min(1, proporcao))

        faixa = int(round(proporcao * 50))
        return f"hm-{faixa}"

    dados = {}
    totais_gerais = {f: 0.0 for f in filiais}

    for conta, descricao_conta, historico, cod_filial, total_valor in rows:
        valor = float(total_valor or 0)

        if conta not in dados:
            dados[conta] = {
                "nome": descricao_conta,
                "linhas": {},
                "totais": {f: 0.0 for f in filiais}
            }

        if historico not in dados[conta]["linhas"]:
            dados[conta]["linhas"][historico] = {
                "filiais": {f: 0.0 for f in filiais},
                "classes": {f: "" for f in filiais}
            }

        dados[conta]["linhas"][historico]["filiais"][cod_filial] = valor
        dados[conta]["totais"][cod_filial] += valor
        totais_gerais[cod_filial] += valor

    # aplica heatmap por linha de histórico
    for conta, info in dados.items():
        for hist, linha in info["linhas"].items():
            valores = list(linha["filiais"].values())
            minimo = min(valores) if valores else 0
            maximo = max(valores) if valores else 0

            for f in filiais:
                linha["classes"][f] = classe_heatmap(
                    linha["filiais"][f],
                    minimo,
                    maximo
                )

    # ordena contas
    dados_ordenados = dict(
        sorted(
            dados.items(),
            key=lambda item: conta_para_ordenacao(item[0])
        )
    )

    return render_template(
        "dados_detalhados.html",
        ano=ano,
        mes=mes,
        grupo=grupo,
        conta_sel=conta_sel,
        filiais=filiais,
        mapa_filiais=mapa_filiais,
        dados=dados_ordenados,
        totais_gerais=totais_gerais,
        anos_disp=anos_disp,
        grupos_disp=grupos_disp,
        contas_disp=contas_disp,
        nomes_meses=nomes_meses,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_fluxo_caixa"),
        texto_voltar="← Voltar"
    )

# =========================
# MARGEM BRUTA
# =========================

@financeiro_bp.route("/margem_bruta", methods=["GET", "POST"])
def margem_bruta():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    mensagem = ""
    erro = ""

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = TRUE
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall()

        if request.method == "POST":
            try:
                for chave, valor_txt in request.form.items():
                    if not chave.startswith("valor_"):
                        continue

                    _, ano_txt, mes_txt, cod_filial_txt = chave.split("_", 3)

                    ano = int(ano_txt)
                    mes = int(mes_txt)
                    cod_filial = int(cod_filial_txt)

                    valor_txt = (valor_txt or "").strip()
                    if valor_txt == "":
                        valor = 0.0
                    else:
                        valor = float(valor_txt.replace(".", "").replace(",", "."))

                    cur.execute("""
                        INSERT INTO vendas_mb_sintetico (
                            cod_empresa,
                            cod_filial,
                            ano,
                            mes,
                            margem_bruta
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (cod_empresa, cod_filial, ano, mes)
                        DO UPDATE SET
                            margem_bruta = EXCLUDED.margem_bruta,
                            data_importacao = NOW()
                    """, (cod_empresa, cod_filial, ano, mes, valor))

                conn.commit()
                mensagem = "Margem bruta salva com sucesso."

            except Exception as e:
                conn.rollback()
                erro = str(e)

        cur.execute("""
            SELECT DISTINCT ano, mes
            FROM vendas_mb_sintetico
            WHERE cod_empresa = %s
            ORDER BY ano, mes
        """, (cod_empresa,))
        periodos = cur.fetchall()

        cur.execute("""
            SELECT cod_filial, ano, mes, margem_bruta
            FROM vendas_mb_sintetico
            WHERE cod_empresa = %s
            ORDER BY ano, mes, cod_filial
        """, (cod_empresa,))
        registros = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    mapa = {}
    for cod_filial, ano, mes, margem_bruta in registros:
        chave = (ano, mes)
        if chave not in mapa:
            mapa[chave] = {
                "ano": ano,
                "mes": mes,
                "valores": {},
                "total": 0.0
            }

        v = float(margem_bruta or 0)
        mapa[chave]["valores"][cod_filial] = v
        mapa[chave]["total"] += v

    linhas = []
    for ano, mes in periodos:
        chave = (ano, mes)
        if chave in mapa:
            linhas.append(mapa[chave])
        else:
            linhas.append({
                "ano": ano,
                "mes": mes,
                "valores": {},
                "total": 0.0
            })

    return render_template(
        "margem_bruta.html",
        filiais=filiais,
        linhas=linhas,
        mensagem=mensagem,
        erro=erro,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_fluxo_caixa"),
        texto_voltar="← Voltar"
    )

    
# =========================
# EXCLUSÕES DE LANÇAMENTOS
# =========================
@financeiro_bp.route("/exclusoes", methods=["GET", "POST"])
def exclusoes():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    mensagem = ""
    erro = ""

    # filtros
    ano_sel = request.values.get("ano", "")
    mes_sel = request.values.get("mes", "")
    filial_sel = request.values.get("filial", "")
    grupo_sel = request.values.get("grupo", "")
    conta_sel = request.values.get("conta", "")
    data_ini = request.values.get("data_ini", "")
    data_fim = request.values.get("data_fim", "")
    busca = request.values.get("busca", "")

    conn = get_connection()
    cur = conn.cursor()

    try:
        # =========================
        # EXCLUSÃO (POST)
        # =========================
        if request.method == "POST":
            ids_raw = request.form.getlist("ids_marcados")

            try:
                ids = [int(x) for x in ids_raw if str(x).strip().isdigit()]
            except Exception:
                ids = []

            if ids:
                cur.execute("""
                    DELETE FROM lancamentos
                    WHERE id_lancamento = ANY(%s::int[])
                      AND cod_empresa = %s
                """, (ids, cod_empresa))

                conn.commit()
                mensagem = f"{len(ids)} lançamento(s) excluído(s) com sucesso."
            else:
                erro = "Nenhum registro válido selecionado."

        # =========================
        # PERÍODO SUGERIDO (primeiro acesso)
        # =========================
        # Sem ano/mês informados nenhuma <option> ficava marcada e o navegador
        # exibia a primeira da lista — e o combo de meses é crescente, então
        # aparecia o mês MAIS ANTIGO com dados. Sugere o mais recente, mesmo
        # critério já usado na consulta matricial.
        if not ano_sel and not mes_sel:
            cur.execute("""
                SELECT ano, mes FROM lancamentos
                WHERE cod_empresa = %s
                  AND ano IS NOT NULL
                  AND mes IS NOT NULL
                ORDER BY ano DESC, mes DESC
                LIMIT 1
            """, (cod_empresa,))
            row_def = cur.fetchone()
            if row_def:
                ano_sel = str(int(row_def[0]))
                mes_sel = str(int(row_def[1]))

        # =========================
        # FILTROS DINÂMICOS
        # =========================
        where = ["cod_empresa = %s"]
        params = [cod_empresa]

        if ano_sel:
            where.append("ano = %s")
            params.append(ano_sel)

        if mes_sel:
            where.append("mes = %s")
            params.append(mes_sel)

        if filial_sel:
            where.append("nome_filial = %s")
            params.append(filial_sel)

        if grupo_sel:
            where.append("grupo = %s")
            params.append(grupo_sel)

        if conta_sel:
            where.append("conta = %s")
            params.append(conta_sel)

        if data_ini:
            where.append("data >= %s")
            params.append(data_ini)

        if data_fim:
            where.append("data <= %s")
            params.append(data_fim)

        if busca:
            where.append("""
                (historico ILIKE %s OR
                 descricao_conta ILIKE %s OR
                 complemento ILIKE %s)
            """)
            like = f"%{busca}%"
            params.extend([like, like, like])

        where_sql = " AND ".join(where)

        # =========================
        # CONSULTA PRINCIPAL
        # =========================
        cur.execute(f"""
            SELECT
                id_lancamento,
                data,
                nome_filial,
                historico,
                valor,
                grupo,
                conta,
                descricao_conta,
                complemento,
                cod_empresa
            FROM lancamentos
            WHERE {where_sql}
            ORDER BY data DESC, id_lancamento DESC
            LIMIT 1000
        """, params)

        rows = cur.fetchall()

        # =========================
        # COMBOS (FILTROS)
        # =========================
        cur.execute("""
            SELECT DISTINCT ano FROM lancamentos
            WHERE cod_empresa = %s
            ORDER BY ano DESC
        """, (cod_empresa,))
        anos = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT mes FROM lancamentos
            WHERE cod_empresa = %s
            ORDER BY mes
        """, (cod_empresa,))
        meses = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT nome_filial FROM lancamentos
            WHERE cod_empresa = %s
            ORDER BY nome_filial
        """, (cod_empresa,))
        filiais = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT grupo FROM lancamentos
            WHERE cod_empresa = %s
            ORDER BY grupo
        """, (cod_empresa,))
        grupos = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT conta FROM lancamentos
            WHERE cod_empresa = %s
            ORDER BY conta
        """, (cod_empresa,))
        contas = [r[0] for r in cur.fetchall()]

    finally:
        cur.close()
        conn.close()

    return render_template(
        "exclusoes.html",
        rows=rows,
        anos=anos,
        meses=meses,
        filiais=filiais,
        grupos=grupos,
        contas=contas,
        ano_sel=ano_sel,
        mes_sel=mes_sel,
        filial_sel=filial_sel,
        grupo_sel=grupo_sel,
        conta_sel=conta_sel,
        data_ini=data_ini,
        data_fim=data_fim,
        busca=busca,
        mensagem=mensagem,
        erro=erro,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_fluxo_caixa"),
        texto_voltar="← Voltar"
    )


#---------------------------------------------------------
# MENU EMPRÉSTIMOS E FINANCIAMENTOS
#---------------------------------------------------------

@financeiro_bp.route("/emprestimos-financiamentos/menu")
def menu_emprestimos_financiamentos():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_cadastrar_contratos = True
        pode_consultar_emprestimos_financiamentos = True
        pode_registrar_pagamentos_emprestimos_financiamentos = True
    else:
        pode_cadastrar_contratos = usuario_tem_permissao(
            id_usuario,
            cod_empresa,
            "FINANCEIRO",
            "CADASTRO_EMPRESTIMOS_FINANCIAMENTOS"
        )

        pode_consultar_emprestimos_financiamentos = usuario_tem_permissao(
            id_usuario,
            cod_empresa,
            "FINANCEIRO",
            "CONSULTA_EMPRESTIMOS_FINANCIAMENTOS"
        )

        pode_registrar_pagamentos_emprestimos_financiamentos = usuario_tem_permissao(
            id_usuario,
            cod_empresa,
            "FINANCEIRO",
            "REGISTRAR_PAGAMENTOS_EMPRESTIMOS_FINANCIAMENTOS"
        )

    return render_template(
        "menu_emprestimos_financiamentos.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_empresa"),
        pode_cadastrar_contratos=pode_cadastrar_contratos,
        pode_consultar_emprestimos_financiamentos=pode_consultar_emprestimos_financiamentos,
        pode_registrar_pagamentos_emprestimos_financiamentos=pode_registrar_pagamentos_emprestimos_financiamentos
    )

#---------------------------------------------------------
# CADASTRO E CONSULTA DE EMPRÉSTIMOS E FINANCIAMENTOS
#---------------------------------------------------------

@financeiro_bp.route("/emprestimos-financiamentos/cadastro")
def cadastro_emprestimos_financiamentos():
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
                e.id_emprestimo,
                e.codigo,
                e.descricao,
                e.instituicao,
                e.tipo,
                e.valor_contratado,
                e.valor_parcela,
                e.quantidade_parcelas,
                e.meses_carencia,
                e.data_contratacao,
                e.data_primeiro_vencimento,
                e.data_ultima_parcela,
                e.saldo_devedor,
                e.situacao,
                e.ativo,

                COALESCE(p.qtde_parcelas_geradas, 0) AS qtde_parcelas_geradas

            FROM financeiro_emprestimos e

            LEFT JOIN (
                SELECT
                    id_emprestimo,
                    COUNT(*) AS qtde_parcelas_geradas
                FROM financeiro_emprestimos_parcelas
                GROUP BY id_emprestimo
            ) p
                ON p.id_emprestimo = e.id_emprestimo

            WHERE e.cod_empresa = %s

            ORDER BY
                e.ativo DESC,
                e.id_emprestimo DESC
        """, (cod_empresa,))

        contratos = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return render_template(
        "financeiro_emprestimos_cadastro.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        contratos=contratos,
        formatar_numero_br=formatar_numero_br,
        url_voltar=url_for("financeiro.menu_emprestimos_financiamentos")
    )

# =========================================================
# CONSULTA DE EMPRÉSTIMOS E FINANCIAMENTOS
# =================================================

@financeiro_bp.route("/emprestimos-financiamentos/consulta")
def consulta_emprestimos_financiamentos():
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
                e.id_emprestimo,
                e.codigo,
                e.descricao,
                e.instituicao,
                e.tipo,
                e.valor_contratado,
                e.valor_parcela,
                e.quantidade_parcelas,
                e.situacao,
                e.ativo,

                COUNT(p.id_parcela) AS qtde_parcelas_geradas,

                COALESCE(SUM(
                    CASE WHEN p.situacao = 'PAGO'
                    THEN 1 ELSE 0 END
                ), 0) AS parcelas_pagas,

                COALESCE(SUM(
                    CASE WHEN p.situacao = 'PAGO'
                    THEN p.valor_pago ELSE 0 END
                ), 0) AS valor_pago,

                COALESCE(SUM(
                    CASE WHEN p.situacao <> 'PAGO'
                    THEN 1 ELSE 0 END
                ), 0) AS parcelas_restantes,

                COALESCE(SUM(
                    CASE WHEN p.situacao <> 'PAGO'
                    THEN p.valor_parcela ELSE 0 END
                ), 0) AS valor_a_pagar

            FROM financeiro_emprestimos e
            LEFT JOIN financeiro_emprestimos_parcelas p
              ON p.id_emprestimo = e.id_emprestimo
             AND p.cod_empresa = e.cod_empresa

            WHERE e.cod_empresa = %s
              AND e.ativo = TRUE

            GROUP BY
                e.id_emprestimo,
                e.codigo,
                e.descricao,
                e.instituicao,
                e.tipo,
                e.valor_contratado,
                e.valor_parcela,
                e.quantidade_parcelas,
                e.situacao,
                e.ativo

            ORDER BY e.codigo
        """, (cod_empresa,))

        contratos = cur.fetchall()
        total_valor_contratado = sum(float(c["valor_contratado"] or 0) for c in contratos)
        total_valor_pago = sum(float(c["valor_pago"] or 0) for c in contratos)
        total_valor_a_pagar = sum(float(c["valor_a_pagar"] or 0) for c in contratos)

    finally:
        cur.close()
        conn.close()

    return render_template(
        "financeiro_emprestimos_consulta.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        contratos=contratos,
        total_valor_contratado=total_valor_contratado,
        total_valor_pago=total_valor_pago,
        total_valor_a_pagar=total_valor_a_pagar,
        formatar_numero_br=formatar_numero_br,
        url_voltar=url_for("financeiro.menu_emprestimos_financiamentos")
    )

# =========================
# MENU FLUXO DE CAIXA
# =========================
@financeiro_bp.route("/fluxo-caixa/menu")
def menu_fluxo_caixa():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_resultado_mb = True
        pode_resultado_mb_anual = True
        pode_lancamentos = True
        pode_importacoes = True
        pode_matricial = True
        pode_matricial_detalhado = True
        pode_matricial_anual = True
        pode_variacoes = True
        pode_margem_bruta = True
        pode_exclusoes = True
        pode_cr_fiado = True
    else:
        pode_resultado_mb = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "RESULTADO_MB")
        pode_resultado_mb_anual = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "RESULTADO_MB_ANUAL")
        pode_lancamentos = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "LANCAMENTOS")
        pode_importacoes = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "IMPORTACOES")
        pode_matricial = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "MATRICIAL")
        pode_matricial_detalhado = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "MATRICIAL_DETALHADO")
        pode_matricial_anual = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "MATRICIAL_ANUAL")
        pode_variacoes = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "VARIACOES")
        pode_margem_bruta = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "MARGEM_BRUTA")
        pode_exclusoes = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "EXCLUSOES")
        pode_cr_fiado = (
            usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "CR_MENU") or
            usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "CR_FIADO_MENU") or
            usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "CR_CARTOES_MENU")
        )

    return render_template(
        "menu_fluxo_caixa.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        ano_atual=datetime.now().year,
        url_voltar=url_for("financeiro.menu_empresa"),

        pode_resultado_mb=pode_resultado_mb,
        pode_resultado_mb_anual=pode_resultado_mb_anual,
        pode_lancamentos=pode_lancamentos,
        pode_importacoes=pode_importacoes,
        pode_matricial=pode_matricial,
        pode_matricial_detalhado=pode_matricial_detalhado,
        pode_matricial_anual=pode_matricial_anual,
        pode_variacoes=pode_variacoes,
        pode_margem_bruta=pode_margem_bruta,
        pode_exclusoes=pode_exclusoes,
        pode_cr_fiado=pode_cr_fiado,
    )
#---------------------------------------------------------
# NOVO EMPRÉSTIMOS E FINANCIAMENTOS
#---------------------------------------------------------

@financeiro_bp.route("/emprestimos-financiamentos/novo", methods=["GET", "POST"])
def novo_emprestimo_financiamento():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    if request.method == "POST":
        descricao = (request.form.get("descricao") or "").strip()
        if not descricao:
            raise ValueError("Informe a descrição do contrato.")
        instituicao = (request.form.get("instituicao") or "").strip()
        tipo = (request.form.get("tipo") or "").strip()

        data_contratacao = validar_data_contrato(
            request.form.get("data_contratacao"),
            "Data da contratação"
        )

        data_primeiro_vencimento = validar_data_contrato(
            request.form.get("data_primeiro_vencimento"),
            "Primeiro vencimento"
        )

        valor_contratado = converter_numero_br(request.form.get("valor_contratado"))
        valor_parcela = converter_numero_br(request.form.get("valor_parcela"))
        taxa_juros = converter_numero_br(request.form.get("taxa_juros"))

        quantidade_parcelas = int(request.form.get("quantidade_parcelas") or 0)
        meses_carencia = int(request.form.get("meses_carencia") or 0)

        saldo_devedor = valor_contratado
        situacao = request.form.get("situacao") or "ATIVO"
        ativo = True if request.form.get("ativo") == "true" else False
        observacoes = request.form.get("observacoes") or None

        possui_carencia = meses_carencia > 0
        valor_juros_carencia = 0

        conn = get_connection()
        cur = conn.cursor()

        try:
            cur.execute("""
                SELECT COALESCE(MAX(id_emprestimo), 0) + 1
                FROM financeiro_emprestimos
                WHERE cod_empresa = %s
            """, (cod_empresa,))

            proximo_id = cur.fetchone()[0]
            codigo = f"EF{proximo_id:06d}"

            cur.execute("""
                INSERT INTO financeiro_emprestimos (
                    cod_empresa,
                    codigo,
                    descricao,
                    instituicao,
                    tipo,
                    valor_contratado,
                    valor_parcela,
                    taxa_juros,
                    quantidade_parcelas,
                    possui_carencia,
                    meses_carencia,
                    valor_juros_carencia,
                    data_contratacao,
                    data_primeiro_vencimento,
                    saldo_devedor,
                    situacao,
                    observacoes,
                    ativo
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s
                )
                RETURNING id_emprestimo
            """, (
                cod_empresa,
                codigo,
                descricao,
                instituicao,
                tipo,
                valor_contratado,
                valor_parcela,
                taxa_juros,
                quantidade_parcelas,
                possui_carencia,
                meses_carencia,
                valor_juros_carencia,
                data_contratacao,
                data_primeiro_vencimento,
                saldo_devedor,
                situacao,
                observacoes,
                ativo
            ))

            id_emprestimo = cur.fetchone()[0]
            conn.commit()

        except Exception as e:
            conn.rollback()
            cur.close()
            conn.close()
            return f"Erro ao salvar contrato: {str(e)}", 400

        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

        return redirect(url_for("financeiro.cadastro_emprestimos_financiamentos"))

    return render_template(
        "financeiro_emprestimos_form.html",
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
        contrato=None,
        modo="novo",
        url_voltar=url_for("financeiro.cadastro_emprestimos_financiamentos"),
        formatar_numero_br=formatar_numero_br
    )
#---------------------------------------------------------
# EDIÇÃO DE EMPRÉSTIMOS E FINANCIAMENTOS
#---------------------------------------------------------

@financeiro_bp.route("/emprestimos-financiamentos/editar/<int:id_emprestimo>", methods=["GET", "POST"])
def editar_emprestimo_financiamento(id_emprestimo):
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
            instituicao = (request.form.get("instituicao") or "").strip()
            observacoes = request.form.get("observacoes") or None

            cur.execute("""
                SELECT COUNT(*) AS qtde
                FROM financeiro_emprestimos_parcelas
                WHERE id_emprestimo = %s
                AND cod_empresa = %s
            """, (id_emprestimo, cod_empresa))

            qtde_parcelas_geradas = int(cur.fetchone()["qtde"] or 0)

            if qtde_parcelas_geradas > 0:
                cur.execute("""
                    UPDATE financeiro_emprestimos
                       SET descricao = %s,
                           instituicao = %s,
                           observacoes = %s,
                           atualizado_em = now()
                     WHERE id_emprestimo = %s
                       AND cod_empresa = %s
                """, (
                    descricao,
                    instituicao,
                    observacoes,
                    id_emprestimo,
                    cod_empresa
                ))

            else:
                codigo = (request.form.get("codigo") or "").strip()
                tipo = (request.form.get("tipo") or "").strip()

                data_contratacao = validar_data_contrato(
                    request.form.get("data_contratacao"),
                    "Data da contratação"
                )

                data_primeiro_vencimento = validar_data_contrato(
                    request.form.get("data_primeiro_vencimento"),
                    "Primeiro vencimento"
                )

                valor_contratado = converter_numero_br(request.form.get("valor_contratado"))
                valor_parcela = converter_numero_br(request.form.get("valor_parcela"))
                taxa_juros = converter_numero_br(request.form.get("taxa_juros"))

                quantidade_parcelas = int(request.form.get("quantidade_parcelas") or 0)
                meses_carencia = int(request.form.get("meses_carencia") or 0)
                modalidade_calculo = request.form.get("modalidade_calculo") or "PARCELA_FIXA"

                saldo_devedor = valor_contratado
                situacao = request.form.get("situacao") or "ATIVO"
                ativo = True if request.form.get("ativo") == "true" else False
                possui_carencia = meses_carencia > 0

                cur.execute("""
                    UPDATE financeiro_emprestimos
                       SET codigo = %s,
                           descricao = %s,
                           instituicao = %s,
                           tipo = %s,
                           valor_contratado = %s,
                           valor_parcela = %s,
                           taxa_juros = %s,
                           quantidade_parcelas = %s,
                           possui_carencia = %s,
                           meses_carencia = %s,
                           data_contratacao = %s,
                           data_primeiro_vencimento = %s,
                           saldo_devedor = %s,
                           situacao = %s,
                           observacoes = %s,
                           ativo = %s,
                           modalidade_calculo = %s,
                           atualizado_em = now()
                     WHERE id_emprestimo = %s
                       AND cod_empresa = %s
                """, (
                    codigo,
                    descricao,
                    instituicao,
                    tipo,
                    valor_contratado,
                    valor_parcela,
                    taxa_juros,
                    quantidade_parcelas,
                    possui_carencia,
                    meses_carencia,
                    data_contratacao,
                    data_primeiro_vencimento,
                    saldo_devedor,
                    situacao,
                    observacoes,
                    ativo,
                    modalidade_calculo,
                    id_emprestimo,
                    cod_empresa
                ))

            conn.commit()

            return redirect(url_for("financeiro.cadastro_emprestimos_financiamentos"))

        cur.execute("""
            SELECT
                e.id_emprestimo,
                e.cod_empresa,
                e.codigo,
                e.descricao,
                e.instituicao,
                e.tipo,
                e.valor_contratado,
                e.valor_parcela,
                e.taxa_juros,
                e.quantidade_parcelas,
                e.possui_carencia,
                e.meses_carencia,
                e.valor_juros_carencia,
                e.data_contratacao,
                e.data_primeiro_vencimento,
                e.data_ultima_parcela,
                e.saldo_devedor,
                e.situacao,
                e.observacoes,
                e.ativo,
                (
                    SELECT COUNT(*)
                    FROM financeiro_emprestimos_parcelas p
                    WHERE p.id_emprestimo = e.id_emprestimo
                ) AS qtde_parcelas_geradas
            FROM financeiro_emprestimos e
            WHERE e.id_emprestimo = %s
              AND e.cod_empresa = %s
        """, (id_emprestimo, cod_empresa))

        contrato = cur.fetchone()

        if not contrato:
            return "Contrato não encontrado.", 404

    finally:
        cur.close()
        conn.close()

    return render_template(
        "financeiro_emprestimos_form.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        contrato=contrato,
        modo="editar",
        url_voltar=url_for("financeiro.cadastro_emprestimos_financiamentos"),
        formatar_numero_br=formatar_numero_br
    )



#---------------------------------------------------------
# EXCLUSÃO DE EMPRÉSTIMOS E FINANCIAMENTOS
#---------------------------------------------------------

@financeiro_bp.route("/emprestimos-financiamentos/excluir/<int:id_emprestimo>", methods=["POST"])
def excluir_emprestimo_financiamento(id_emprestimo):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT COUNT(*)
            FROM financeiro_emprestimos_parcelas
            WHERE id_emprestimo = %s
        """, (id_emprestimo,))

        qtde_parcelas = cur.fetchone()[0]

        if qtde_parcelas > 0:
            return "Este contrato possui parcelas geradas e não pode ser excluído.", 400

        cur.execute("""
            DELETE FROM financeiro_emprestimos
            WHERE id_emprestimo = %s
              AND cod_empresa = %s
        """, (id_emprestimo, cod_empresa))

        conn.commit()

    except Exception as e:
        conn.rollback()
        return f"Erro ao excluir contrato: {str(e)}", 400

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("financeiro.cadastro_emprestimos_financiamentos"))
#---------------------------------------------------------
# GERAR PARCELAS DO EMPRÉSTIMO / FINANCIAMENTO
#---------------------------------------------------------

@financeiro_bp.route("/emprestimos-financiamentos/gerar-parcelas/<int:id_emprestimo>", methods=["POST"])
def gerar_parcelas_emprestimo_financiamento(id_emprestimo):
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
                id_emprestimo,
                valor_contratado,
                quantidade_parcelas,
                valor_parcela,
                taxa_juros,
                modalidade_calculo,
                data_primeiro_vencimento,
                meses_carencia
            FROM financeiro_emprestimos
            WHERE id_emprestimo = %s
              AND cod_empresa = %s
        """, (id_emprestimo, cod_empresa))

        contrato = cur.fetchone()

        if not contrato:
            return "Contrato não encontrado.", 404

        valor_contratado = float(contrato["valor_contratado"] or 0)
        quantidade_parcelas = int(contrato["quantidade_parcelas"] or 0)
        valor_principal_base = float(contrato["valor_parcela"] or 0)
        taxa_juros = float(contrato["taxa_juros"] or 0)
        modalidade_calculo = contrato["modalidade_calculo"] or "PARCELA_FIXA"
        data_primeiro_vencimento = contrato["data_primeiro_vencimento"]
        meses_carencia = int(contrato["meses_carencia"] or 0)

        if valor_contratado <= 0:
            return "Valor contratado inválido.", 400
        if quantidade_parcelas <= 0:
            return "Quantidade de parcelas inválida.", 400
        if modalidade_calculo == "PARCELA_FIXA" and valor_principal_base <= 0:
            return "Valor da parcela inválido para modalidade Parcela Fixa.", 400
        if not data_primeiro_vencimento:
            return "Informe o primeiro vencimento antes de gerar as parcelas.", 400

        # Segurança: não permite gerar novamente se já existirem parcelas
        cur.execute("""
            SELECT COUNT(*) AS qtde
            FROM financeiro_emprestimos_parcelas
            WHERE id_emprestimo = %s
        """, (id_emprestimo,))
        if int(cur.fetchone()["qtde"] or 0) > 0:
            return "Este contrato já possui parcelas geradas. Exclua as parcelas antes de gerar novamente.", 400

        taxa_mensal = taxa_juros / 100.0
        saldo_atual = valor_contratado

        # Pré-calcula PMT para PRICE
        if modalidade_calculo == "PRICE":
            if taxa_mensal > 0:
                pmt = valor_contratado * taxa_mensal / (1 - (1 + taxa_mensal) ** (-quantidade_parcelas))
            else:
                pmt = valor_contratado / quantidade_parcelas

        # Principal fixo para SAC
        if modalidade_calculo == "SAC":
            principal_sac = valor_contratado / quantidade_parcelas

        # Contador global de sequência (carência + normais)
        seq = 0

        def inserir_parcela(numero, tipo, saldo_ini, principal, juros, total, saldo_fin, taxa):
            nonlocal seq
            seq += 1
            cur.execute("""
                INSERT INTO financeiro_emprestimos_parcelas (
                    id_emprestimo, cod_empresa, numero_parcela, tipo_parcela,
                    data_vencimento, saldo_inicial, valor_principal, valor_juros,
                    valor_parcela, valor_pago, saldo_final, taxa_juros, situacao
                ) VALUES (
                    %s, %s, %s, %s,
                    (%s::date + ((%s - 1) * interval '1 month'))::date,
                    %s, %s, %s, %s, 0, %s, %s, 'EM_ABERTO'
                )
            """, (
                id_emprestimo, cod_empresa, numero, tipo,
                data_primeiro_vencimento, seq,
                saldo_ini, principal, juros, total, saldo_fin, taxa
            ))

        # --- Fase 1: Carência ---
        for m in range(1, meses_carencia + 1):
            if modalidade_calculo == "PARCELA_FIXA":
                juros_car = 0.0
            else:
                juros_car = round(saldo_atual * taxa_mensal, 2)
            inserir_parcela(m, 'CARENCIA', saldo_atual, 0.0, juros_car, juros_car, saldo_atual, taxa_juros)

        # --- Fase 2: Parcelas normais ---
        for i in range(1, quantidade_parcelas + 1):
            numero = meses_carencia + i
            saldo_inicial = saldo_atual

            if modalidade_calculo == "PARCELA_FIXA":
                valor_principal = min(valor_principal_base, saldo_inicial)
                valor_juros = 0.0
                taxa_parcela = 0.0

            elif modalidade_calculo == "PRICE":
                valor_juros = round(saldo_atual * taxa_mensal, 2)
                valor_principal = round(pmt - valor_juros, 2)
                if valor_principal > saldo_atual:
                    valor_principal = round(saldo_atual, 2)
                taxa_parcela = taxa_juros

            elif modalidade_calculo == "SAC":
                valor_principal = round(min(principal_sac, saldo_atual), 2)
                valor_juros = round(saldo_atual * taxa_mensal, 2)
                taxa_parcela = taxa_juros

            elif modalidade_calculo in ("PARCELA_INFORMADA", "JUROS_VARIAVEIS"):
                valor_principal = 0.0
                valor_juros = 0.0
                taxa_parcela = taxa_juros

            else:
                valor_principal = min(valor_principal_base, saldo_inicial)
                valor_juros = 0.0
                taxa_parcela = 0.0

            valor_parcela_total = round(valor_principal + valor_juros, 2)
            saldo_final = round(saldo_inicial - valor_principal, 2)

            inserir_parcela(numero, 'NORMAL', saldo_inicial, valor_principal,
                            valor_juros, valor_parcela_total, saldo_final, taxa_parcela)

            saldo_atual = saldo_final
            if saldo_atual <= 0:
                break

        conn.commit()

    except Exception as e:
        conn.rollback()
        return f"Erro ao gerar parcelas: {str(e)}", 400

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("financeiro.cadastro_emprestimos_financiamentos"))

#---------------------------------------------------------
# VISUALIZAR PARCELAS DO EMPRÉSTIMO / FINANCIAMENTO
#---------------------------------------------------------

@financeiro_bp.route("/emprestimos-financiamentos/parcelas/<int:id_emprestimo>")
def visualizar_parcelas_emprestimo_financiamento(id_emprestimo):
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
                id_emprestimo,
                codigo,
                descricao,
                instituicao,
                tipo,
                valor_contratado,
                valor_parcela,
                taxa_juros,
                quantidade_parcelas,
                possui_carencia,
                meses_carencia,
                valor_juros_carencia,
                data_contratacao,
                data_primeiro_vencimento,
                data_ultima_parcela,
                saldo_devedor,
                situacao,
                observacoes,
                ativo,
                COALESCE(modalidade_calculo, 'PARCELA_FIXA') AS modalidade_calculo
            FROM financeiro_emprestimos
            WHERE id_emprestimo = %s
              AND cod_empresa = %s
        """, (id_emprestimo, cod_empresa))

        contrato = cur.fetchone()

        if not contrato:
            return "Contrato não encontrado.", 404

        cur.execute("""
            SELECT
                id_parcela,
                numero_parcela,
                tipo_parcela,
                data_vencimento,
                saldo_inicial,
                valor_principal,
                valor_juros,
                valor_parcela,
                valor_pago,
                saldo_final,
                taxa_juros,
                data_pagamento,
                situacao,
                observacao
            FROM financeiro_emprestimos_parcelas
            WHERE id_emprestimo = %s
            ORDER BY numero_parcela, data_vencimento
        """, (id_emprestimo,))

        parcelas = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    modalidade = contrato["modalidade_calculo"] if contrato else "PARCELA_FIXA"
    editavel = modalidade in ("PARCELA_INFORMADA", "JUROS_VARIAVEIS")

    return render_template(
        "financeiro_emprestimos_parcelas.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        contrato=contrato,
        parcelas=parcelas,
        hoje=date.today(),
        formatar_numero_br=formatar_numero_br,
        url_voltar=url_for("financeiro.cadastro_emprestimos_financiamentos"),
        editavel=editavel,
        modalidade=modalidade,
    )

#---------------------------------------------------------
# EXCLUIR TODAS AS PARCELAS DO CONTRATO
#---------------------------------------------------------

@financeiro_bp.route("/emprestimos-financiamentos/parcelas/excluir-todas/<int:id_emprestimo>", methods=["POST"])
def excluir_todas_parcelas_emprestimo_financiamento(id_emprestimo):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT COUNT(*)
            FROM financeiro_emprestimos
            WHERE id_emprestimo = %s
              AND cod_empresa = %s
        """, (id_emprestimo, cod_empresa))

        if cur.fetchone()[0] == 0:
            return "Contrato não encontrado.", 404

        cur.execute("""
            SELECT COUNT(*)
            FROM financeiro_emprestimos_parcelas
            WHERE id_emprestimo = %s
              AND situacao <> 'EM_ABERTO'
        """, (id_emprestimo,))

        qtde_nao_abertas = cur.fetchone()[0]

        if qtde_nao_abertas > 0:
            return "Não é possível excluir as parcelas. Existem parcelas que não estão em aberto.", 400

        cur.execute("""
            DELETE FROM financeiro_emprestimos_parcelas
            WHERE id_emprestimo = %s
        """, (id_emprestimo,))

        conn.commit()

    except Exception as e:
        conn.rollback()
        return f"Erro ao excluir parcelas: {str(e)}", 400

    finally:
        cur.close()
        conn.close()

    return redirect(url_for(
        "financeiro.visualizar_parcelas_emprestimo_financiamento",
        id_emprestimo=id_emprestimo
    ))

#---------------------------------------------------------
# EDITAR PARCELA INDIVIDUAL (PARCELA_INFORMADA / JUROS_VARIAVEIS)
#---------------------------------------------------------

@financeiro_bp.route("/emprestimos-financiamentos/parcelas/editar/<int:id_parcela>", methods=["POST"])
def editar_parcela_emprestimo_financiamento(id_parcela):
    if "id_usuario" not in session or "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    def conv(v):
        try:
            return float((v or "0").replace(".", "").replace(",", "."))
        except ValueError:
            return 0.0

    valor_principal = conv(request.form.get("valor_principal"))
    valor_juros     = conv(request.form.get("valor_juros"))
    valor_parcela   = round(valor_principal + valor_juros, 2)
    id_emprestimo   = int(request.form.get("id_emprestimo") or 0)

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Busca saldo_inicial da parcela anterior para calcular saldo_final
        cur.execute("""
            SELECT p.saldo_inicial,
                   (SELECT p2.saldo_final FROM financeiro_emprestimos_parcelas p2
                    WHERE p2.id_emprestimo = p.id_emprestimo
                      AND p2.numero_parcela < p.numero_parcela
                      AND p2.tipo_parcela = 'NORMAL'
                    ORDER BY p2.numero_parcela DESC LIMIT 1) AS saldo_anterior
            FROM financeiro_emprestimos_parcelas p
            WHERE p.id_parcela = %s AND p.id_emprestimo IN (
                SELECT id_emprestimo FROM financeiro_emprestimos WHERE cod_empresa = %s
            )
        """, (id_parcela, cod_empresa))
        row = cur.fetchone()
        if not row:
            return "Parcela não encontrada.", 404

        saldo_ini = float(row["saldo_anterior"] or row["saldo_inicial"] or 0)
        saldo_fin = round(saldo_ini - valor_principal, 2)

        cur.execute("""
            UPDATE financeiro_emprestimos_parcelas
               SET valor_principal = %s,
                   valor_juros     = %s,
                   valor_parcela   = %s,
                   saldo_inicial   = %s,
                   saldo_final     = %s
             WHERE id_parcela = %s
        """, (valor_principal, valor_juros, valor_parcela, saldo_ini, saldo_fin, id_parcela))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return f"Erro: {str(e)}", 400
    finally:
        cur.close()
        conn.close()

    return redirect(url_for("financeiro.visualizar_parcelas_emprestimo_financiamento",
                            id_emprestimo=id_emprestimo))


#---------------------------------------------------------
# PAGAMENTOS DE PARCELAS DE EMPRÉSTIMOS / FINANCIAMENTOS
#---------------------------------------------------------

@financeiro_bp.route("/emprestimos-financiamentos/pagamentos", methods=["GET", "POST"])
def pagamentos_emprestimos_financiamentos():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    hoje = datetime.now()

    ano_sel = int(request.values.get("ano") or hoje.year)
    mes_sel = int(request.values.get("mes") or hoje.month)

    mensagem = ""
    erro = ""

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            try:
                id_parcela = int(request.form.get("id_parcela") or 0)
                valor_pago = converter_numero_br(request.form.get("valor_pago"))
                data_pagamento = request.form.get("data_pagamento") or None

                if id_parcela <= 0:
                    raise ValueError("Selecione uma parcela.")

                if valor_pago <= 0:
                    raise ValueError("Informe o valor pago.")

                if not data_pagamento:
                    raise ValueError("Informe a data de pagamento.")

                cur.execute("""
                    SELECT
                        p.id_parcela,
                        p.id_emprestimo,
                        p.valor_principal,
                        p.valor_juros,
                        p.valor_parcela
                    FROM financeiro_emprestimos_parcelas p
                    JOIN financeiro_emprestimos e
                    ON e.id_emprestimo = p.id_emprestimo
                    WHERE p.id_parcela = %s
                    AND e.cod_empresa = %s
                    AND p.situacao = 'EM_ABERTO'
                    AND date_trunc('month', p.data_vencimento)
                        <= date_trunc('month', CURRENT_DATE)
                """, (id_parcela, cod_empresa))

                parcela = cur.fetchone()

                if not parcela:
                    raise ValueError("Parcela não encontrada, já paga ou com vencimento futuro.")

                cur.execute("""
                    INSERT INTO financeiro_emprestimos_pagamentos (
                        id_emprestimo,
                        id_parcela,
                        data_pagamento,
                        valor_principal_pago,
                        valor_juros_pago,
                        valor_pago,
                        observacao
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    parcela["id_emprestimo"],
                    parcela["id_parcela"],
                    data_pagamento,
                    parcela["valor_principal"] or 0,
                    parcela["valor_juros"] or 0,
                    valor_pago,
                    request.form.get("observacao") or None
                ))

                cur.execute("""
                    UPDATE financeiro_emprestimos_parcelas
                    SET valor_pago = %s,
                        data_pagamento = %s,
                        situacao = 'PAGO',
                        atualizado_em = now()
                    WHERE id_parcela = %s
                    AND situacao = 'EM_ABERTO'
                """, (
                    valor_pago,
                    data_pagamento,
                    id_parcela
                ))

                conn.commit()
                mensagem = "Pagamento registrado com sucesso."

            except Exception as e:
                conn.rollback()
                erro = str(e)

        # Contratos ativos com parcelas em aberto
        cur.execute("""
            SELECT
                e.id_emprestimo,
                e.codigo,
                e.descricao,
                e.instituicao,
                p.id_parcela,
                p.numero_parcela,
                p.data_vencimento,
                p.valor_parcela
            FROM financeiro_emprestimos e
            JOIN LATERAL (
                SELECT
                    id_parcela,
                    numero_parcela,
                    data_vencimento,
                    valor_parcela
                FROM financeiro_emprestimos_parcelas
                WHERE id_emprestimo = e.id_emprestimo
                AND situacao = 'EM_ABERTO'
                AND date_trunc('month', data_vencimento)
                    <= date_trunc('month', CURRENT_DATE)
                ORDER BY numero_parcela
                LIMIT 1
            ) p ON TRUE
            WHERE e.cod_empresa = %s
              AND e.ativo = TRUE
            ORDER BY e.codigo
        """, (cod_empresa,))

        contratos = cur.fetchall()

        # Pagamentos já realizados no mês/ano filtrado
        cur.execute("""
            SELECT
                pg.id_pagamento,
                e.codigo,
                e.descricao,
                e.instituicao,
                p.numero_parcela,
                p.data_vencimento,
                p.valor_parcela,
                pg.valor_pago,
                pg.data_pagamento,
                p.situacao
            FROM financeiro_emprestimos_pagamentos pg
            JOIN financeiro_emprestimos_parcelas p
            ON p.id_parcela = pg.id_parcela
            JOIN financeiro_emprestimos e
            ON e.id_emprestimo = pg.id_emprestimo
            WHERE e.cod_empresa = %s
            AND EXTRACT(YEAR FROM pg.data_pagamento) = %s
            AND EXTRACT(MONTH FROM pg.data_pagamento) = %s
            ORDER BY pg.data_pagamento DESC, e.codigo, p.numero_parcela
        """, (
            cod_empresa,
            ano_sel,
            mes_sel
        ))

        pagamentos = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return render_template(
        "financeiro_emprestimos_pagamentos.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        contratos=contratos,
        pagamentos=pagamentos,
        ano_sel=ano_sel,
        mes_sel=mes_sel,
        mensagem=mensagem,
        erro=erro,
        hoje=hoje.date(),
        formatar_numero_br=formatar_numero_br,
        url_voltar=url_for("financeiro.menu_emprestimos_financiamentos")
    )

#---------------------------------------------------------
# EXCLUIR PAGAMENTO DE PARCELA
#---------------------------------------------------------

@financeiro_bp.route("/emprestimos-financiamentos/pagamentos/excluir/<int:id_pagamento>", methods=["POST"])
def excluir_pagamento_emprestimo_financiamento(id_pagamento):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    hoje = datetime.now()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT
                pg.id_pagamento,
                pg.id_parcela,
                pg.data_pagamento
            FROM financeiro_emprestimos_pagamentos pg
            JOIN financeiro_emprestimos e
              ON e.id_emprestimo = pg.id_emprestimo
            WHERE pg.id_pagamento = %s
              AND e.cod_empresa = %s
        """, (id_pagamento, cod_empresa))

        pagamento = cur.fetchone()

        if not pagamento:
            raise ValueError("Pagamento não encontrado.")

        if pagamento["data_pagamento"].year != hoje.year or pagamento["data_pagamento"].month != hoje.month:
            raise ValueError("Só é permitido excluir pagamentos do mês atual.")

        cur.execute("""
            DELETE FROM financeiro_emprestimos_pagamentos
            WHERE id_pagamento = %s
        """, (id_pagamento,))

        cur.execute("""
            UPDATE financeiro_emprestimos_parcelas
               SET valor_pago = 0,
                   data_pagamento = NULL,
                   situacao = 'EM_ABERTO',
                   atualizado_em = now()
             WHERE id_parcela = %s
        """, (pagamento["id_parcela"],))

        conn.commit()

    except Exception as e:
        conn.rollback()
        return f"Erro ao excluir pagamento: {str(e)}", 400

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("financeiro.pagamentos_emprestimos_financiamentos"))

#---------------------------------------------------------
# CONSULTA FLUXO DE PAGAMENTOS DE EMPRÉSTIMOS / FINANCIAMENTOS
#---------------------------------------------------------

@financeiro_bp.route("/emprestimos-financiamentos/fluxo-pagamentos")
def consulta_fluxo_pagamentos_emprestimos_financiamentos():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    ano_atual = datetime.now().year
    mes_atual = datetime.now().month

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT
                e.id_emprestimo,
                e.codigo,
                e.descricao,
                e.instituicao,
                EXTRACT(YEAR FROM p.data_vencimento)::int AS ano,
                COALESCE(SUM(p.valor_parcela - p.valor_pago), 0) AS valor_aberto
            FROM financeiro_emprestimos_parcelas p
            JOIN financeiro_emprestimos e
              ON e.id_emprestimo = p.id_emprestimo
            WHERE e.cod_empresa = %s
             AND e.ativo = TRUE
             AND COALESCE(p.valor_parcela, 0) > COALESCE(p.valor_pago, 0)
            GROUP BY
                e.id_emprestimo,
                e.codigo,
                e.descricao,
                e.instituicao,
                EXTRACT(YEAR FROM p.data_vencimento)
            ORDER BY
                e.codigo,
                ano
        """, (cod_empresa,))

        registros = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    anos = sorted({int(r["ano"]) for r in registros})

    linhas_mapa = {}

    for r in registros:
        id_emprestimo = r["id_emprestimo"]
        ano = int(r["ano"])
        valor = float(r["valor_aberto"] or 0)

        if id_emprestimo not in linhas_mapa:
            linhas_mapa[id_emprestimo] = {
                "codigo": r["codigo"],
                "descricao": r["descricao"],
                "instituicao": r["instituicao"],
                "valores": {a: 0 for a in anos},
                "total": 0
            }

        linhas_mapa[id_emprestimo]["valores"][ano] = valor
        linhas_mapa[id_emprestimo]["total"] += valor

    linhas = list(linhas_mapa.values())

    totais_por_ano = {ano: 0 for ano in anos}
    total_geral = 0

    for linha in linhas:
        for ano in anos:
            valor = linha["valores"].get(ano, 0)
            totais_por_ano[ano] += valor
            total_geral += valor

    media_mensal_por_ano = {}

    for ano in anos:
        if ano == ano_atual:
            meses_restantes = 13 - mes_atual
        else:
            meses_restantes = 12

        if meses_restantes <= 0:
            meses_restantes = 12

        media_mensal_por_ano[ano] = (
            totais_por_ano[ano] / meses_restantes
            if totais_por_ano[ano] else 0
        )

    media_mensal_total = (
        total_geral / sum(
            13 - mes_atual if ano == ano_atual else 12
            for ano in anos
        )
        if anos else 0
    )

    return render_template(
        "financeiro_emprestimos_fluxo_pagamentos.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        anos=anos,
        linhas=linhas,
        totais_por_ano=totais_por_ano,
        total_geral=total_geral,
        media_mensal_por_ano=media_mensal_por_ano,
        media_mensal_total=media_mensal_total,
        formatar_numero_br=formatar_numero_br,
        url_voltar=url_for("financeiro.menu_emprestimos_financiamentos")
    )

#---------------------------------------------------------
# FLUXO DE CAIXA PROJETADO
#---------------------------------------------------------

@financeiro_bp.route("/fluxo-caixa-projetado")
def fluxo_caixa_projetado():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    hoje = date.today()
    ano_atual = hoje.year
    mes_atual = hoje.month

    try:
        ano_sel = int(request.args.get("ano") or ano_atual)
    except ValueError:
        ano_sel = ano_atual

    ano_aberto = (ano_sel == ano_atual)
    meses = list(range(1, 13))

    meses_disponiveis = mes_atual - 1  # meses concluídos no ano
    try:
        qtd_meses = int(request.args.get("qtd_meses") or meses_disponiveis)
        qtd_meses = max(1, min(qtd_meses, meses_disponiveis)) if meses_disponiveis > 0 else 1
    except ValueError:
        qtd_meses = meses_disponiveis or 1

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT
                l.grupo,
                COALESCE(gg.descricao, '') AS nome_grupo,
                l.conta,
                COALESCE(NULLIF(TRIM(l.descricao_conta), ''), 'SEM DESCRIÇÃO') AS descricao,
                l.mes,
                COALESCE(SUM(l.valor), 0) AS valor
            FROM lancamentos l
            LEFT JOIN grupos_gerenciais gg
              ON gg.cod_grupo = l.grupo
            WHERE l.cod_empresa = %s
              AND l.ano = %s
              AND l.grupo IS NOT NULL
              AND l.mes IS NOT NULL
            GROUP BY l.grupo, gg.descricao, l.conta,
                COALESCE(NULLIF(TRIM(l.descricao_conta), ''), 'SEM DESCRIÇÃO'),
                l.mes
            ORDER BY l.grupo, l.conta, descricao, l.mes
        """, (cod_empresa, ano_sel))
        registros = cur.fetchall()

        cur.execute("""
            SELECT DISTINCT grupo
            FROM lancamentos
            WHERE cod_empresa = %s AND grupo IS NOT NULL
            ORDER BY grupo
        """, (cod_empresa,))
        todos_grupos = [str(r["grupo"]) for r in cur.fetchall()]

        # Contas que o usuário desmarcou: não viram média nos meses futuros.
        # Guardado no cadastro de contas, então vale para os dois tipos de
        # análise e persiste entre acessos.
        cur.execute("""
            SELECT cod_grupo, cod_conta
            FROM contas_gerenciais
            WHERE cod_empresa = %s AND projetar = FALSE
        """, (cod_empresa,))
        contas_sem_projecao = {(str(r["cod_grupo"]), r["cod_conta"]) for r in cur.fetchall()}

        cur.execute("""
            SELECT mes, COALESCE(SUM(margem_bruta), 0) AS mb
            FROM vendas_mb_sintetico
            WHERE cod_empresa = %s AND ano = %s
            GROUP BY mes
            ORDER BY mes
        """, (cod_empresa, ano_sel))
        mb_registros = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    # Montar MB por mês com projeção
    mb_vals = {int(r["mes"]): float(r["mb"] or 0) for r in mb_registros}
    mes_inicio_media = mes_atual - qtd_meses
    proj_mb = (sum(mb_vals.get(m, 0) for m in range(mes_inicio_media, mes_atual)) / qtd_meses) if qtd_meses > 0 else 0.0

    mb_por_mes = {}
    for m in meses:
        if ano_aberto and m >= mes_atual:
            mb_por_mes[m] = {"valor": proj_mb, "projetado": True}
        else:
            mb_por_mes[m] = {"valor": mb_vals.get(m, 0), "projetado": False}
    total_mb = sum(v["valor"] for v in mb_por_mes.values())

    # Montar estrutura por grupo > conta > mês
    grupos_mapa = {}
    for r in registros:
        g = str(r["grupo"])
        conta = r["conta"]
        desc = r["descricao"]
        nome_grupo = r["nome_grupo"] or ""
        m = int(r["mes"])
        v = float(r["valor"] or 0)

        if g not in grupos_mapa:
            grupos_mapa[g] = {"grupo": g, "nome_grupo": nome_grupo, "contas": {}}

        if conta not in grupos_mapa[g]["contas"]:
            grupos_mapa[g]["contas"][conta] = {
                "cod_conta": conta,
                "descricao": desc,
                "valores": {mes: 0 for mes in meses}
            }
        else:
            # Atualiza descrição para a mais recente encontrada
            grupos_mapa[g]["contas"][conta]["descricao"] = desc

        grupos_mapa[g]["contas"][conta]["valores"][m] += v

    # Calcular projeção para meses futuros (ano aberto)
    def calcular_projecao(valores_mes):
        if qtd_meses <= 0:
            return 0.0
        total = sum(valores_mes.get(m, 0) for m in range(mes_inicio_media, mes_atual))
        return total / qtd_meses

    grupos = []
    for g in sorted(grupos_mapa.keys(), key=lambda x: int(x) if x.isdigit() else 999):
        info = grupos_mapa[g]
        contas = []

        for chave in sorted(info["contas"].keys(), key=lambda x: (x if x is not None else 999)):
            conta_info = info["contas"][chave]
            vals = conta_info["valores"]
            proj = calcular_projecao(vals) if ano_aberto else None
            projetar = (g, conta_info["cod_conta"]) not in contas_sem_projecao
            valores_exibir = {}
            for m in meses:
                if ano_aberto and m >= mes_atual:
                    # zero sai em branco na tela e não soma nos totais
                    valor_proj = 0.0 if (g == "7" or not projetar) else proj
                    valores_exibir[m] = {"valor": valor_proj, "projetado": True}
                else:
                    valores_exibir[m] = {"valor": vals.get(m, 0), "projetado": False}
            total = sum(v["valor"] for v in valores_exibir.values())
            contas.append({
                "cod_conta": conta_info["cod_conta"],
                "descricao": conta_info["descricao"],
                "projetar": projetar,
                "valores": valores_exibir,
                "total": total
            })

        # Total do grupo por mês
        total_grupo_mes = {}
        for m in meses:
            total_grupo_mes[m] = {
                "valor": sum(c["valores"][m]["valor"] for c in contas),
                "projetado": ano_aberto and m >= mes_atual
            }
        total_grupo = sum(v["valor"] for v in total_grupo_mes.values())
        grupos.append({
            "grupo": g,
            "nome_grupo": grupos_mapa[g]["nome_grupo"],
            "contas": contas,
            "total_mes": total_grupo_mes,
            "total": total_grupo
        })

    # Total geral por mês (todos os grupos — modo fluxo)
    totais_mes = {}
    for m in meses:
        totais_mes[m] = {
            "valor": sum(g["total_mes"][m]["valor"] for g in grupos),
            "projetado": ano_aberto and m >= mes_atual
        }
    total_geral = sum(v["valor"] for v in totais_mes.values())

    # Saldo MB: MB + grupos 4, 5, 6
    grupos_mb = [g for g in grupos if g["grupo"] in ("4", "5", "6")]
    totais_mes_mb = {}
    for m in meses:
        valor_grupos = sum(g["total_mes"][m]["valor"] for g in grupos_mb)
        totais_mes_mb[m] = {
            "valor": mb_por_mes[m]["valor"] + valor_grupos,
            "projetado": ano_aberto and m >= mes_atual
        }
    total_saldo_mb = sum(v["valor"] for v in totais_mes_mb.values())

    nomes_meses = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]

    return render_template(
        "fluxo_caixa_projetado.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        ano_sel=ano_sel,
        ano_aberto=ano_aberto,
        mes_atual=mes_atual,
        qtd_meses=qtd_meses,
        meses_disponiveis=meses_disponiveis,
        meses=meses,
        nomes_meses=nomes_meses,
        grupos=grupos,
        todos_grupos=todos_grupos,
        totais_mes=totais_mes,
        total_geral=total_geral,
        mb_por_mes=mb_por_mes,
        totais_mes_mb=totais_mes_mb,
        total_saldo_mb=total_saldo_mb,
        total_mb=total_mb,
        formatar_numero_br=formatar_numero_br,
        url_voltar=url_for("financeiro.menu_empresa")
    )


@financeiro_bp.route("/api/contas-projecao", methods=["PUT"])
def api_marcar_conta_projecao():
    """Liga/desliga a conta na projeção, direto do grid do Fluxo Projetado.
    Grava no cadastro de contas, então a marcação fica memorizada e vale
    também para a tela de Contas Gerenciais."""
    if "cod_empresa" not in session:
        return jsonify({"ok": False, "erro": "Sessão expirada."}), 401

    cod_empresa = str(session["cod_empresa"]).strip()
    dados = request.get_json(silent=True) or {}

    try:
        cod_grupo = int(dados["cod_grupo"])
        cod_conta = int(dados["cod_conta"])
    except (KeyError, TypeError, ValueError):
        return jsonify({"ok": False, "erro": "Informe cod_grupo e cod_conta."}), 400

    projetar = bool(dados.get("projetar", True))

    conn = get_connection()
    cur = conn.cursor()
    try:
        # contas_gerenciais é alimentada pela importação e tem trigger que
        # bloqueia inclusão — aqui só atualiza o que já existe no cadastro
        cur.execute("""
            UPDATE contas_gerenciais
            SET projetar = %s
            WHERE cod_empresa = %s
              AND cod_grupo = %s
              AND cod_conta = %s
        """, (projetar, cod_empresa, cod_grupo, cod_conta))

        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({
                "ok": False,
                "erro": f"Conta {cod_grupo}-{cod_conta} não encontrada no cadastro de contas gerenciais.",
            }), 404

        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "projetar": projetar})


#---------------------------------------------------------
# SALDOS (CONCILIAÇÃO BANCÁRIA)
#---------------------------------------------------------

# =========================
# TELAS
# =========================

@financeiro_bp.route("/caixas")
@permissao_obrigatoria("FINANCEIRO", "MENU_CAIXAS",
                       redirecionar_para="financeiro.menu_empresa")
def menu_caixas():
    if "id_usuario" not in session or "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    id_usuario = session["id_usuario"]
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_atualizar_caixas = True
        pode_configuracoes_caixas = True
        pode_acessos_caixas = True
    else:
        pode_atualizar_caixas = usuario_tem_permissao(
            id_usuario, cod_empresa, "FINANCEIRO", "ATUALIZAR_CAIXAS"
        )
        pode_configuracoes_caixas = usuario_tem_permissao(
            id_usuario, cod_empresa, "FINANCEIRO", "CONFIGURACOES_CAIXAS"
        )
        pode_acessos_caixas = usuario_tem_permissao(
            id_usuario, cod_empresa, "FINANCEIRO", "ACESSOS_CAIXAS"
        )

    return render_template(
        "menu_caixas.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_empresa"),
        pode_atualizar_caixas=pode_atualizar_caixas,
        pode_configuracoes_caixas=pode_configuracoes_caixas,
        pode_acessos_caixas=pode_acessos_caixas,
    )


@financeiro_bp.route("/caixas/acessos", methods=["GET", "POST"])
@permissao_obrigatoria("FINANCEIRO", "ACESSOS_CAIXAS",
                       redirecionar_para="financeiro.menu_caixas")
def acessos_caixas():
    """Define, por usuário e por área, quem consulta e quem altera os caixas.

    Três colunas por usuário: o resumo de todas as áreas (só consulta) e,
    para cada área, consultar e alterar. Alterar implica consultar — o banco
    também garante isso. Superusuário não aparece na lista: já pode tudo.
    """
    if "id_usuario" not in session or "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id_area, nome_area FROM areas
             WHERE cod_empresa=%s AND ativo = TRUE
             ORDER BY nome_area
        """, (cod_empresa,))
        areas = cur.fetchall()

        # Só entram na lista os usuários que já alcançam o módulo de caixas.
        # Quem não tem nenhuma dessas permissões não teria como usar o acesso
        # concedido aqui, então só poluiria a tela.
        cur.execute("""
            SELECT u.id_usuario, u.nome, u.email
              FROM usuarios u
              JOIN usuarios_empresas ue
                ON ue.id_usuario = u.id_usuario AND ue.cod_empresa = %s
             WHERE ue.ativo = TRUE AND u.ativo = TRUE
               AND COALESCE(LOWER(u.tipo_global), '') <> 'superusuario'
               AND EXISTS (
                     SELECT 1 FROM usuarios_permissoes up
                      WHERE up.id_usuario = u.id_usuario
                        AND up.cod_empresa = %s
                        AND up.ativo = TRUE
                        AND up.sistema = 'FINANCEIRO'
                        AND up.opcao IN ('MENU_CAIXAS', 'ATUALIZAR_CAIXAS',
                                         'CONFIGURACOES_CAIXAS', 'ACESSOS_CAIXAS')
                   )
             ORDER BY u.nome
        """, (cod_empresa, cod_empresa))
        usuarios = cur.fetchall()

        if request.method == "POST":
            # A tela manda o estado inteiro. Em vez de sair lendo as chaves
            # que vierem no formulário, percorro a mesma matriz que montei
            # para exibir: assim nada de fora entra e nada de dentro escapa.
            cur.execute("DELETE FROM caixas_acessos WHERE cod_empresa=%s", (cod_empresa,))

            linhas = []
            for u in usuarios:
                uid = u["id_usuario"]

                # resumo de todas as áreas: só consulta
                if f"consultar-{uid}-todas" in request.form:
                    linhas.append((cod_empresa, uid, None, True, False))

                for a in areas:
                    aid = a["id_area"]
                    alterar = f"alterar-{uid}-{aid}" in request.form
                    # alterar implica consultar, mesmo que a caixa de
                    # consultar não tenha vindo marcada
                    consultar = alterar or f"consultar-{uid}-{aid}" in request.form
                    if consultar:
                        linhas.append((cod_empresa, uid, aid, True, alterar))

            if linhas:
                cur.executemany("""
                    INSERT INTO caixas_acessos
                        (cod_empresa, id_usuario, id_area, pode_consultar, pode_alterar)
                    VALUES (%s, %s, %s, %s, %s)
                """, linhas)

            conn.commit()
            flash(f"Acessos atualizados: {len(linhas)} concessões gravadas.", "success")
            return redirect(url_for("financeiro.acessos_caixas"))

        cur.execute("""
            SELECT id_usuario, id_area, pode_consultar, pode_alterar
              FROM caixas_acessos WHERE cod_empresa=%s
        """, (cod_empresa,))
        acessos = {(r["id_usuario"], r["id_area"]): r for r in cur.fetchall()}
    finally:
        cur.close()
        conn.close()

    return render_template(
        "acessos_caixas.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_caixas"),
        areas=areas,
        usuarios=usuarios,
        acessos=acessos,
    )


# Janela de digitação do caixa. Por convenção da operação, o caixa que se
# confere hoje é o de ontem, mas a digitação do próprio dia de hoje também é
# liberada. Data futura nunca pode ser digitada, e mais de três dias atrás já
# se considera fechado. Sobram hoje, ontem, anteontem e o dia anterior a
# anteontem.
DIAS_EDICAO_CAIXA = 3


def _liberacoes_caixa_vigentes(cod_empresa):
    """Todas as liberações temporárias ainda dentro do prazo, mais recentes
    primeiro.

    As liberações são **acumulativas**: cada linha abre um período fechado
    (desde/até) para uma filial ou para todas (`cod_filial` nulo), e todas as
    que ainda não expiraram valem ao mesmo tempo. Antes só a última linha era
    lida (`ORDER BY ativado_em DESC LIMIT 1`), então liberar uma segunda data
    cancelava a primeira sem avisar — era isso que fazia a liberação "não
    funcionar" depois de vários cliques.

    Não há rotina de expiração: quem expira é o relógio, na leitura.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT cl.id_liberacao, cl.data_liberada_desde, cl.data_liberada_ate,
                   cl.cod_filial, cl.ativado_em, u.nome AS ativado_por,
                   f.nome_filial
            FROM caixas_liberacao_temporaria cl
            LEFT JOIN usuarios u ON u.id_usuario = cl.id_usuario_ativou
            LEFT JOIN filiais f ON f.cod_empresa = cl.cod_empresa
                               AND f.cod_filial = cl.cod_filial
            WHERE cl.cod_empresa = %s
              AND cl.revogado_em IS NULL
              AND cl.ativado_em >= (NOW() AT TIME ZONE 'UTC') - %s::interval
            ORDER BY cl.ativado_em DESC
        """, (cod_empresa, f"{HORAS_LIBERACAO_CAIXAS} hours"))
        linhas = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    agora = _agora_local()
    vigentes = []
    for lib in linhas:
        expira_em = (_ativado_em_local(lib["ativado_em"])
                     + timedelta(hours=HORAS_LIBERACAO_CAIXAS))
        if agora >= expira_em:
            continue
        lib["expira_em"] = expira_em
        vigentes.append(lib)
    return vigentes


def _liberacao_cobre(lib, d, cod_filial=None):
    """A liberação abrange esta data nesta filial?

    `cod_filial` nulo na liberação = todas as filiais. `cod_filial` nulo na
    pergunta (tela que ainda não escolheu filial) aceita qualquer liberação —
    a trava por filial que vale é a dos endpoints de gravação, onde a filial
    é sempre conhecida.
    """
    if not (lib["data_liberada_desde"] <= d <= lib["data_liberada_ate"]):
        return False
    if lib["cod_filial"] is None or cod_filial is None:
        return True
    return int(lib["cod_filial"]) == int(cod_filial)


def _janela_edicao_caixa(hoje=None, cod_empresa=None):
    """(primeiro_dia, ultimo_dia) da janela **normal** de digitação.

    As datas abertas por liberação temporária não entram aqui: elas podem ser
    períodos soltos, um por filial, e não cabem num intervalo único. Quem
    precisa delas usa `_datas_liberadas_caixa`.
    """
    hoje = hoje or _hoje_local()
    return (hoje - timedelta(days=DIAS_EDICAO_CAIXA), hoje)


def _datas_liberadas_caixa(cod_empresa=None, cod_filial=None):
    """Conjunto de datas extras abertas por liberação temporária vigente."""
    if cod_empresa is None:
        cod_empresa = str(session.get("cod_empresa") or "").strip()
    if not cod_empresa:
        return set()

    datas = set()
    for lib in _liberacoes_caixa_vigentes(cod_empresa):
        if cod_filial is not None and lib["cod_filial"] is not None \
                and int(lib["cod_filial"]) != int(cod_filial):
            continue
        d = lib["data_liberada_desde"]
        while d <= lib["data_liberada_ate"]:
            datas.add(d)
            d += timedelta(days=1)
    return datas


def _data_caixa_editavel(data_str, hoje=None, cod_empresa=None, cod_filial=None):
    """A data pode receber digitação? Vale para a grade e para o detalhamento."""
    try:
        d = data_str if isinstance(data_str, date) else date.fromisoformat(str(data_str)[:10])
    except (TypeError, ValueError):
        return False

    ini, fim = _janela_edicao_caixa(hoje, cod_empresa)
    if ini <= d <= fim:
        return True

    # Data futura nunca é digitável, nem liberada.
    if d > fim:
        return False

    if cod_empresa is None:
        cod_empresa = str(session.get("cod_empresa") or "").strip()
    if not cod_empresa:
        return False

    return any(_liberacao_cobre(lib, d, cod_filial)
               for lib in _liberacoes_caixa_vigentes(cod_empresa))


def _resumo_por_agrupamento(formas, valores, datas):
    """De onde veio o dinheiro no período: soma cada forma de recebimento no
    seu agrupamento (ex.: DÉB REDE + DÉB CIELO -> "DÉBITO"). Forma sem
    agrupamento cadastrado aparece sozinha, com o próprio nome. Forma sem
    nenhum valor lançado no período não entra — evita fatia de 0% na pizza.
    """
    somas = {}
    for f in formas:
        total_forma = sum((valores.get(d, {}).get(f["id"], 0) or 0) for d in datas)
        if not total_forma:
            continue
        rotulo = (f.get("agrupamento") or f["nome"]).strip()
        somas[rotulo] = somas.get(rotulo, 0) + total_forma

    total_geral = sum(somas.values())
    itens = [
        {"rotulo": rotulo, "valor": valor,
         "percentual": (valor / total_geral * 100) if total_geral else 0}
        for rotulo, valor in somas.items()
    ]
    itens.sort(key=lambda x: x["valor"], reverse=True)
    return itens


def _pode_alterar_filial(cur, cod_empresa, id_usuario, tipo_global, cod_filial):
    """O usuário pode digitar nesta filial?

    A concessão é por área (tabela caixas_acessos); a filial herda da área a
    que pertence. Bloquear só na tela não protegeria nada — qualquer POST
    direto passaria por cima.
    """
    if str(tipo_global or "").strip().lower() == "superusuario":
        return True

    cur.execute("""
        SELECT 1
          FROM areas_filiais af
          JOIN caixas_acessos ca
            ON ca.id_area = af.id_area
           AND ca.cod_empresa = af.cod_empresa
         WHERE af.cod_empresa = %s AND af.cod_filial = %s
           AND ca.id_usuario = %s AND ca.pode_alterar = TRUE
         LIMIT 1
    """, (cod_empresa, cod_filial, id_usuario))
    return cur.fetchone() is not None


def _celula_tem_detalhe(cur, tipo, cod_empresa, cod_filial, data_str, id_item):
    """A célula da grade é resultado de um detalhamento? Se for, o valor vem
    da soma das linhas e não pode ser sobrescrito digitando direto na grade."""
    tabela_det, campo_id = _tabela_detalhe(tipo)
    cur.execute(f"""
        SELECT 1 FROM {tabela_det}
         WHERE cod_empresa=%s AND cod_filial=%s AND data=%s AND {campo_id}=%s
         LIMIT 1
    """, (cod_empresa, cod_filial, data_str, id_item))
    return cur.fetchone() is not None


@financeiro_bp.route("/caixas/conferir", methods=["GET", "POST"])
@permissao_obrigatoria("FINANCEIRO", "ATUALIZAR_CAIXAS",
                       redirecionar_para="financeiro.menu_caixas")
def conferir_caixas():
    if "id_usuario" not in session or "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    id_usuario  = session["id_usuario"]
    tipo_global = str(session.get("tipo_global") or "").strip().lower()
    # Data de Brasília, não a do relógio do servidor (que em produção é UTC
    # e vira o dia seguinte às 21h daqui) — é ela que define a janela de
    # digitação e o "dia que se está processando".
    hoje        = _hoje_local()

    mes_sel  = int(request.args.get("mes")  or hoje.month)
    ano_sel  = int(request.args.get("ano")  or hoje.year)
    area_sel = request.args.get("area", "TODOS")

    import calendar as _cal
    _, ultimo_dia = _cal.monthrange(ano_sel, mes_sel)

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # ---- POST: salvar célula ----
        if request.method == "POST":
            tipo_campo   = request.form.get("tipo")
            cod_filial_p = int(request.form.get("cod_filial") or 0)
            data_post    = request.form.get("data")

            if not _pode_alterar_filial(cur, cod_empresa, id_usuario,
                                        tipo_global, cod_filial_p):
                return jsonify({
                    "ok": False,
                    "erro": "Você não tem permissão para alterar esta filial."
                }), 403

            # A tela já bloqueia, mas o bloqueio que vale é este: fora da
            # janela de digitação nada entra, venha de onde vier.
            if not _data_caixa_editavel(data_post, hoje, cod_empresa, cod_filial_p):
                return jsonify({
                    "ok": False,
                    "erro": "Data fora do prazo de digitação do caixa."
                }), 403

            def conv(v):
                # O JS (parseBR) já converte o valor digitado (formato BR,
                # "78,89") para um Number antes de enviar; o FormData manda
                # esse Number como string no formato JS puro ("78.89", ponto
                # decimal, sem separador de milhar). Tratar aqui como se
                # fosse texto no formato brasileiro multiplicava por 100.
                # Negativo só existe pelo detalhamento (onde a observação
                # explica o porquê). Digitado direto na grade, o sinal cai —
                # a tela também já mostra o número sem sinal.
                try: return abs(float(v or "0"))
                except: return 0.0

            # Célula alimentada por detalhamento: o valor é a soma das linhas,
            # então só o painel de detalhe pode mudá-la.
            if tipo_campo in ("forma", "controle"):
                _id_item = int(request.form.get(
                    "id_forma" if tipo_campo == "forma" else "id_controle") or 0)
                if _celula_tem_detalhe(cur, tipo_campo, cod_empresa,
                                       cod_filial_p, data_post, _id_item):
                    return jsonify({
                        "ok": False,
                        "erro": "Este valor vem de um detalhamento (soma). "
                                "Altere pelo botão de detalhar."
                    }), 409

            if tipo_campo == "forma":
                id_forma = int(request.form.get("id_forma") or 0)
                valor    = conv(request.form.get("valor"))
                cur.execute("""
                    INSERT INTO caixas_lancamentos (cod_empresa, cod_filial, data, id_forma, valor, atualizado_em)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (cod_empresa, cod_filial, data, id_forma)
                    DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = NOW()
                """, (cod_empresa, cod_filial_p, data_post, id_forma, valor))

            elif tipo_campo == "total_cx":
                valor = conv(request.form.get("valor"))
                cur.execute("""
                    INSERT INTO caixas_total_cx (cod_empresa, cod_filial, data, valor, atualizado_em)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (cod_empresa, cod_filial, data)
                    DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = NOW()
                """, (cod_empresa, cod_filial_p, data_post, valor))

            elif tipo_campo == "controle":
                id_controle = int(request.form.get("id_controle") or 0)
                valor       = conv(request.form.get("valor"))
                cur.execute("""
                    INSERT INTO caixas_controles_valores (cod_empresa, cod_filial, data, id_controle, valor, atualizado_em)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (cod_empresa, cod_filial, data, id_controle)
                    DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = NOW()
                """, (cod_empresa, cod_filial_p, data_post, id_controle, valor))

            conn.commit()
            return "", 204

        # ---- GET: montar tabela ----
        # Áreas e filiais permitidas
        cur.execute("""
            SELECT a.id_area, a.nome_area,
                   f.cod_filial, f.nome_filial, af.ordem
            FROM areas a
            JOIN areas_filiais af ON af.id_area = a.id_area AND af.cod_empresa = a.cod_empresa
            JOIN filiais f ON f.cod_filial = af.cod_filial AND f.cod_empresa = a.cod_empresa
            WHERE a.cod_empresa = %s AND a.ativo = TRUE AND f.ativo = TRUE
            ORDER BY a.nome_area, af.ordem, f.cod_filial
        """, (cod_empresa,))
        rows = cur.fetchall()

        # Agrupa: areas_dict = {id_area: {nome, filiais: [{cod_filial, nome_filial}]}}
        import collections as _col
        areas_dict = _col.OrderedDict()
        for r in rows:
            ia = r["id_area"]
            if ia not in areas_dict:
                areas_dict[ia] = {"id_area": ia, "nome_area": r["nome_area"], "filiais": []}
            areas_dict[ia]["filiais"].append({"cod_filial": r["cod_filial"], "nome_filial": r["nome_filial"]})
        areas = list(areas_dict.values())

        # ---- acesso por área (tabela caixas_acessos) ----
        # Superusuário passa direto. Para os demais, a área só aparece se
        # houver consulta concedida, e a digitação só é liberada onde houver
        # alteração. A aba de resumo (todas as áreas) é a linha id_area NULL.
        if tipo_global == "superusuario":
            areas_consulta = {a["id_area"] for a in areas}
            areas_alterar  = set(areas_consulta)
            ve_resumo      = True
        else:
            cur.execute("""
                SELECT id_area, pode_consultar, pode_alterar
                  FROM caixas_acessos
                 WHERE cod_empresa=%s AND id_usuario=%s
            """, (cod_empresa, id_usuario))
            areas_consulta, areas_alterar, ve_resumo = set(), set(), False
            for r in cur.fetchall():
                if r["id_area"] is None:
                    ve_resumo = bool(r["pode_consultar"])
                    continue
                if r["pode_consultar"]:
                    areas_consulta.add(r["id_area"])
                if r["pode_alterar"]:
                    areas_alterar.add(r["id_area"])

            areas = [a for a in areas if a["id_area"] in areas_consulta]

            if not areas:
                flash("Você não tem acesso a nenhuma área de caixas. "
                      "Peça a liberação em Acessos de Caixas.", "error")
                return redirect(url_for("financeiro.menu_caixas"))

        # Todas as filiais permitidas (lista plana)
        todos_filiais = [f["cod_filial"] for a in areas for f in a["filiais"]]

        cur.execute("""
            SELECT id, nome, agrupamento FROM caixas_formas_recebimento
            WHERE cod_empresa = %s
              AND (ativo = TRUE
                   OR EXISTS (
                       SELECT 1 FROM caixas_lancamentos l
                       WHERE l.id_forma = caixas_formas_recebimento.id
                         AND l.cod_empresa = %s
                         AND EXTRACT(MONTH FROM l.data) = %s
                         AND EXTRACT(YEAR  FROM l.data) = %s
                   ))
            ORDER BY ordem, nome
        """, (cod_empresa, cod_empresa, mes_sel, ano_sel))
        formas = cur.fetchall()

        # Parâmetros de seleção
        area_sel   = request.args.get("area",   "TODOS")   # "TODOS" | id_area
        filial_sel = request.args.get("filial",  "")        # "" | cod_filial

        # Sem direito ao resumo, "todas as áreas" não existe para este
        # usuário: a tela já abre na área dele. Se pedir uma área que não
        # pode consultar (URL montada na mão, link antigo), volta para o que
        # ele pode ver em vez de mostrar dado alheio.
        ids_permitidos = {a["id_area"] for a in areas}
        if area_sel != "TODOS":
            try:
                if int(area_sel) not in ids_permitidos:
                    area_sel, filial_sel = "TODOS", ""
            except ValueError:
                area_sel, filial_sel = "TODOS", ""
        if area_sel == "TODOS" and not ve_resumo:
            area_sel, filial_sel = str(areas[0]["id_area"]), ""
        # Empresa com uma área só (Vilela): "todas as áreas" seria a mesma
        # coisa que a área única, com um clique a mais. Abre direto nela.
        # Só quando a tela não pediu área nenhuma — trocar para "TODOS" na
        # mão continua valendo.
        if area_sel == "TODOS" and len(areas) == 1 and "area" not in request.args:
            area_sel, filial_sel = str(areas[0]["id_area"]), ""

        # A filial pedida precisa pertencer à área selecionada.
        if filial_sel and area_sel != "TODOS":
            filiais_da_area = {
                f["cod_filial"] for a in areas
                if a["id_area"] == int(area_sel) for f in a["filiais"]
            }
            try:
                if int(filial_sel) not in filiais_da_area:
                    filial_sel = ""
            except ValueError:
                filial_sel = ""

        id_area_atual   = None
        cod_filial_atual = None
        editavel = False

        if area_sel == "TODOS":
            # Soma de todas as filiais
            cods = todos_filiais
        elif filial_sel:
            # Filial individual dentro da área — editável só se houver
            # alteração concedida naquela área
            cod_filial_atual = int(filial_sel)
            id_area_atual    = int(area_sel)
            cods     = [cod_filial_atual]
            editavel = id_area_atual in areas_alterar
        else:
            # Área selecionada sem filial → soma da área
            id_area_atual = int(area_sel)
            area_obj = areas_dict.get(id_area_atual, {})
            cods = [f["cod_filial"] for f in area_obj.get("filiais", [])]

        if editavel:
            cur.execute("""
                SELECT data, id_forma, valor
                FROM caixas_lancamentos
                WHERE cod_empresa = %s AND cod_filial = %s
                  AND EXTRACT(MONTH FROM data) = %s AND EXTRACT(YEAR FROM data) = %s
            """, (cod_empresa, cod_filial_atual, mes_sel, ano_sel))
            lancs = cur.fetchall()

            cur.execute("""
                SELECT data, valor FROM caixas_total_cx
                WHERE cod_empresa = %s AND cod_filial = %s
                  AND EXTRACT(MONTH FROM data) = %s AND EXTRACT(YEAR FROM data) = %s
            """, (cod_empresa, cod_filial_atual, mes_sel, ano_sel))
            totais_cx = {r["data"]: float(r["valor"]) for r in cur.fetchall()}
        else:
            cur.execute("""
                SELECT data, id_forma, SUM(valor) AS valor
                FROM caixas_lancamentos
                WHERE cod_empresa = %s AND cod_filial = ANY(%s)
                  AND EXTRACT(MONTH FROM data) = %s AND EXTRACT(YEAR FROM data) = %s
                GROUP BY data, id_forma
            """, (cod_empresa, cods, mes_sel, ano_sel))
            lancs = cur.fetchall()

            cur.execute("""
                SELECT data, SUM(valor) AS valor
                FROM caixas_total_cx
                WHERE cod_empresa = %s AND cod_filial = ANY(%s)
                  AND EXTRACT(MONTH FROM data) = %s AND EXTRACT(YEAR FROM data) = %s
                GROUP BY data
            """, (cod_empresa, cods, mes_sel, ano_sel))
            totais_cx = {r["data"]: float(r["valor"]) for r in cur.fetchall()}

        import datetime as _dt
        valores = {}
        for l in lancs:
            d = l["data"]
            if d not in valores: valores[d] = {}
            valores[d][l["id_forma"]] = float(l["valor"])

        datas = [_dt.date(ano_sel, mes_sel, d) for d in range(1, ultimo_dia + 1)]

        # ---- Controles adicionais ----
        cur.execute("""
            SELECT id, nome, tipo FROM caixas_controles_adicionais
            WHERE cod_empresa = %s
              AND (ativo = TRUE
                   OR EXISTS (
                       SELECT 1 FROM caixas_controles_valores v
                       WHERE v.id_controle = caixas_controles_adicionais.id
                         AND v.cod_empresa = %s
                         AND EXTRACT(MONTH FROM v.data) = %s
                         AND EXTRACT(YEAR  FROM v.data) = %s
                   ))
            ORDER BY ordem
        """, (cod_empresa, cod_empresa, mes_sel, ano_sel))
        controles = cur.fetchall()

        if editavel:
            cur.execute("""
                SELECT data, id_controle, valor FROM caixas_controles_valores
                WHERE cod_empresa = %s AND cod_filial = %s
                  AND EXTRACT(MONTH FROM data) = %s AND EXTRACT(YEAR FROM data) = %s
            """, (cod_empresa, cod_filial_atual, mes_sel, ano_sel))
        else:
            cur.execute("""
                SELECT data, id_controle, SUM(valor) AS valor FROM caixas_controles_valores
                WHERE cod_empresa = %s AND cod_filial = ANY(%s)
                  AND EXTRACT(MONTH FROM data) = %s AND EXTRACT(YEAR FROM data) = %s
                GROUP BY data, id_controle
            """, (cod_empresa, cods, mes_sel, ano_sel))

        controles_valores = {}
        for r in cur.fetchall():
            d = r["data"]
            if d not in controles_valores: controles_valores[d] = {}
            controles_valores[d][r["id_controle"]] = float(r["valor"])

        # ---- abas de totais: coluna sem nenhum valor não aparece ----
        # Nas abas de total (de uma área ou de todas) não há digitação nem
        # checkbox, então coluna vazia é só ruído: sai da lista antes de
        # renderizar e some da grade inteira — cabeçalho, dias, total e
        # legenda. Não altera soma nenhuma, já que só sai o que é zero.
        # Só filtra se houver movimento no período. Com o mês inteiro zerado
        # não há o que destacar: esconder tudo deixaria a tela sem grade
        # nenhuma, então a grade completa é exibida como sempre foi.
        periodo_tem_movimento = (
            any(v for dia in valores.values() for v in dia.values())
            or any(v for dia in controles_valores.values() for v in dia.values())
        )
        if not editavel and periodo_tem_movimento:
            com_mov = [
                f for f in formas
                if any(valores.get(d, {}).get(f["id"]) for d in datas)
            ]
            # A lista de formas nunca pode ficar vazia: sem ela a tela troca a
            # grade pelo aviso de "nenhuma forma cadastrada", que seria falso.
            # Acontece quando só os controles adicionais tiveram movimento.
            formas = com_mov or formas

            # Os controles podem esvaziar sem problema — aí o bloco inteiro
            # da direita simplesmente não aparece.
            controles = [
                c for c in controles
                if any(controles_valores.get(d, {}).get(c["id"]) for d in datas)
            ]

        # quais (data, item) têm detalhamento — só existe com UMA filial
        # selecionada; usado pra marcar visualmente a célula na grade e para
        # o olho de visualização, que também vale para quem só consulta.
        com_detalhe_forma = set()
        com_detalhe_controle = set()
        dias_com_soma = set()
        if cod_filial_atual:
            # Uma consulta por tabela de detalhe resolve as duas coisas: quais
            # células têm detalhamento (para a marca na grade) e em que dias
            # algum item recebeu MAIS DE UM lançamento (para o olho verde).
            # Antes eram três idas ao banco lendo as mesmas duas tabelas.
            for tabela, campo, destino in (
                ("caixas_lancamentos_detalhe", "id_forma", com_detalhe_forma),
                ("caixas_controles_detalhe", "id_controle", com_detalhe_controle),
            ):
                cur.execute(f"""
                    SELECT data, {campo} AS id_item, COUNT(*) AS linhas
                      FROM {tabela}
                     WHERE cod_empresa=%s AND cod_filial=%s
                       AND EXTRACT(MONTH FROM data)=%s AND EXTRACT(YEAR FROM data)=%s
                     GROUP BY data, {campo}
                """, (cod_empresa, cod_filial_atual, mes_sel, ano_sel))
                for r in cur.fetchall():
                    dia = r["data"].isoformat()
                    destino.add((dia, r["id_item"]))
                    # mais de uma linha = o valor da célula é uma soma
                    if r["linhas"] > 1:
                        dias_com_soma.add(dia)

        # ---- quais colunas ficam abertas ----
        # Vale para as formas de recebimento e para os controles adicionais.
        # Fora disso ficam a DATA repetida do bloco de controles e as colunas
        # fixas do sistema (TOTAL, TOTAL CX, FALTAS / SOBRAS).
        # Só faz sentido com uma filial selecionada: a escolha é dela.
        formas_com_valor    = set()
        controles_com_valor = set()
        colunas_fechadas    = set()
        controles_fechados  = set()
        if editavel:
            # Quem tem valor no mês exibido já está em memória: `valores` e
            # `controles_valores` foram carregados acima, desta mesma filial
            # e deste mesmo mês. Perguntar de novo ao banco era uma viagem
            # de ~100 ms para uma resposta que já estava aqui.
            formas_com_valor = {
                i for dia in valores.values() for i, v in dia.items() if v
            }
            controles_com_valor = {
                i for dia in controles_valores.values() for i, v in dia.items() if v
            }

            cur.execute("""
                SELECT tipo, id_item, visivel FROM caixas_colunas_visiveis
                 WHERE cod_empresa=%s AND cod_filial=%s
            """, (cod_empresa, cod_filial_atual))
            pref = {(r["tipo"], r["id_item"]): r["visivel"] for r in cur.fetchall()}

            # O mês anterior só interessa para coluna indecisa: sem valor no
            # mês e sem escolha sua. Se não sobrou nenhuma, a consulta não
            # tem para quem responder e é pulada. Conforme as filiais vão
            # sendo configuradas, isto deixa de custar.
            mes_ant = 12 if mes_sel == 1 else mes_sel - 1
            ano_ant = ano_sel - 1 if mes_sel == 1 else ano_sel

            def _mes_anterior(itens, tipo, com_valor, tabela, campo):
                indecisos = [
                    it["id"] for it in itens
                    if it["id"] not in com_valor
                    and (tipo, it["id"]) not in pref
                ]
                if not indecisos:
                    return set()
                cur.execute(f"""
                    SELECT DISTINCT {campo} AS id_item FROM {tabela}
                     WHERE cod_empresa=%s AND cod_filial=%s AND valor <> 0
                       AND {campo} = ANY(%s)
                       AND EXTRACT(MONTH FROM data)=%s AND EXTRACT(YEAR FROM data)=%s
                """, (cod_empresa, cod_filial_atual, indecisos, mes_ant, ano_ant))
                return {r["id_item"] for r in cur.fetchall()}

            formas_mes_ant = _mes_anterior(
                formas, "forma", formas_com_valor,
                "caixas_lancamentos", "id_forma")
            controles_mes_ant = _mes_anterior(
                controles, "controle", controles_com_valor,
                "caixas_controles_valores", "id_controle")

            def _resolver(itens, tipo, com_valor, mes_anterior, destino):
                """Decide coluna a coluna, na ordem de prioridade:

                1. tem valor no mês exibido  -> aberta, sem discussão
                2. você marcou ou desmarcou  -> a sua escolha manda
                3. teve valor no mês passado -> aberta (só como ponto de
                   partida; é aprendizado, não regra fixa)
                4. resto                     -> fechada, se a filial já tem
                   movimento no mês; mês vazio não serve de base pra
                   inferir nada, então fica tudo aberto.
                """
                for it in itens:
                    if it["id"] in com_valor:
                        continue                          # 1
                    escolha = pref.get((tipo, it["id"]))
                    if escolha is not None:
                        if not escolha:
                            destino.add(it["id"])         # 2
                        continue
                    if it["id"] in mes_anterior:
                        continue                          # 3
                    if com_valor:
                        destino.add(it["id"])             # 4

            _resolver(formas, "forma", formas_com_valor,
                      formas_mes_ant, colunas_fechadas)
            _resolver(controles, "controle", controles_com_valor,
                      controles_mes_ant, controles_fechados)

    finally:
        cur.close()
        conn.close()

    nomes_meses = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                   "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]

    # Resumo por origem do dinheiro: só nas abas de totais (área específica
    # ou todas as áreas). Na aba de filial não roda sozinho — pesa demais
    # pra abrir toda vez; lá existe um botão que busca isso sob demanda.
    resumo_agrupamento = (
        _resumo_por_agrupamento(formas, valores, datas) if not cod_filial_atual else None
    )

    # uma consulta só — a janela é usada em dois pontos do template
    janela_edicao = _janela_edicao_caixa(hoje, cod_empresa)
    # Datas fora da janela normal abertas por liberação temporária. Numa aba
    # de filial vale a liberação daquela filial (mais as de "todas"); nas abas
    # de total ninguém digita, então o conjunto nem é consultado.
    datas_liberadas = (_datas_liberadas_caixa(cod_empresa, cod_filial_atual)
                       if cod_filial_atual else set())

    return render_template(
        "conferir_caixas.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_caixas"),
        areas=areas,
        ve_resumo=ve_resumo,
        area_sel=area_sel,
        filial_sel=filial_sel,
        id_area_atual=id_area_atual,
        cod_filial_atual=cod_filial_atual,
        editavel=editavel,
        # o olho (detalhamento só-leitura) aparece sempre que houver uma
        # filial selecionada, mesmo para quem só tem permissão de consulta
        visualizar=bool(cod_filial_atual),
        formas=formas,
        datas=datas,
        valores=valores,
        totais_cx=totais_cx,
        controles=controles,
        controles_valores=controles_valores,
        com_detalhe_forma=com_detalhe_forma,
        com_detalhe_controle=com_detalhe_controle,
        dias_com_soma=dias_com_soma,
        formas_com_valor=formas_com_valor,
        colunas_fechadas=colunas_fechadas,
        controles_com_valor=controles_com_valor,
        controles_fechados=controles_fechados,
        resumo_agrupamento=resumo_agrupamento,
        edicao_ini=janela_edicao[0],
        edicao_fim=janela_edicao[1],
        datas_liberadas=datas_liberadas,
        hoje=hoje,
        # "dia que se está processando": por convenção da operação, o caixa
        # fechado hoje é sempre o de ontem
        dia_processando=hoje - timedelta(days=1),
        mes_sel=mes_sel,
        ano_sel=ano_sel,
        meses=list(range(1, 13)),
        anos=list(range(hoje.year - 2, hoje.year + 2)),
        nomes_meses=nomes_meses,
        formatar_numero_br=formatar_numero_br,
    )


# ---------------------------------------------------------------------------
# DETALHAMENTO — quebra de um valor da grade em várias linhas (observação +
# valor), cuja soma vira o valor mostrado na célula. Só existe no modo
# editável (uma filial específica), tanto para formas de recebimento quanto
# para controles adicionais.
# ---------------------------------------------------------------------------

def _tabela_detalhe(tipo):
    return ("caixas_lancamentos_detalhe", "id_forma") if tipo == "forma" \
        else ("caixas_controles_detalhe", "id_controle")


def _tabela_pai(tipo):
    return ("caixas_lancamentos", "id_forma", "valor") if tipo == "forma" \
        else ("caixas_controles_valores", "id_controle", "valor")


def _recalcular_total_pai(cur, tipo, cod_empresa, cod_filial, data_str, id_item):
    """Soma as linhas de detalhe e grava o total na célula da grade
    (upsert), devolvendo o novo total."""
    tabela_det, campo_id = _tabela_detalhe(tipo)
    tabela_pai, _, campo_valor = _tabela_pai(tipo)

    cur.execute(f"""
        SELECT COALESCE(SUM(valor), 0) AS total FROM {tabela_det}
        WHERE cod_empresa=%s AND cod_filial=%s AND data=%s AND {campo_id}=%s
    """, (cod_empresa, cod_filial, data_str, id_item))
    total = float(cur.fetchone()["total"])

    cur.execute(f"""
        INSERT INTO {tabela_pai} (cod_empresa, cod_filial, data, {campo_id}, {campo_valor}, atualizado_em)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (cod_empresa, cod_filial, data, {campo_id})
        DO UPDATE SET {campo_valor} = EXCLUDED.{campo_valor}, atualizado_em = NOW()
    """, (cod_empresa, cod_filial, data_str, id_item, total))
    return total


@financeiro_bp.route("/api/caixas/detalhe", methods=["GET"])
# Só leitura: serve tanto o painel de edição quanto o olho de visualização,
# que quem só consulta também usa. Gravar continua exigindo ATUALIZAR_CAIXAS.
@permissao_obrigatoria("FINANCEIRO", "MENU_CAIXAS",
                       redirecionar_para="financeiro.menu_caixas")
def api_caixas_detalhe():
    """Devolve, para um dia e uma filial, todas as linhas de detalhe já
    lançadas em cada forma de recebimento e cada controle adicional."""
    cod_empresa = str(session["cod_empresa"]).strip()
    cod_filial  = int(request.args.get("cod_filial") or 0)
    data_str    = request.args.get("data", "")

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id, observacao, valor, id_forma AS id_item FROM caixas_lancamentos_detalhe
            WHERE cod_empresa=%s AND cod_filial=%s AND data=%s
            ORDER BY id_forma, ordem, id
        """, (cod_empresa, cod_filial, data_str))
        linhas_forma = cur.fetchall()

        cur.execute("""
            SELECT id, observacao, valor, id_controle AS id_item FROM caixas_controles_detalhe
            WHERE cod_empresa=%s AND cod_filial=%s AND data=%s
            ORDER BY id_controle, ordem, id
        """, (cod_empresa, cod_filial, data_str))
        linhas_controle = cur.fetchall()

        cur.execute("""
            SELECT observacao FROM caixas_observacoes_dia
            WHERE cod_empresa=%s AND cod_filial=%s AND data=%s
        """, (cod_empresa, cod_filial, data_str))
        linha_obs = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    def agrupar(linhas):
        agrupado = {}
        for l in linhas:
            agrupado.setdefault(l["id_item"], []).append({
                "id": l["id"], "observacao": l["observacao"] or "", "valor": float(l["valor"]),
            })
        return agrupado

    return jsonify({
        "ok": True,
        "forma":    agrupar(linhas_forma),
        "controle": agrupar(linhas_controle),
        "observacao_dia": (linha_obs or {}).get("observacao") or "",
    })


@financeiro_bp.route("/api/caixas/observacao-dia", methods=["POST"])
@permissao_obrigatoria("FINANCEIRO", "ATUALIZAR_CAIXAS",
                       redirecionar_para="financeiro.menu_caixas")
def api_caixas_observacao_dia():
    """Grava a observação livre do dia. Texto em branco apaga a linha —
    não guardar registro vazio mantém a tabela só com o que foi escrito."""
    cod_empresa = str(session["cod_empresa"]).strip()
    cod_filial  = int(request.form.get("cod_filial") or 0)
    data_str    = request.form.get("data", "")
    observacao  = (request.form.get("observacao") or "").strip()

    # Mesma janela de digitação do resto do caixa.
    if not _data_caixa_editavel(data_str, cod_empresa=cod_empresa, cod_filial=cod_filial):
        return jsonify({
            "ok": False,
            "erro": "Data fora do prazo de digitação do caixa."
        }), 403

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if not _pode_alterar_filial(cur, cod_empresa, session["id_usuario"],
                                    session.get("tipo_global"), cod_filial):
            return jsonify({
                "ok": False,
                "erro": "Você não tem permissão para alterar esta filial."
            }), 403

        if observacao:
            cur.execute("""
                INSERT INTO caixas_observacoes_dia
                    (cod_empresa, cod_filial, data, observacao)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (cod_empresa, cod_filial, data)
                DO UPDATE SET observacao = EXCLUDED.observacao, atualizado_em = NOW()
            """, (cod_empresa, cod_filial, data_str, observacao))
        else:
            cur.execute("""
                DELETE FROM caixas_observacoes_dia
                WHERE cod_empresa=%s AND cod_filial=%s AND data=%s
            """, (cod_empresa, cod_filial, data_str))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True})


@financeiro_bp.route("/api/caixas/resumo-agrupamento", methods=["GET"])
# Só leitura: o botão "ver resumo" da aba de filial também é usado por quem
# só tem consulta.
@permissao_obrigatoria("FINANCEIRO", "MENU_CAIXAS",
                       redirecionar_para="financeiro.menu_caixas")
def api_caixas_resumo_agrupamento():
    """Resumo por origem do dinheiro de UMA filial, sob demanda — é o que o
    botão "Resumo" busca na aba de filial, que não calcula isso sozinha."""
    cod_empresa = str(session["cod_empresa"]).strip()
    cod_filial  = int(request.args.get("cod_filial") or 0)
    mes = int(request.args.get("mes") or 0)
    ano = int(request.args.get("ano") or 0)
    if not cod_filial or not mes or not ano:
        return jsonify({"ok": False, "erro": "Parâmetros inválidos."}), 400

    import calendar as _cal
    _, ultimo_dia = _cal.monthrange(ano, mes)
    datas = [date(ano, mes, d) for d in range(1, ultimo_dia + 1)]

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id, nome, agrupamento FROM caixas_formas_recebimento
             WHERE cod_empresa = %s
               AND (ativo = TRUE
                    OR EXISTS (
                        SELECT 1 FROM caixas_lancamentos l
                         WHERE l.id_forma = caixas_formas_recebimento.id
                           AND l.cod_empresa = %s
                           AND EXTRACT(MONTH FROM l.data) = %s
                           AND EXTRACT(YEAR  FROM l.data) = %s
                    ))
             ORDER BY ordem, nome
        """, (cod_empresa, cod_empresa, mes, ano))
        formas = cur.fetchall()

        cur.execute("""
            SELECT data, id_forma, valor FROM caixas_lancamentos
             WHERE cod_empresa = %s AND cod_filial = %s
               AND EXTRACT(MONTH FROM data) = %s AND EXTRACT(YEAR FROM data) = %s
        """, (cod_empresa, cod_filial, mes, ano))
        valores = {}
        for r in cur.fetchall():
            valores.setdefault(r["data"], {})[r["id_forma"]] = float(r["valor"])
    finally:
        cur.close()
        conn.close()

    itens = _resumo_por_agrupamento(formas, valores, datas)
    return jsonify({"ok": True, "itens": itens})


@financeiro_bp.route("/api/caixas/coluna-visivel", methods=["POST"])
@permissao_obrigatoria("FINANCEIRO", "ATUALIZAR_CAIXAS",
                       redirecionar_para="financeiro.menu_caixas")
def api_caixas_coluna_visivel():
    """Grava se uma coluna fica aberta ou fechada nesta filial. Vale para
    forma de recebimento e para controle adicional. Coluna com valor lançado
    no mês não pode ser fechada."""
    cod_empresa = str(session["cod_empresa"]).strip()
    cod_filial  = int(request.form.get("cod_filial") or 0)
    tipo        = request.form.get("tipo", "forma")
    id_item     = int(request.form.get("id_item") or 0)
    visivel     = request.form.get("visivel") == "1"

    if tipo not in ("forma", "controle"):
        return jsonify({"ok": False, "erro": "Tipo inválido."}), 400
    if not cod_filial or not id_item:
        return jsonify({"ok": False, "erro": "Filial ou coluna inválida."}), 400

    tabela_valores, campo_id = (
        ("caixas_lancamentos", "id_forma") if tipo == "forma"
        else ("caixas_controles_valores", "id_controle")
    )

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # A escolha de colunas é da filial e vale para todo mundo que a abrir,
        # então só quem altera aquela filial pode mexer.
        if not _pode_alterar_filial(cur, cod_empresa, session["id_usuario"],
                                    session.get("tipo_global"), cod_filial):
            return jsonify({
                "ok": False,
                "erro": "Você não tem permissão para alterar esta filial."
            }), 403

        if not visivel:
            # Trava: com valor lançado no mês corrente a coluna não fecha.
            hoje = date.today()
            mes = int(request.form.get("mes") or hoje.month)
            ano = int(request.form.get("ano") or hoje.year)
            cur.execute(f"""
                SELECT 1 FROM {tabela_valores}
                 WHERE cod_empresa=%s AND cod_filial=%s AND {campo_id}=%s AND valor <> 0
                   AND EXTRACT(MONTH FROM data)=%s AND EXTRACT(YEAR FROM data)=%s
                 LIMIT 1
            """, (cod_empresa, cod_filial, id_item, mes, ano))
            if cur.fetchone():
                return jsonify({
                    "ok": False,
                    "erro": "Esta coluna tem valor lançado no mês e não pode ser fechada."
                }), 409

        cur.execute("""
            INSERT INTO caixas_colunas_visiveis
                (cod_empresa, cod_filial, tipo, id_item, visivel, atualizado_em)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (cod_empresa, cod_filial, tipo, id_item)
            DO UPDATE SET visivel = EXCLUDED.visivel, atualizado_em = NOW()
        """, (cod_empresa, cod_filial, tipo, id_item, visivel))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "visivel": visivel})


@financeiro_bp.route("/api/caixas/detalhe/salvar", methods=["POST"])
@permissao_obrigatoria("FINANCEIRO", "ATUALIZAR_CAIXAS",
                       redirecionar_para="financeiro.menu_caixas")
def api_caixas_detalhe_salvar():
    """
    Cria ou atualiza uma linha de detalhe (observação + valor). Sem id_linha
    = cria uma nova. Com os dois campos em branco = apaga a linha. Depois
    de qualquer mudança, recalcula e devolve o novo total da célula-mãe.
    """
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo        = request.form.get("tipo", "")
    if tipo not in ("forma", "controle"):
        return jsonify({"ok": False, "erro": "Tipo inválido."}), 400

    cod_filial  = int(request.form.get("cod_filial") or 0)
    data_str    = request.form.get("data", "")
    id_item     = int(request.form.get("id_item") or 0)
    id_linha    = request.form.get("id_linha") or ""

    # Mesma janela de digitação da grade — detalhar é digitar.
    if not _data_caixa_editavel(data_str, cod_empresa=cod_empresa, cod_filial=cod_filial):
        return jsonify({
            "ok": False,
            "erro": "Data fora do prazo de digitação do caixa."
        }), 403

    observacao  = (request.form.get("observacao") or "").strip()
    try:
        valor = float(request.form.get("valor") or "0")
    except ValueError:
        valor = 0.0

    # Valor negativo é aceito aqui (e só aqui), mas nunca solto: a observação
    # é o que justifica o abatimento na célula da grade.
    if valor < 0 and not observacao:
        return jsonify({
            "ok": False,
            "erro": "Escreva na coluna Observação o motivo do valor negativo — "
                    "sem isso o abatimento não é gravado."
        }), 400

    tabela_det, campo_id = _tabela_detalhe(tipo)

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if not _pode_alterar_filial(cur, cod_empresa, session["id_usuario"],
                                    session.get("tipo_global"), cod_filial):
            return jsonify({
                "ok": False,
                "erro": "Você não tem permissão para alterar esta filial."
            }), 403

        nova_id = None
        if not observacao and not valor:
            # em branco: se já existia, apaga; se era nova, não faz nada
            if id_linha:
                cur.execute(f"DELETE FROM {tabela_det} WHERE id=%s AND cod_empresa=%s",
                            (id_linha, cod_empresa))
        elif id_linha:
            cur.execute(f"""
                UPDATE {tabela_det} SET observacao=%s, valor=%s, atualizado_em=NOW()
                WHERE id=%s AND cod_empresa=%s
            """, (observacao, valor, id_linha, cod_empresa))
        else:
            cur.execute(f"""
                INSERT INTO {tabela_det}
                    (cod_empresa, cod_filial, data, {campo_id}, observacao, valor, ordem)
                VALUES (%s, %s, %s, %s, %s, %s,
                        COALESCE((SELECT MAX(ordem)+1 FROM {tabela_det}
                                  WHERE cod_empresa=%s AND cod_filial=%s AND data=%s AND {campo_id}=%s), 0))
                RETURNING id
            """, (cod_empresa, cod_filial, data_str, id_item, observacao, valor,
                  cod_empresa, cod_filial, data_str, id_item))
            nova_id = cur.fetchone()["id"]

        total = _recalcular_total_pai(cur, tipo, cod_empresa, cod_filial, data_str, id_item)
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "id": nova_id, "total": total})


# ---------------------------------------------------------------------------
# LIBERAÇÃO TEMPORÁRIA DE DATAS ANTIGAS (caixas)
# Abre um PERÍODO já fora da janela de digitação, para uma filial ou para
# todas, por algumas horas. As liberações se somam e expiram sozinhas.
# ---------------------------------------------------------------------------
def _filiais_empresa(cod_empresa):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT cod_filial, nome_filial FROM filiais
             WHERE cod_empresa = %s AND ativo = TRUE
             ORDER BY cod_filial
        """, (cod_empresa,))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def _sugestao_periodo_liberacao(cod_empresa, hoje=None):
    """(data_inicial, data_final) sugeridas na tela.

    Inicial: primeiro dia do mês corrente. Final: o último dia que ainda NÃO
    está aberto — a véspera do início da janela normal de digitação. Se o mês
    mal começou e tudo já está aberto, as duas pontas se encostam no dia 1º.
    """
    hoje = hoje or _hoje_local()
    ini = hoje.replace(day=1)
    fim = _janela_edicao_caixa(hoje, cod_empresa)[0] - timedelta(days=1)
    return (ini, max(ini, fim))


def _liberacao_json(lib):
    return {
        "id_liberacao": lib["id_liberacao"],
        "data_liberada_desde": lib["data_liberada_desde"].isoformat(),
        "data_liberada_ate": lib["data_liberada_ate"].isoformat(),
        "cod_filial": lib["cod_filial"],
        "nome_filial": lib["nome_filial"] if lib["cod_filial"] is not None else None,
        "expira_em": lib["expira_em"].isoformat(),
        "ativado_por": lib["ativado_por"],
    }


def _payload_liberacoes(cod_empresa):
    """O que a tela precisa para se redesenhar: as liberações vigentes e a
    data mínima da janela normal. Criar, encerrar e consultar devolvem tudo
    isso, para a lista não depender de uma segunda requisição."""
    return {
        "liberacoes": [_liberacao_json(l)
                       for l in _liberacoes_caixa_vigentes(cod_empresa)],
        "data_minima_editavel":
            _janela_edicao_caixa(cod_empresa=cod_empresa)[0].isoformat(),
    }


@financeiro_bp.route("/caixas/liberacao-temporaria")
@permissao_obrigatoria("FINANCEIRO", "CONFIGURACOES_CAIXAS",
                       redirecionar_para="financeiro.menu_caixas")
def caixas_liberacao_temporaria():
    cod_empresa = str(session["cod_empresa"]).strip()
    sug_ini, sug_fim = _sugestao_periodo_liberacao(cod_empresa)
    return render_template(
        "caixas_liberacao_temporaria.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_caixas"),
        horas_liberacao=HORAS_LIBERACAO_CAIXAS,
        dias_edicao=DIAS_EDICAO_CAIXA,
        filiais=_filiais_empresa(cod_empresa),
        sugestao_inicial=sug_ini.isoformat(),
        sugestao_final=sug_fim.isoformat(),
    )


@financeiro_bp.route("/api/caixas/liberacao-temporaria", methods=["GET"])
@permissao_obrigatoria("FINANCEIRO", "CONFIGURACOES_CAIXAS",
                       redirecionar_para="financeiro.menu_caixas")
def api_status_liberacao_temporaria_caixas():
    cod_empresa = str(session["cod_empresa"]).strip()
    return jsonify(dict(_payload_liberacoes(cod_empresa), ok=True))


@financeiro_bp.route("/api/caixas/liberacao-temporaria", methods=["POST"])
@permissao_obrigatoria("FINANCEIRO", "CONFIGURACOES_CAIXAS",
                       redirecionar_para="financeiro.menu_caixas")
def api_ativar_liberacao_temporaria_caixas():
    cod_empresa = str(session["cod_empresa"]).strip()
    id_usuario = session["id_usuario"]
    dados = request.get_json(silent=True) or {}

    def _data(campo):
        try:
            return datetime.strptime(dados.get(campo), "%Y-%m-%d").date()
        except (TypeError, ValueError):
            return None

    data_inicial = _data("data_inicial")
    data_final = _data("data_final")
    if not data_inicial or not data_final:
        return jsonify({"ok": False,
                        "erro": "Informe data_inicial e data_final (YYYY-MM-DD)."}), 400
    if data_final < data_inicial:
        return jsonify({"ok": False,
                        "erro": "A data final não pode ser anterior à inicial."}), 400

    # "TODAS" (ou vazio) grava NULL — vale para a empresa inteira.
    cod_filial = dados.get("cod_filial")
    if cod_filial in (None, "", "TODAS"):
        cod_filial = None
    else:
        try:
            cod_filial = int(cod_filial)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "erro": "Filial inválida."}), 400
        if cod_filial not in {f["cod_filial"] for f in _filiais_empresa(cod_empresa)}:
            return jsonify({"ok": False, "erro": "Filial inválida."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            INSERT INTO caixas_liberacao_temporaria
                (cod_empresa, data_liberada_desde, data_liberada_ate,
                 cod_filial, id_usuario_ativou)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id_liberacao
        """, (cod_empresa, data_inicial, data_final, cod_filial, id_usuario))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify(dict(_payload_liberacoes(cod_empresa), ok=True)), 201


@financeiro_bp.route("/api/caixas/liberacao-temporaria/<int:id_liberacao>",
                     methods=["DELETE"])
@permissao_obrigatoria("FINANCEIRO", "CONFIGURACOES_CAIXAS",
                       redirecionar_para="financeiro.menu_caixas")
def api_revogar_liberacao_temporaria_caixas(id_liberacao):
    """Encerra uma liberação antes da hora. A linha fica no histórico —
    marcar `revogado_em` diz quando parou de valer; apagar não diria nada."""
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE caixas_liberacao_temporaria
               SET revogado_em = NOW()
             WHERE id_liberacao = %s AND cod_empresa = %s AND revogado_em IS NULL
        """, (id_liberacao, cod_empresa))
        encontrou = cur.rowcount > 0
        conn.commit()
    finally:
        cur.close()
        conn.close()

    if not encontrou:
        return jsonify({"ok": False, "erro": "Liberação não encontrada."}), 404

    return jsonify(dict(_payload_liberacoes(cod_empresa), ok=True))


@financeiro_bp.route("/caixas/configuracoes", methods=["GET", "POST"])
@permissao_obrigatoria("FINANCEIRO", "CONFIGURACOES_CAIXAS",
                       redirecionar_para="financeiro.menu_caixas")
def configuracoes_caixas():
    if "id_usuario" not in session or "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            acao = request.form.get("acao")

            if acao == "incluir":
                nome  = (request.form.get("nome") or "").strip().upper()
                agrupamento = (request.form.get("agrupamento") or "").strip().upper() or None
                ordem = int(request.form.get("ordem") or 0)
                if nome:
                    cur.execute("""
                        INSERT INTO caixas_formas_recebimento (cod_empresa, nome, agrupamento, ordem)
                        VALUES (%s, %s, %s, %s)
                    """, (cod_empresa, nome, agrupamento, ordem))
                    conn.commit()
                    flash("Forma de recebimento incluída.", "success")

            elif acao == "editar":
                id_ed = request.form.get("id_editar")
                nome  = (request.form.get("nome_editar") or "").strip().upper()
                agrupamento = (request.form.get("agrupamento_editar") or "").strip().upper() or None
                ordem = int(request.form.get("ordem_editar") or 0)
                if id_ed and nome:
                    cur.execute("""
                        UPDATE caixas_formas_recebimento
                        SET nome = %s, agrupamento = %s, ordem = %s WHERE id = %s AND cod_empresa = %s
                    """, (nome, agrupamento, ordem, id_ed, cod_empresa))
                    conn.commit()
                    flash("Forma de recebimento atualizada.", "success")

            elif acao == "excluir":
                id_ex = request.form.get("id_excluir")
                if id_ex:
                    cur.execute("SELECT COUNT(*) FROM caixas_lancamentos WHERE id_forma = %s AND cod_empresa = %s", (id_ex, cod_empresa))
                    if cur.fetchone()["count"] > 0:
                        flash("Esta forma possui lançamentos e não pode ser excluída. Use Inativar para ocultá-la.", "error")
                    else:
                        cur.execute("DELETE FROM caixas_formas_recebimento WHERE id = %s AND cod_empresa = %s", (id_ex, cod_empresa))
                        conn.commit()
                        flash("Forma de recebimento excluída.", "success")

            elif acao == "inativar":
                id_in = request.form.get("id_inativar")
                if id_in:
                    cur.execute("UPDATE caixas_formas_recebimento SET ativo = NOT ativo WHERE id = %s AND cod_empresa = %s", (id_in, cod_empresa))
                    conn.commit()
                    flash("Status da forma de recebimento alterado.", "success")

            # ---- Controles adicionais ----
            elif acao == "ctrl_incluir":
                nome  = (request.form.get("ctrl_nome") or "").strip().upper()
                tipo  = request.form.get("ctrl_tipo") or "INFO"
                ordem = int(request.form.get("ctrl_ordem") or 0)
                if nome:
                    cur.execute("""
                        INSERT INTO caixas_controles_adicionais (cod_empresa, nome, tipo, ordem)
                        VALUES (%s, %s, %s, %s)
                    """, (cod_empresa, nome, tipo, ordem))
                    conn.commit()
                    flash("Controle adicional incluído.", "success")

            elif acao == "ctrl_editar":
                id_ed = request.form.get("ctrl_id_editar")
                nome  = (request.form.get("ctrl_nome_editar") or "").strip().upper()
                tipo  = request.form.get("ctrl_tipo_editar") or "INFO"
                ordem = int(request.form.get("ctrl_ordem_editar") or 0)
                if id_ed and nome:
                    cur.execute("""
                        UPDATE caixas_controles_adicionais
                        SET nome = %s, tipo = %s, ordem = %s WHERE id = %s AND cod_empresa = %s
                    """, (nome, tipo, ordem, id_ed, cod_empresa))
                    conn.commit()
                    flash("Controle adicional atualizado.", "success")

            elif acao == "ctrl_excluir":
                id_ex = request.form.get("ctrl_id_excluir")
                if id_ex:
                    cur.execute("SELECT COUNT(*) FROM caixas_controles_valores WHERE id_controle = %s AND cod_empresa = %s", (id_ex, cod_empresa))
                    if cur.fetchone()["count"] > 0:
                        flash("Este controle possui lançamentos e não pode ser excluído. Use Inativar para ocultá-lo.", "error")
                    else:
                        cur.execute("DELETE FROM caixas_controles_adicionais WHERE id = %s AND cod_empresa = %s", (id_ex, cod_empresa))
                        conn.commit()
                        flash("Controle adicional excluído.", "success")

            elif acao == "ctrl_inativar":
                id_in = request.form.get("ctrl_id_inativar")
                if id_in:
                    cur.execute("UPDATE caixas_controles_adicionais SET ativo = NOT ativo WHERE id = %s AND cod_empresa = %s", (id_in, cod_empresa))
                    conn.commit()
                    flash("Status do controle adicional alterado.", "success")

        cur.execute("""
            SELECT id, nome, agrupamento, ordem, ativo FROM caixas_formas_recebimento
            WHERE cod_empresa = %s ORDER BY ordem, nome
        """, (cod_empresa,))
        formas = cur.fetchall()

        cur.execute("""
            SELECT id, nome, tipo, ordem, ativo FROM caixas_controles_adicionais
            WHERE cod_empresa = %s ORDER BY ordem, nome
        """, (cod_empresa,))
        controles = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return render_template(
        "configuracoes_caixas.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_caixas"),
        formas=formas,
        controles=controles,
    )


@financeiro_bp.route("/saldos")
@permissao_obrigatoria("FINANCEIRO", "MENU_SALDOS")
def menu_saldos():
    """Menu do módulo Saldos: Informar, Consultar Variações e Configurar."""
    cod_empresa = str(session["cod_empresa"]).strip()
    id_usuario = session["id_usuario"]
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_informar = True
        pode_antecipacao = True
        pode_variacoes = True
        pode_variacoes_filial = True
        pode_configurar = True
        pode_acessos_area = True
    else:
        pode_informar = usuario_tem_permissao(
            id_usuario, cod_empresa, "FINANCEIRO", "INFORMAR_SALDOS"
        )
        pode_antecipacao = usuario_tem_permissao(
            id_usuario, cod_empresa, "FINANCEIRO", "ANTECIPACAO_DIVIDENDOS"
        )
        pode_variacoes = usuario_tem_permissao(
            id_usuario, cod_empresa, "FINANCEIRO", "CONSULTAR_VARIACOES_SALDOS"
        )
        pode_variacoes_filial = usuario_tem_permissao(
            id_usuario, cod_empresa, "FINANCEIRO", "CONSULTAR_VARIACOES_FILIAL"
        )
        pode_acessos_area = usuario_tem_permissao(
            id_usuario, cod_empresa, "FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS"
        )
        # Configurar Saldos virou menu: períodos/feriados (CONFIGURAR_SALDOS) e
        # os cadastros (CADASTRO_CONTAS_BANCARIAS). Qualquer uma das duas abre.
        pode_configurar = pode_acessos_area or usuario_tem_permissao(
            id_usuario, cod_empresa, "FINANCEIRO", "CONFIGURAR_SALDOS"
        )

    return render_template(
        "menu_saldos.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_empresa"),
        pode_informar=pode_informar,
        pode_antecipacao=pode_antecipacao,
        pode_variacoes=pode_variacoes,
        pode_variacoes_filial=pode_variacoes_filial,
        pode_configurar=pode_configurar,
        pode_acessos_area=pode_acessos_area,
    )


@financeiro_bp.route("/saldos/informar")
@permissao_obrigatoria("FINANCEIRO", "INFORMAR_SALDOS")
def informar_saldos():
    cod_empresa = str(session["cod_empresa"]).strip()
    id_usuario = session["id_usuario"]
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_lancamento = True
    else:
        pode_lancamento = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "LANCAMENTO_SALDOS")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id_area, nome_area
            FROM areas
            WHERE cod_empresa = %s AND ativo = TRUE
            ORDER BY nome_area
        """, (cod_empresa,))
        areas = cur.fetchall()

        if tipo_global != "superusuario":
            areas_ok = areas_permitidas_usuario(cur, cod_empresa, id_usuario)
            areas = [a for a in areas if a["id_area"] in areas_ok]

        config = config_saldos_empresa(cur, cod_empresa)
    finally:
        cur.close()
        conn.close()

    return render_template(
        "saldos.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        areas=areas,
        pode_lancamento=pode_lancamento,
        config=config,
        url_voltar=url_for("financeiro.menu_saldos"),
    )


@financeiro_bp.route("/saldos/variacoes")
@permissao_obrigatoria("FINANCEIRO", "CONSULTAR_VARIACOES_SALDOS")
def consultar_variacoes_saldos():
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id_competencia, mes_ano, data_inicio, data_fim
              FROM competencia_mes
             WHERE cod_empresa = %s
             ORDER BY mes_ano DESC
        """, (cod_empresa,))
        periodos = [_periodo_json(p) for p in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

    return render_template(
        "variacoes_saldos.html",
        periodos=periodos,
        empresa_ativa=str(session["cod_empresa"]).strip(),
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_saldos"),
    )


@financeiro_bp.route("/saldos/configurar")
def configurar_saldos():
    """Menu de configuração dos Saldos: períodos, feriados e os cadastros."""
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))
    if "cod_empresa" not in session:
        return redirect(url_for("auth.escolher_empresa"))

    cod_empresa = str(session["cod_empresa"]).strip()
    id_usuario = session["id_usuario"]

    if str(session.get("tipo_global") or "").strip().lower() == "superusuario":
        pode_configurar = True
        pode_cadastros = True
    else:
        pode_configurar = usuario_tem_permissao(
            id_usuario, cod_empresa, "FINANCEIRO", "CONFIGURAR_SALDOS"
        )
        pode_cadastros = usuario_tem_permissao(
            id_usuario, cod_empresa, "FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS"
        )

    # As duas permissões abrem opções diferentes deste menu — basta uma delas.
    if not pode_configurar and not pode_cadastros:
        flash("Você não tem permissão para acessar esta opção.", "error")
        return redirect(url_for("sistema.selecionar_sistema"))

    return render_template(
        "configurar_saldos.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_saldos"),
        pode_configurar=pode_configurar,
        pode_cadastros=pode_cadastros,
    )


OPCOES_VISUALIZACAO_SALDOS = (
    "mostrar_valores_informados",
    "mostrar_recebiveis",
    "mostrar_variacoes",
)


def config_saldos_empresa(cur, cod_empresa):
    """Parâmetros de visualização da empresa. Sem linha cadastrada, tudo desligado."""
    cur.execute(f"""
        SELECT {", ".join(OPCOES_VISUALIZACAO_SALDOS)}
          FROM saldos_configuracoes
         WHERE cod_empresa = %s
    """, (cod_empresa,))
    linha = cur.fetchone()
    return {opcao: bool(linha[opcao]) if linha else False for opcao in OPCOES_VISUALIZACAO_SALDOS}


@financeiro_bp.route("/saldos/configurar/parametros-visualizacao")
@permissao_obrigatoria("FINANCEIRO", "CONFIGURAR_SALDOS")
def saldos_parametros_visualizacao():
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        config = config_saldos_empresa(cur, cod_empresa)
    finally:
        cur.close()
        conn.close()

    return render_template(
        "saldos_parametros_visualizacao.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.configurar_saldos"),
        config=config,
    )


@financeiro_bp.route("/api/saldos/configuracoes", methods=["PUT"])
@permissao_obrigatoria("FINANCEIRO", "CONFIGURAR_SALDOS")
def api_salvar_configuracoes_saldos():
    cod_empresa = str(session["cod_empresa"]).strip()
    dados = request.get_json(silent=True) or {}
    valores = [bool(dados.get(opcao)) for opcao in OPCOES_VISUALIZACAO_SALDOS]

    colunas = ", ".join(OPCOES_VISUALIZACAO_SALDOS)
    marcadores = ", ".join(["%s"] * len(OPCOES_VISUALIZACAO_SALDOS))
    atribuicoes = ", ".join(f"{opcao} = EXCLUDED.{opcao}" for opcao in OPCOES_VISUALIZACAO_SALDOS)

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO saldos_configuracoes (cod_empresa, {colunas})
            VALUES (%s, {marcadores})
            ON CONFLICT (cod_empresa) DO UPDATE
               SET {atribuicoes},
                   atualizado_em = now()
        """, (cod_empresa, *valores))
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True})


@financeiro_bp.route("/saldos/configurar/periodos")
@permissao_obrigatoria("FINANCEIRO", "CONFIGURAR_SALDOS")
def saldos_periodos():
    return render_template(
        "saldos_periodos.html",
        empresa_ativa=str(session["cod_empresa"]).strip(),
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.configurar_saldos"),
    )


@financeiro_bp.route("/saldos/configurar/feriados")
@permissao_obrigatoria("FINANCEIRO", "CONFIGURAR_SALDOS")
def saldos_feriados():
    return render_template(
        "saldos_feriados.html",
        empresa_ativa=str(session["cod_empresa"]).strip(),
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.configurar_saldos"),
    )


@financeiro_bp.route("/saldos/configurar/contas-bancarias")
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def saldos_contas_bancarias():
    return render_template(
        "saldos_contas_bancarias.html",
        empresa_ativa=str(session["cod_empresa"]).strip(),
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.configurar_saldos"),
    )


@financeiro_bp.route("/saldos/configurar/indicadores-recebiveis")
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def saldos_indicadores_recebiveis():
    return render_template(
        "saldos_indicadores_recebiveis.html",
        empresa_ativa=str(session["cod_empresa"]).strip(),
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.configurar_saldos"),
    )


@financeiro_bp.route("/saldos/configurar/liberacao-temporaria")
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def saldos_liberacao_temporaria():
    return render_template(
        "saldos_liberacao_temporaria.html",
        empresa_ativa=str(session["cod_empresa"]).strip(),
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.configurar_saldos"),
    )


@financeiro_bp.route("/saldos/acessos-area")
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def acessos_area_saldos():
    """Concessão de acesso aos saldos por área, dentro do menu de Saldos."""
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id_area, nome_area
            FROM areas
            WHERE cod_empresa = %s AND ativo = TRUE
            ORDER BY nome_area
        """, (cod_empresa,))
        areas = cur.fetchall()

        cur.execute("""
            SELECT u.id_usuario, u.nome
            FROM usuarios u
            JOIN usuarios_empresas ue ON ue.id_usuario = u.id_usuario
            WHERE ue.cod_empresa = %s AND u.ativo = TRUE
            ORDER BY u.nome
        """, (cod_empresa,))
        usuarios = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return render_template(
        "acessos_area_saldos.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        areas=areas,
        usuarios=usuarios,
        url_voltar=url_for("financeiro.menu_saldos"),
    )


# =========================
# API
# =========================

def filiais_da_area(cur, cod_empresa, id_area):
    cur.execute("""
        SELECT af.cod_filial, f.nome_filial
        FROM areas_filiais af
        JOIN filiais f
          ON f.cod_empresa = af.cod_empresa
         AND f.cod_filial = af.cod_filial
        WHERE af.cod_empresa = %s
          AND af.id_area = %s
          AND f.ativo = TRUE
        ORDER BY af.ordem, f.nome_filial
    """, (cod_empresa, id_area))
    return cur.fetchall()


def _areas_do_usuario(cur, cod_empresa, id_usuario, coluna):
    """Áreas concedidas ao usuário em caixas_acessos.

    Saldos usa a MESMA tabela de Caixas: quem consulta a área no caixa
    consulta em saldos, quem altera altera. A linha de resumo (id_area NULL)
    é ignorada aqui — saldos não tem aba de todas as áreas.
    """
    cur.execute(f"""
        SELECT id_area FROM caixas_acessos
         WHERE cod_empresa = %s AND id_usuario = %s
           AND id_area IS NOT NULL AND {coluna} = TRUE
    """, (cod_empresa, id_usuario))
    linhas = cur.fetchall()
    if not linhas:
        return set()
    if isinstance(linhas[0], dict):
        return {int(r["id_area"]) for r in linhas}
    return {int(r[0]) for r in linhas}


def areas_permitidas_usuario(cur, cod_empresa, id_usuario):
    """Áreas que o usuário pode VER em saldos."""
    return _areas_do_usuario(cur, cod_empresa, id_usuario, "pode_consultar")


def areas_alteracao_usuario(cur, cod_empresa, id_usuario):
    """Áreas em que o usuário pode LANÇAR em saldos."""
    return _areas_do_usuario(cur, cod_empresa, id_usuario, "pode_alterar")


def filiais_permitidas_usuario(cur, cod_empresa, id_area):
    filiais = filiais_da_area(cur, cod_empresa, id_area)

    if session.get("tipo_global") == "superusuario":
        return filiais

    areas_ok = areas_permitidas_usuario(cur, cod_empresa, session["id_usuario"])
    if id_area not in areas_ok:
        return []

    return filiais


def cod_filiais_permitidas_lancamento(cur, cod_empresa):
    if session.get("tipo_global") == "superusuario":
        cur.execute("SELECT cod_filial FROM filiais WHERE cod_empresa = %s AND ativo = TRUE", (cod_empresa,))
        return {int(r[0]) for r in cur.fetchall()}

    # Lançar é alterar: aqui vale pode_alterar, não pode_consultar. Quem só
    # consulta enxerga a área mas não grava nada nela.
    areas_ok = areas_alteracao_usuario(cur, cod_empresa, session["id_usuario"])
    if not areas_ok:
        return set()

    cur.execute("""
        SELECT DISTINCT af.cod_filial
        FROM areas_filiais af
        WHERE af.cod_empresa = %s AND af.id_area = ANY(%s)
    """, (cod_empresa, list(areas_ok)))
    return {int(r[0]) for r in cur.fetchall()}


FUSO_LOCAL = ZoneInfo("America/Sao_Paulo")

# Duração da liberação temporária de saldos, em horário local.
HORAS_LIBERACAO_SALDOS = 4

# Mesma ideia para o caixa — a janela normal de digitação é curta, e a
# liberação abre uma data antiga por um tempo limitado.
HORAS_LIBERACAO_CAIXAS = 4


def _ativado_em_local(ativado_em):
    """Converte o `ativado_em` da liberação para o horário de Brasília.

    A coluna é `timestamp without time zone` e é preenchida pelo NOW() do
    banco, que roda em UTC. Comparar esse valor direto com datetime.now(),
    que é local, criava três horas de folga: a liberação anunciada como de
    1 hora durava 4, e a tela mostrava a expiração em UTC.
    """
    if ativado_em is None:
        return None
    return (ativado_em.replace(tzinfo=timezone.utc)
                      .astimezone(FUSO_LOCAL)
                      .replace(tzinfo=None))


def _agora_local():
    """Agora em horário de Brasília, ingênuo — o mesmo referencial de
    `_ativado_em_local`.

    Não usar `datetime.now()` para comparar com a expiração de uma liberação:
    ele devolve o relógio do **servidor**, que em produção roda em UTC. O
    `expira_em` já está em Brasília, então a comparação encurtava a liberação
    em três horas — a liberação de 4 horas morria em 1, e a tela anunciava um
    horário de expiração que nunca chegava a valer.
    """
    return datetime.now(FUSO_LOCAL).replace(tzinfo=None)


def _hoje_local():
    """A data de hoje em Brasília. Pela mesma razão: num servidor em UTC,
    `date.today()` já vira o dia seguinte às 21h daqui, e a janela de
    digitação passaria a contar de um dia que ainda não começou."""
    return _agora_local().date()


# =========================
# DIAS ÚTEIS (SALDOS SÓ TRABALHA COM DIA ÚTIL)
# =========================
DIAS_SEMANA_PT = ["Segunda-feira", "Terça-feira", "Quarta-feira", "Quinta-feira",
                  "Sexta-feira", "Sábado", "Domingo"]


def carregar_feriados(cod_empresa, data_inicio=None, data_fim=None):
    """Conjunto de datas de feriado da empresa (opcionalmente no período)."""
    sql = "SELECT data FROM saldos_feriados WHERE cod_empresa = %s"
    params = [cod_empresa]
    if data_inicio:
        sql += " AND data >= %s"
        params.append(data_inicio)
    if data_fim:
        sql += " AND data <= %s"
        params.append(data_fim)

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return {linha[0] for linha in cur.fetchall()}
    finally:
        cur.close()
        conn.close()


def eh_dia_util(data, feriados):
    """Sábado, domingo e feriado cadastrado não são dias úteis."""
    return data.weekday() < 5 and data not in feriados


def dia_util_anterior(data, feriados):
    """Dia útil imediatamente anterior — é com ele que a variação é calculada
    (numa segunda-feira, encosta na sexta; feriado no meio também é pulado)."""
    anterior = data - timedelta(days=1)
    # limite de segurança: sem ele um cadastro de feriados errado viraria laço infinito
    for _ in range(60):
        if eh_dia_util(anterior, feriados):
            return anterior
        anterior -= timedelta(days=1)
    return anterior


def data_minima_editavel(cod_empresa):
    """Normalmente só os dois últimos dias úteis são editáveis. Uma liberação
    temporária recente abaixa esse limite para a data liberada."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT data_liberada_desde, ativado_em
            FROM saldos_liberacao_temporaria
            WHERE cod_empresa = %s
            ORDER BY ativado_em DESC
            LIMIT 1
        """, (cod_empresa,))
        liberacao = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if liberacao:
        ativado = _ativado_em_local(liberacao["ativado_em"])
        if (_agora_local() - ativado) < timedelta(hours=HORAS_LIBERACAO_SALDOS):
            return liberacao["data_liberada_desde"]

    # dois últimos dias úteis: numa segunda-feira libera sexta e quinta,
    # não sábado e domingo (que a tela nem gera)
    feriados = carregar_feriados(cod_empresa, date.today() - timedelta(days=60), date.today())
    return dia_util_anterior(dia_util_anterior(date.today(), feriados), feriados)


# =========================
# CADASTRO: CONTAS BANCÁRIAS
# =========================
@financeiro_bp.route("/api/contas-bancarias", methods=["GET"])
@permissao_obrigatoria("FINANCEIRO", "CONSULTA_SALDOS")
def api_listar_contas_bancarias():
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id_conta_bancaria, banco, apelido, ordem, ativo, espelhar_sistema
            FROM contas_bancarias
            WHERE cod_empresa = %s
            ORDER BY ordem, banco
        """, (cod_empresa,))
        contas = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "contas_bancarias": contas})


@financeiro_bp.route("/api/contas-bancarias", methods=["POST"])
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def api_criar_conta_bancaria():
    cod_empresa = str(session["cod_empresa"]).strip()
    dados = request.get_json(silent=True) or {}

    banco = (dados.get("banco") or "").strip()
    apelido = (dados.get("apelido") or "").strip() or None
    ordem = dados.get("ordem", 10)
    espelhar_sistema = bool(dados.get("espelhar_sistema", False))

    if not banco:
        return jsonify({"ok": False, "erro": "Informe o banco."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            INSERT INTO contas_bancarias (cod_empresa, banco, apelido, ordem, espelhar_sistema)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id_conta_bancaria, banco, apelido, ordem, ativo, espelhar_sistema
        """, (cod_empresa, banco, apelido, ordem, espelhar_sistema))
        conta = cur.fetchone()
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "conta_bancaria": conta}), 201


@financeiro_bp.route("/api/contas-bancarias/<int:id_conta_bancaria>", methods=["PUT"])
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def api_alterar_conta_bancaria(id_conta_bancaria):
    cod_empresa = str(session["cod_empresa"]).strip()
    dados = request.get_json(silent=True) or {}

    banco = (dados.get("banco") or "").strip()
    apelido = (dados.get("apelido") or "").strip() or None
    ordem = dados.get("ordem", 10)
    ativo = bool(dados.get("ativo", True))
    espelhar_sistema = bool(dados.get("espelhar_sistema", False))

    if not banco:
        return jsonify({"ok": False, "erro": "Informe o banco."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            UPDATE contas_bancarias
            SET banco = %s, apelido = %s, ordem = %s, ativo = %s,
                espelhar_sistema = %s, atualizado_em = NOW()
            WHERE id_conta_bancaria = %s AND cod_empresa = %s
            RETURNING id_conta_bancaria, banco, apelido, ordem, ativo, espelhar_sistema
        """, (banco, apelido, ordem, ativo, espelhar_sistema, id_conta_bancaria, cod_empresa))
        conta = cur.fetchone()

        if not conta:
            conn.rollback()
            return jsonify({"ok": False, "erro": "Conta bancária não encontrada."}), 404

        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "conta_bancaria": conta})


@financeiro_bp.route("/api/contas-bancarias/<int:id_conta_bancaria>", methods=["DELETE"])
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def api_excluir_conta_bancaria(id_conta_bancaria):
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE contas_bancarias
            SET ativo = FALSE, atualizado_em = NOW()
            WHERE id_conta_bancaria = %s AND cod_empresa = %s
        """, (id_conta_bancaria, cod_empresa))

        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({"ok": False, "erro": "Conta bancária não encontrada."}), 404

        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True})


# =========================
# CADASTRO: INDICADORES DE RECEBÍVEIS
# =========================
@financeiro_bp.route("/api/indicadores-recebiveis", methods=["GET"])
@permissao_obrigatoria("FINANCEIRO", "CONSULTA_SALDOS")
def api_listar_indicadores_recebiveis():
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id_indicador_recebivel, nome, ordem, ativo
            FROM indicadores_recebiveis
            WHERE cod_empresa = %s
            ORDER BY ordem, nome
        """, (cod_empresa,))
        indicadores = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "indicadores_recebiveis": indicadores})


@financeiro_bp.route("/api/indicadores-recebiveis", methods=["POST"])
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def api_criar_indicador_recebivel():
    cod_empresa = str(session["cod_empresa"]).strip()
    dados = request.get_json(silent=True) or {}

    nome = (dados.get("nome") or "").strip()
    ordem = dados.get("ordem", 10)

    if not nome:
        return jsonify({"ok": False, "erro": "Informe o nome do indicador."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            INSERT INTO indicadores_recebiveis (cod_empresa, nome, ordem)
            VALUES (%s, %s, %s)
            RETURNING id_indicador_recebivel, nome, ordem, ativo
        """, (cod_empresa, nome, ordem))
        indicador = cur.fetchone()
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "indicador_recebivel": indicador}), 201


@financeiro_bp.route("/api/indicadores-recebiveis/<int:id_indicador_recebivel>", methods=["PUT"])
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def api_alterar_indicador_recebivel(id_indicador_recebivel):
    cod_empresa = str(session["cod_empresa"]).strip()
    dados = request.get_json(silent=True) or {}

    nome = (dados.get("nome") or "").strip()
    ordem = dados.get("ordem", 10)
    ativo = bool(dados.get("ativo", True))

    if not nome:
        return jsonify({"ok": False, "erro": "Informe o nome do indicador."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            UPDATE indicadores_recebiveis
            SET nome = %s, ordem = %s, ativo = %s, atualizado_em = NOW()
            WHERE id_indicador_recebivel = %s AND cod_empresa = %s
            RETURNING id_indicador_recebivel, nome, ordem, ativo
        """, (nome, ordem, ativo, id_indicador_recebivel, cod_empresa))
        indicador = cur.fetchone()

        if not indicador:
            conn.rollback()
            return jsonify({"ok": False, "erro": "Indicador não encontrado."}), 404

        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "indicador_recebivel": indicador})


@financeiro_bp.route("/api/indicadores-recebiveis/<int:id_indicador_recebivel>", methods=["DELETE"])
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def api_excluir_indicador_recebivel(id_indicador_recebivel):
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE indicadores_recebiveis
            SET ativo = FALSE, atualizado_em = NOW()
            WHERE id_indicador_recebivel = %s AND cod_empresa = %s
        """, (id_indicador_recebivel, cod_empresa))

        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({"ok": False, "erro": "Indicador não encontrado."}), 404

        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True})


# =========================
# CADASTRO: PERÍODOS (COMPETÊNCIA — MÊS/ANO, INÍCIO E FIM)
# =========================
def _periodo_json(row):
    """Datas em ISO — o padrão do Flask (RFC 822) quebra o parse no JS."""
    if not row:
        return row
    return {
        "id_competencia": row["id_competencia"],
        "mes_ano": row["mes_ano"].isoformat(),
        "data_inicio": row["data_inicio"].isoformat(),
        "data_fim": row["data_fim"].isoformat(),
    }


@financeiro_bp.route("/api/competencias", methods=["GET"])
@permissao_obrigatoria("FINANCEIRO", "CONFIGURAR_SALDOS")
def api_listar_competencias():
    cod_empresa = str(session["cod_empresa"]).strip()
    ano = request.args.get("ano", type=int)

    sql = """
        SELECT id_competencia, mes_ano, data_inicio, data_fim
        FROM competencia_mes
        WHERE cod_empresa = %s
    """
    params = [cod_empresa]

    if ano:
        sql += " AND EXTRACT(YEAR FROM mes_ano) = %s"
        params.append(ano)

    sql += " ORDER BY mes_ano DESC"

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(sql, params)
        competencias = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "competencias": [_periodo_json(c) for c in competencias]})


def _validar_periodo(dados):
    """Devolve (mes_ano, data_inicio, data_fim) ou levanta ValueError."""
    mes_ano = (dados.get("mes_ano") or "").strip()
    data_inicio = (dados.get("data_inicio") or "").strip()
    data_fim = (dados.get("data_fim") or "").strip()

    if not mes_ano or not data_inicio or not data_fim:
        raise ValueError("Informe mês/ano, data de início e data de fim.")

    try:
        mes = datetime.strptime(mes_ano, "%Y-%m-%d").date().replace(day=1)
        inicio = datetime.strptime(data_inicio, "%Y-%m-%d").date()
        fim = datetime.strptime(data_fim, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("Datas inválidas.")

    if fim <= inicio:
        raise ValueError("A data de fim deve ser posterior à de início.")

    return mes, inicio, fim


@financeiro_bp.route("/api/competencias", methods=["POST"])
@permissao_obrigatoria("FINANCEIRO", "CONFIGURAR_SALDOS")
def api_criar_competencia():
    cod_empresa = str(session["cod_empresa"]).strip()
    dados = request.get_json(silent=True) or {}

    try:
        mes, inicio, fim = _validar_periodo(dados)
    except ValueError as e:
        return jsonify({"ok": False, "erro": str(e)}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            INSERT INTO competencia_mes (cod_empresa, mes_ano, data_inicio, data_fim)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (cod_empresa, mes_ano)
            DO UPDATE SET data_inicio = EXCLUDED.data_inicio,
                          data_fim = EXCLUDED.data_fim,
                          atualizado_em = NOW()
            RETURNING id_competencia, mes_ano, data_inicio, data_fim
        """, (cod_empresa, mes, inicio, fim))
        competencia = cur.fetchone()
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "competencia": _periodo_json(competencia)}), 201


@financeiro_bp.route("/api/competencias/<int:id_competencia>", methods=["PUT"])
@permissao_obrigatoria("FINANCEIRO", "CONFIGURAR_SALDOS")
def api_atualizar_competencia(id_competencia):
    cod_empresa = str(session["cod_empresa"]).strip()
    dados = request.get_json(silent=True) or {}

    try:
        mes, inicio, fim = _validar_periodo(dados)
    except ValueError as e:
        return jsonify({"ok": False, "erro": str(e)}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            UPDATE competencia_mes
               SET mes_ano = %s, data_inicio = %s, data_fim = %s, atualizado_em = NOW()
             WHERE id_competencia = %s AND cod_empresa = %s
            RETURNING id_competencia, mes_ano, data_inicio, data_fim
        """, (mes, inicio, fim, id_competencia, cod_empresa))
        competencia = cur.fetchone()
        if not competencia:
            conn.rollback()
            return jsonify({"ok": False, "erro": "Período não encontrado."}), 404
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "competencia": _periodo_json(competencia)})


@financeiro_bp.route("/api/competencias/<int:id_competencia>", methods=["DELETE"])
@permissao_obrigatoria("FINANCEIRO", "CONFIGURAR_SALDOS")
def api_excluir_competencia(id_competencia):
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            DELETE FROM competencia_mes
             WHERE id_competencia = %s AND cod_empresa = %s
        """, (id_competencia, cod_empresa))
        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({"ok": False, "erro": "Período não encontrado."}), 404
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True})


# =========================
# CADASTRO: FERIADOS (DIAS QUE A TELA DE SALDOS NÃO GERA)
# =========================
@financeiro_bp.route("/api/saldos/feriados", methods=["GET"])
@permissao_obrigatoria("FINANCEIRO", "CONSULTA_SALDOS")
def api_listar_feriados():
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id_feriado, data, descricao
              FROM saldos_feriados
             WHERE cod_empresa = %s
             ORDER BY data DESC
        """, (cod_empresa,))
        feriados = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "feriados": [
        {"id_feriado": f["id_feriado"], "data": f["data"].isoformat(), "descricao": f["descricao"]}
        for f in feriados
    ]})


@financeiro_bp.route("/api/saldos/feriados", methods=["POST"])
@permissao_obrigatoria("FINANCEIRO", "CONFIGURAR_SALDOS")
def api_criar_feriado():
    cod_empresa = str(session["cod_empresa"]).strip()
    dados = request.get_json(silent=True) or {}

    try:
        data_feriado = datetime.strptime((dados.get("data") or "").strip(), "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"ok": False, "erro": "Informe a data no formato YYYY-MM-DD."}), 400

    descricao = (dados.get("descricao") or "").strip() or None

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            INSERT INTO saldos_feriados (cod_empresa, data, descricao)
            VALUES (%s, %s, %s)
            ON CONFLICT (cod_empresa, data)
            DO UPDATE SET descricao = EXCLUDED.descricao, atualizado_em = NOW()
            RETURNING id_feriado, data, descricao
        """, (cod_empresa, data_feriado, descricao))
        feriado = cur.fetchone()
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "feriado": {
        "id_feriado": feriado["id_feriado"],
        "data": feriado["data"].isoformat(),
        "descricao": feriado["descricao"],
    }}), 201


@financeiro_bp.route("/api/saldos/feriados/<int:id_feriado>", methods=["DELETE"])
@permissao_obrigatoria("FINANCEIRO", "CONFIGURAR_SALDOS")
def api_excluir_feriado(id_feriado):
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            DELETE FROM saldos_feriados
             WHERE id_feriado = %s AND cod_empresa = %s
        """, (id_feriado, cod_empresa))
        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({"ok": False, "erro": "Feriado não encontrado."}), 404
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True})


# =========================
# CADASTRO: ACESSO POR ÁREA (QUEM PODE VER/LANÇAR CADA ÁREA NOS SALDOS)
# =========================
@financeiro_bp.route("/api/usuarios-areas-saldos", methods=["GET"])
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def api_listar_usuarios_areas_saldos():
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT uas.id_usuario_area_saldo, uas.id_usuario, u.nome AS nome_usuario,
                   uas.id_area, a.nome_area
            FROM usuarios_areas_saldos uas
            JOIN usuarios u ON u.id_usuario = uas.id_usuario
            JOIN areas a ON a.id_area = uas.id_area
            WHERE uas.cod_empresa = %s AND uas.ativo = TRUE
            ORDER BY u.nome, a.nome_area
        """, (cod_empresa,))
        acessos = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "acessos": acessos})


@financeiro_bp.route("/api/usuarios-areas-saldos", methods=["POST"])
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def api_conceder_acesso_area_saldos():
    cod_empresa = str(session["cod_empresa"]).strip()
    dados = request.get_json(silent=True) or {}

    id_usuario = dados.get("id_usuario")
    id_area = dados.get("id_area")

    if not id_usuario or not id_area:
        return jsonify({"ok": False, "erro": "Informe id_usuario e id_area."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            INSERT INTO usuarios_areas_saldos (cod_empresa, id_usuario, id_area)
            VALUES (%s, %s, %s)
            ON CONFLICT (cod_empresa, id_usuario, id_area)
            DO UPDATE SET ativo = TRUE, atualizado_em = NOW()
            RETURNING id_usuario_area_saldo, id_usuario, id_area
        """, (cod_empresa, id_usuario, id_area))
        acesso = cur.fetchone()
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "acesso": acesso}), 201


@financeiro_bp.route("/api/usuarios-areas-saldos/<int:id_usuario_area_saldo>", methods=["DELETE"])
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def api_revogar_acesso_area_saldos(id_usuario_area_saldo):
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE usuarios_areas_saldos
            SET ativo = FALSE, atualizado_em = NOW()
            WHERE id_usuario_area_saldo = %s AND cod_empresa = %s
        """, (id_usuario_area_saldo, cod_empresa))

        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({"ok": False, "erro": "Acesso não encontrado."}), 404

        conn.commit()
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True})


# =========================
# CADASTRO: LIBERAÇÃO TEMPORÁRIA DE DATAS ANTIGAS
# =========================
@financeiro_bp.route("/api/saldos/liberacao-temporaria", methods=["GET"])
@permissao_obrigatoria("FINANCEIRO", "CONSULTA_SALDOS")
def api_status_liberacao_temporaria():
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT sl.data_liberada_desde, sl.ativado_em, u.nome AS ativado_por
            FROM saldos_liberacao_temporaria sl
            LEFT JOIN usuarios u ON u.id_usuario = sl.id_usuario_ativou
            WHERE sl.cod_empresa = %s
            ORDER BY sl.ativado_em DESC
            LIMIT 1
        """, (cod_empresa,))
        liberacao = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    expira_em = None
    ativa = False
    if liberacao:
        # tudo em horário de Brasília — é o que a tela mostra e o que o
        # usuário usa para saber quanto tempo ainda tem
        expira_em = (_ativado_em_local(liberacao["ativado_em"])
                     + timedelta(hours=HORAS_LIBERACAO_SALDOS))
        ativa = _agora_local() < expira_em

    return jsonify({
        "ok": True,
        "ativa": ativa,
        "data_liberada_desde": liberacao["data_liberada_desde"].isoformat() if (liberacao and ativa) else None,
        "expira_em": expira_em.isoformat() if (liberacao and ativa) else None,
        "ativado_por": liberacao["ativado_por"] if (liberacao and ativa) else None,
        "data_minima_editavel": data_minima_editavel(cod_empresa).isoformat(),
    })


@financeiro_bp.route("/api/saldos/liberacao-temporaria", methods=["POST"])
@permissao_obrigatoria("FINANCEIRO", "CADASTRO_CONTAS_BANCARIAS")
def api_ativar_liberacao_temporaria():
    cod_empresa = str(session["cod_empresa"]).strip()
    id_usuario = session["id_usuario"]
    dados = request.get_json(silent=True) or {}

    data_liberada_desde = dados.get("data_liberada_desde")
    try:
        data_liberada_desde = datetime.strptime(data_liberada_desde, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return jsonify({"ok": False, "erro": "Informe data_liberada_desde no formato YYYY-MM-DD."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            INSERT INTO saldos_liberacao_temporaria (cod_empresa, data_liberada_desde, id_usuario_ativou)
            VALUES (%s, %s, %s)
            RETURNING id_liberacao, data_liberada_desde, ativado_em
        """, (cod_empresa, data_liberada_desde, id_usuario))
        liberacao = cur.fetchone()
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    expira_em = (_ativado_em_local(liberacao["ativado_em"])
                 + timedelta(hours=HORAS_LIBERACAO_SALDOS))
    return jsonify({
        "ok": True,
        "data_liberada_desde": liberacao["data_liberada_desde"].isoformat(),
        "expira_em": expira_em.isoformat(),
    }), 201


# =========================
# CONSULTA CONSOLIDADA
# =========================
def montar_bloco_dia(data_atual, contas, indicadores, codigos_filiais,
                      linhas_bancarias, linhas_recebiveis, linhas_informados,
                      linhas_bancarias_anterior, linhas_recebiveis_anterior,
                      inicio_competencia):

    def indexar(linhas, chave_id):
        mapa = defaultdict(dict)
        for r in linhas:
            mapa[r[chave_id]][int(r["cod_filial"])] = r
        return mapa

    bancarios_map = indexar(linhas_bancarias, "id_conta_bancaria")
    bancarios_map_anterior = indexar(linhas_bancarias_anterior, "id_conta_bancaria")
    recebiveis_map = indexar(linhas_recebiveis, "id_indicador_recebivel")
    recebiveis_map_anterior = indexar(linhas_recebiveis_anterior, "id_indicador_recebivel")
    informados_map = {int(r["cod_filial"]): r for r in linhas_informados}

    def montar_linhas(cadastros, chave_id, mapa, mapa_anterior, valor_banco_col, valor_sistema_col):
        linhas = []
        subtotal_banco = {f: 0.0 for f in codigos_filiais}
        subtotal_sistema = {f: 0.0 for f in codigos_filiais}
        subtotal_variacao = {f: 0.0 for f in codigos_filiais}

        for cadastro in cadastros:
            linhas_cadastro = mapa.get(cadastro[chave_id], {})
            linhas_cadastro_anterior = mapa_anterior.get(cadastro[chave_id], {})
            saldo_banco, saldo_sistema, diferenca, variacao = {}, {}, {}, {}

            for f in codigos_filiais:
                r = linhas_cadastro.get(f)
                r_anterior = linhas_cadastro_anterior.get(f)
                vb = float(r[valor_banco_col]) if r else 0.0
                vs = float(r[valor_sistema_col]) if r else 0.0
                vb_anterior = float(r_anterior[valor_banco_col]) if r_anterior else 0.0
                # calculado em Python (não via LAG do SQL): quando não há lançamento no dia
                # (r é None) o valor efetivo é 0, e a variação precisa refletir isso — o LAG
                # simplesmente pula o dia sem lançamento, o que zerava a variação por engano.
                var = vb - vb_anterior

                saldo_banco[f] = vb
                saldo_sistema[f] = vs
                diferenca[f] = vb - vs
                variacao[f] = var

                subtotal_banco[f] += vb
                subtotal_sistema[f] += vs
                subtotal_variacao[f] += var

            linha = {
                chave_id: cadastro[chave_id],
                "saldo_banco": saldo_banco,
                "saldo_sistema": saldo_sistema,
                "diferenca": diferenca,
                "variacao": variacao,
                "total_banco": sum(saldo_banco.values()),
                "total_sistema": sum(saldo_sistema.values()),
            }
            linha.update({k: v for k, v in cadastro.items() if k != chave_id})
            linhas.append(linha)

        subtotal = {
            "banco": subtotal_banco,
            "sistema": subtotal_sistema,
            "total_banco": sum(subtotal_banco.values()),
            "total_sistema": sum(subtotal_sistema.values()),
        }
        return linhas, subtotal, subtotal_variacao

    contas_bloco, subtotal_contas, variacao_contas = montar_linhas(
        contas, "id_conta_bancaria", bancarios_map, bancarios_map_anterior, "saldo_banco", "saldo_sistema"
    )
    recebiveis_bloco, subtotal_recebiveis, variacao_recebiveis = montar_linhas(
        indicadores, "id_indicador_recebivel", recebiveis_map, recebiveis_map_anterior, "valor_banco", "valor_sistema"
    )

    total_banco = {f: subtotal_contas["banco"][f] + subtotal_recebiveis["banco"][f] for f in codigos_filiais}
    total_sistema = {f: subtotal_contas["sistema"][f] + subtotal_recebiveis["sistema"][f] for f in codigos_filiais}
    variacao_total = {f: variacao_contas[f] + variacao_recebiveis[f] for f in codigos_filiais}

    valores_informados_bloco = {}
    for f in codigos_filiais:
        r = informados_map.get(f)
        perdas_sobras = float(r["perdas_sobras"]) if r else 0.0
        extras = float(r["extras"]) if r else 0.0
        emprestimos_devolucoes = float(r["emprestimos_devolucoes"]) if r else 0.0
        despesas = float(r["despesas"]) if r else 0.0
        despesas_caixa = float(r["despesas_caixa"]) if r else 0.0

        valores_informados_bloco[f] = {
            "perdas_sobras": perdas_sobras,
            "extras": extras,
            "emprestimos_devolucoes": emprestimos_devolucoes,
            "despesas": despesas,
            "despesas_caixa": despesas_caixa,
            "variacao_final": variacao_total[f] + perdas_sobras + extras + emprestimos_devolucoes - despesas - despesas_caixa,
        }

    return {
        "data": data_atual.isoformat(),
        "dia_semana": DIAS_SEMANA_PT[data_atual.weekday()],
        "inicio_competencia": inicio_competencia,
        "contas_bancarias": contas_bloco,
        "subtotal_contas": subtotal_contas,
        "recebiveis": recebiveis_bloco,
        "subtotal_recebiveis": subtotal_recebiveis,
        "total": {
            "banco": total_banco,
            "sistema": total_sistema,
            "total_banco": sum(total_banco.values()),
            "total_sistema": sum(total_sistema.values()),
            "total_diferenca": sum(total_banco.values()) - sum(total_sistema.values()),
        },
        "variacao": {
            "contas": variacao_contas,
            "recebiveis": variacao_recebiveis,
            "total": variacao_total,
        },
        "valores_informados": valores_informados_bloco,
    }


def _areas_com_filiais(cur, cod_empresa):
    """Áreas visíveis ao usuário, cada uma com suas filiais, na ordem da tela."""
    cur.execute("""
        SELECT id_area, nome_area
          FROM areas
         WHERE cod_empresa = %s AND ativo = TRUE
         ORDER BY nome_area
    """, (cod_empresa,))
    areas = cur.fetchall()

    if str(session.get("tipo_global") or "").strip().lower() != "superusuario":
        areas_ok = areas_permitidas_usuario(cur, cod_empresa, session["id_usuario"])
        areas = [a for a in areas if a["id_area"] in areas_ok]

    resultado = []
    for a in areas:
        resultado.append({
            "id_area": a["id_area"],
            "nome_area": a["nome_area"],
            "filiais": [
                {"cod_filial": int(f["cod_filial"]), "nome_filial": f["nome_filial"]}
                for f in filiais_da_area(cur, cod_empresa, a["id_area"])
            ],
        })
    return resultado


@financeiro_bp.route("/saldos/antecipacao-dividendos")
@permissao_obrigatoria("FINANCEIRO", "ANTECIPACAO_DIVIDENDOS")
def antecipacao_dividendos():
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        areas = _areas_com_filiais(cur, cod_empresa)
    finally:
        cur.close()
        conn.close()

    return render_template(
        "antecipacao_dividendos.html",
        areas=areas,
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_saldos"),
    )


@financeiro_bp.route("/api/antecipacao-dividendos", methods=["GET"])
@permissao_obrigatoria("FINANCEIRO", "ANTECIPACAO_DIVIDENDOS")
def api_listar_antecipacao_dividendos():
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        areas = _areas_com_filiais(cur, cod_empresa)
        codigos = [f["cod_filial"] for a in areas for f in a["filiais"]]

        lancamentos = []
        if codigos:
            cur.execute("""
                SELECT data, cod_filial, valor
                  FROM antecipacao_dividendos
                 WHERE cod_empresa = %s AND cod_filial = ANY(%s)
                 ORDER BY data DESC
            """, (cod_empresa, codigos))
            lancamentos = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    # Uma linha por data, com o valor de cada filial.
    por_data = OrderedDict()
    for r in lancamentos:
        linha = por_data.setdefault(r["data"], {})
        linha[str(int(r["cod_filial"]))] = float(r["valor"] or 0)

    return jsonify({
        "ok": True,
        "areas": areas,
        "linhas": [{"data": d.isoformat(), "valores": v} for d, v in por_data.items()],
    })


@financeiro_bp.route("/api/antecipacao-dividendos", methods=["PUT"])
@permissao_obrigatoria("FINANCEIRO", "ANTECIPACAO_DIVIDENDOS")
def api_salvar_antecipacao_dividendos():
    cod_empresa = str(session["cod_empresa"]).strip()
    dados = request.get_json(silent=True) or {}

    data_str = (dados.get("data") or "").strip()
    valores = dados.get("valores") or {}

    try:
        data = datetime.strptime(data_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"ok": False, "erro": "Data inválida."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        areas = _areas_com_filiais(cur, cod_empresa)
        permitidas = {f["cod_filial"] for a in areas for f in a["filiais"]}

        for cod_filial, valor in valores.items():
            cod_filial = int(cod_filial)
            if cod_filial not in permitidas:
                conn.rollback()
                return jsonify({"ok": False, "erro": "Filial fora das suas áreas."}), 403

            cur.execute("""
                INSERT INTO antecipacao_dividendos
                    (cod_empresa, cod_filial, data, valor, usuario_lancamento)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (cod_empresa, cod_filial, data)
                DO UPDATE SET valor = EXCLUDED.valor,
                              usuario_lancamento = EXCLUDED.usuario_lancamento,
                              atualizado_em = NOW()
            """, (cod_empresa, cod_filial, data, float(valor or 0), session["id_usuario"]))

        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True})


@financeiro_bp.route("/api/antecipacao-dividendos", methods=["DELETE"])
@permissao_obrigatoria("FINANCEIRO", "ANTECIPACAO_DIVIDENDOS")
def api_excluir_antecipacao_dividendos():
    cod_empresa = str(session["cod_empresa"]).strip()
    data_str = (request.args.get("data") or "").strip()

    try:
        data = datetime.strptime(data_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"ok": False, "erro": "Data inválida."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        areas = _areas_com_filiais(cur, cod_empresa)
        codigos = [f["cod_filial"] for a in areas for f in a["filiais"]]
        cur.execute("""
            DELETE FROM antecipacao_dividendos
             WHERE cod_empresa = %s AND data = %s AND cod_filial = ANY(%s)
        """, (cod_empresa, data, codigos))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True})


def _arredondar_linhas(*grupos):
    """Arredonda para centavos na saída: somas em float acumulam resíduo e as
    duas telas de variação chegavam a divergir 1 centavo entre si."""
    for grupo in grupos:
        for linha in (grupo if isinstance(grupo, list) else [grupo]):
            for momento in ("inicio", "fim", "variacao"):
                if momento in linha:
                    linha[momento] = {k: round(v, 2) for k, v in linha[momento].items()}


class ErroConsulta(Exception):
    """Erro de negócio das consultas de variação (vira 400/403 no endpoint)."""

    def __init__(self, mensagem, status=400):
        super().__init__(mensagem)
        self.status = status


def _coletar_variacoes(cod_empresa, id_competencia, recuo=0):
    """Dados crus das duas telas de variação: período, janela e saldos.

    A janela comparada usa só dias em que TODAS as áreas visíveis lançaram —
    nas duas pontas, senão área atrasada entra zerada e inventa variação.

    `recuo` empurra a ponta final N dias completos para trás (a setinha da
    tela): o dia mais recente costuma ainda estar sendo digitado.
    """

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if id_competencia:
            cur.execute("""
                SELECT id_competencia, mes_ano, data_inicio, data_fim
                  FROM competencia_mes
                 WHERE cod_empresa = %s AND id_competencia = %s
            """, (cod_empresa, id_competencia))
        else:
            cur.execute("""
                SELECT id_competencia, mes_ano, data_inicio, data_fim
                  FROM competencia_mes
                 WHERE cod_empresa = %s
                 ORDER BY mes_ano DESC LIMIT 1
            """, (cod_empresa,))
        periodo = cur.fetchone()

        if not periodo:
            raise ErroConsulta("Nenhum período cadastrado.")

        data_inicio = periodo["data_inicio"]
        data_fim = periodo["data_fim"]

        cur.execute("""
            SELECT id_area, nome_area
              FROM areas
             WHERE cod_empresa = %s AND ativo = TRUE
             ORDER BY nome_area
        """, (cod_empresa,))
        areas = cur.fetchall()

        if str(session.get("tipo_global") or "").strip().lower() != "superusuario":
            areas_ok = areas_permitidas_usuario(cur, cod_empresa, session["id_usuario"])
            areas = [a for a in areas if a["id_area"] in areas_ok]

        if not areas:
            raise ErroConsulta("Nenhuma área liberada para você.", 403)

        ids_areas = [a["id_area"] for a in areas]

        # Filiais de cada área — a soma da área é a soma das filiais dela.
        cur.execute("""
            SELECT id_area, cod_filial
              FROM areas_filiais
             WHERE cod_empresa = %s AND id_area = ANY(%s)
        """, (cod_empresa, ids_areas))
        area_da_filial = {int(r["cod_filial"]): r["id_area"] for r in cur.fetchall()}
        codigos_filiais = list(area_da_filial.keys())

        if not codigos_filiais:
            raise ErroConsulta("Nenhuma filial vinculada às áreas.")

        # Última data do período com lançamento de TODAS as áreas visíveis.
        cur.execute("""
            SELECT s.data, af.id_area
              FROM saldos_bancarios s
              JOIN areas_filiais af
                ON af.cod_empresa = s.cod_empresa AND af.cod_filial = s.cod_filial
             WHERE s.cod_empresa = %s AND af.id_area = ANY(%s)
               AND s.data BETWEEN %s AND %s
             GROUP BY s.data, af.id_area
        """, (cod_empresa, ids_areas, data_inicio, data_fim))

        areas_por_data = defaultdict(set)
        for r in cur.fetchall():
            areas_por_data[r["data"]].add(r["id_area"])

        # A mesma regra vale nas duas pontas: comparar só dias em que todas as
        # áreas lançaram. Se o primeiro dia do período está incompleto (caso do
        # 31/07/26, só a Área I), usa o primeiro dia completo dali em diante —
        # senão a área atrasada entraria zerada e inventaria variação.
        # Feriados do período: sábado, domingo e feriado não servem de ponta do
        # comparativo (é dia sem movimento — encostaria o saldo nele mesmo).
        cur.execute("""
            SELECT data FROM saldos_feriados
             WHERE cod_empresa = %s AND data BETWEEN %s AND %s
        """, (cod_empresa, data_inicio, data_fim))
        feriados = {linha["data"] for linha in cur.fetchall()}

        datas_completas = sorted(d for d, ids in areas_por_data.items()
                                 if ids >= set(ids_areas) and eh_dia_util(d, feriados))
        data_inicial_efetiva = datas_completas[0] if datas_completas else data_inicio

        # A ponta final nunca pode encostar na inicial: as candidatas são as
        # datas depois dela (com uma só data completa, início = fim, como antes).
        candidatas_fim = datas_completas[1:] or datas_completas
        recuo = max(0, min(int(recuo or 0), len(candidatas_fim) - 1)) if candidatas_fim else 0
        data_final = candidatas_fim[-1 - recuo] if candidatas_fim else None
        pode_recuar = recuo + 1 < len(candidatas_fim)

        cur.execute("""
            SELECT id_indicador_recebivel, nome
              FROM indicadores_recebiveis
             WHERE cod_empresa = %s AND ativo = TRUE
             ORDER BY ordem, nome
        """, (cod_empresa,))
        indicadores = cur.fetchall()

        datas_consulta = [d for d in (data_inicial_efetiva, data_final) if d]

        cur.execute("""
            SELECT data, cod_filial, SUM(saldo_banco) AS valor
              FROM saldos_bancarios
             WHERE cod_empresa = %s AND cod_filial = ANY(%s) AND data = ANY(%s)
             GROUP BY data, cod_filial
        """, (cod_empresa, codigos_filiais, datas_consulta))
        bancarios = cur.fetchall()

        cur.execute("""
            SELECT data, cod_filial, id_indicador_recebivel, SUM(valor_banco) AS valor
              FROM saldos_recebiveis
             WHERE cod_empresa = %s AND cod_filial = ANY(%s) AND data = ANY(%s)
             GROUP BY data, cod_filial, id_indicador_recebivel
        """, (cod_empresa, codigos_filiais, datas_consulta))
        recebiveis = cur.fetchall()

        # Antecipação de dividendos retirada DENTRO do intervalo comparado:
        # explica a queda de saldo, por isso soma depois da variação.
        antecipacoes = []
        if data_final:
            cur.execute("""
                SELECT cod_filial, SUM(valor) AS valor
                  FROM antecipacao_dividendos
                 WHERE cod_empresa = %s AND cod_filial = ANY(%s)
                   AND data > %s AND data <= %s
                 GROUP BY cod_filial
            """, (cod_empresa, codigos_filiais, data_inicial_efetiva, data_final))
            antecipacoes = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return {
        "periodo": periodo,
        "areas": areas,
        "ids_areas": ids_areas,
        "area_da_filial": area_da_filial,
        "indicadores": indicadores,
        "data_inicial_efetiva": data_inicial_efetiva,
        "data_final": data_final,
        "recuo": recuo,
        "pode_recuar": pode_recuar,
        "bancarios": bancarios,
        "recebiveis": recebiveis,
        "antecipacoes": antecipacoes,
    }


@financeiro_bp.route("/api/saldos/variacoes", methods=["GET"])
@permissao_obrigatoria("FINANCEIRO", "CONSULTAR_VARIACOES_SALDOS")
def api_consultar_variacoes():
    """Resumo por área na primeira data do período × na última data fechada.

    "Última data fechada" é o dia mais recente do período em que TODAS as áreas
    visíveis têm lançamento — área atrasada não entra zerada no comparativo.
    """
    cod_empresa = str(session["cod_empresa"]).strip()
    id_competencia = request.args.get("id_competencia", type=int)
    recuo = request.args.get("recuo", type=int) or 0

    try:
        dados = _coletar_variacoes(cod_empresa, id_competencia, recuo)
    except ErroConsulta as e:
        return jsonify({"ok": False, "erro": str(e)}), e.status

    periodo = dados["periodo"]
    areas = dados["areas"]
    ids_areas = dados["ids_areas"]
    area_da_filial = dados["area_da_filial"]
    indicadores = dados["indicadores"]
    data_inicial_efetiva = dados["data_inicial_efetiva"]
    data_final = dados["data_final"]
    bancarios = dados["bancarios"]
    recebiveis = dados["recebiveis"]
    antecipacoes = dados["antecipacoes"]
    data_inicio = periodo["data_inicio"]
    data_fim = periodo["data_fim"]

    # chave: (data, rótulo, id_area) -> valor
    acumulado = defaultdict(float)

    for r in bancarios:
        id_area = area_da_filial.get(int(r["cod_filial"]))
        acumulado[(r["data"], "contas", id_area)] += float(r["valor"] or 0)

    for r in recebiveis:
        id_area = area_da_filial.get(int(r["cod_filial"]))
        chave = ("ind", r["id_indicador_recebivel"])
        acumulado[(r["data"], chave, id_area)] += float(r["valor"] or 0)

    def montar_linha(rotulo, chave):
        linha = {"rotulo": rotulo, "inicio": {}, "fim": {}, "variacao": {}}
        for id_area in ids_areas:
            v_ini = acumulado.get((data_inicial_efetiva, chave, id_area), 0.0)
            v_fim = acumulado.get((data_final, chave, id_area), 0.0) if data_final else 0.0
            linha["inicio"][str(id_area)] = v_ini
            linha["fim"][str(id_area)] = v_fim
            linha["variacao"][str(id_area)] = v_fim - v_ini
        for momento in ("inicio", "fim", "variacao"):
            linha[momento]["total"] = sum(linha[momento][str(a)] for a in ids_areas)
        return linha

    linhas = [montar_linha("Contas", "contas")]
    linhas += [montar_linha(i["nome"], ("ind", i["id_indicador_recebivel"])) for i in indicadores]

    total = {"rotulo": "Total", "inicio": {}, "fim": {}, "variacao": {}}
    for momento in ("inicio", "fim", "variacao"):
        for coluna in [str(a) for a in ids_areas] + ["total"]:
            total[momento][coluna] = sum(l[momento][coluna] for l in linhas)

    # A antecipação não é saldo: entra como linha própria, somada ao total.
    antecipacao_por_area = defaultdict(float)
    for r in antecipacoes:
        id_area = area_da_filial.get(int(r["cod_filial"]))
        antecipacao_por_area[id_area] += float(r["valor"] or 0)

    antecipacao = {"rotulo": "Antecipação Dividendos", "inicio": {}, "fim": {}, "variacao": {}}
    com_dividendos = {"rotulo": "Total com Dividendos", "inicio": {}, "fim": {}, "variacao": {}}
    for coluna in [str(a) for a in ids_areas] + ["total"]:
        valor = (sum(antecipacao_por_area.values()) if coluna == "total"
                 else antecipacao_por_area.get(int(coluna), 0.0))
        antecipacao["inicio"][coluna] = 0.0
        antecipacao["fim"][coluna] = valor
        antecipacao["variacao"][coluna] = valor
        com_dividendos["inicio"][coluna] = total["inicio"][coluna]
        com_dividendos["fim"][coluna] = total["fim"][coluna] + valor
        com_dividendos["variacao"][coluna] = total["variacao"][coluna] + valor

    # Projeção: a variação já apurada, esticada para o período inteiro.
    # Ex.: 31/07 -> 04/08 são 4 dias decorridos; o período (31/07 a 31/08) tem 31.
    dias_decorridos = (data_final - data_inicial_efetiva).days if data_final else 0
    dias_periodo = (data_fim - periodo["data_inicio"]).days

    projecao = {"rotulo": "Projeção", "variacao": {}}
    for coluna in [str(a) for a in ids_areas] + ["total"]:
        projecao["variacao"][coluna] = (
            round(com_dividendos["variacao"][coluna], 2) / dias_decorridos * dias_periodo
            if dias_decorridos else 0.0
        )

    _arredondar_linhas(linhas, total, antecipacao, com_dividendos, projecao)

    return jsonify({
        "ok": True,
        "periodo": {
            "id_competencia": periodo["id_competencia"],
            "mes_ano": periodo["mes_ano"].isoformat(),
            "data_inicio": data_inicio.isoformat(),
            "data_fim": data_fim.isoformat(),
        },
        "data_inicio": data_inicial_efetiva.isoformat(),
        "data_final": data_final.isoformat() if data_final else None,
        "recuo": dados["recuo"],
        "pode_recuar": dados["pode_recuar"],
        "areas": [{"id_area": a["id_area"], "nome_area": a["nome_area"]} for a in areas],
        "linhas": linhas,
        "total": total,
        "antecipacao": antecipacao,
        "total_com_dividendos": com_dividendos,
        "projecao": projecao,
        "dias_decorridos": dias_decorridos,
        "dias_periodo": dias_periodo,
    })


@financeiro_bp.route("/api/saldos/variacoes-filial", methods=["GET"])
@permissao_obrigatoria("FINANCEIRO", "CONSULTAR_VARIACOES_FILIAL")
def api_consultar_variacoes_filial():
    """Mesmo comparativo da tela de variações, aberto filial a filial."""
    cod_empresa = str(session["cod_empresa"]).strip()
    id_competencia = request.args.get("id_competencia", type=int)

    try:
        dados = _coletar_variacoes(cod_empresa, id_competencia)
    except ErroConsulta as e:
        return jsonify({"ok": False, "erro": str(e)}), e.status

    periodo = dados["periodo"]
    ids_areas = dados["ids_areas"]
    area_da_filial = dados["area_da_filial"]
    data_inicial_efetiva = dados["data_inicial_efetiva"]
    data_final = dados["data_final"]

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        areas = _areas_com_filiais(cur, cod_empresa)
    finally:
        cur.close()
        conn.close()

    # Colunas na ordem da tela: filiais da área 1, subtotal, filiais da área 2,
    # subtotal, e o total geral no fim.
    colunas = []
    for a in areas:
        colunas += [str(f["cod_filial"]) for f in a["filiais"]]
        colunas.append(f"sub_{a['id_area']}")
    colunas.append("total")

    filiais_da_area = {a["id_area"]: [f["cod_filial"] for f in a["filiais"]] for a in areas}

    # chave: (data, linha, cod_filial) -> valor
    acumulado = defaultdict(float)
    for r in dados["bancarios"]:
        acumulado[(r["data"], "contas", int(r["cod_filial"]))] += float(r["valor"] or 0)
    for r in dados["recebiveis"]:
        chave = ("ind", r["id_indicador_recebivel"])
        acumulado[(r["data"], chave, int(r["cod_filial"]))] += float(r["valor"] or 0)

    def preencher(dic, valor_da_filial):
        """Preenche filiais, subtotais por área e total geral."""
        total = 0.0
        for a in areas:
            subtotal = 0.0
            for cod_filial in filiais_da_area[a["id_area"]]:
                v = valor_da_filial(cod_filial)
                dic[str(cod_filial)] = v
                subtotal += v
            dic[f"sub_{a['id_area']}"] = subtotal
            total += subtotal
        dic["total"] = total

    def montar_linha(rotulo, chave):
        linha = {"rotulo": rotulo, "inicio": {}, "fim": {}, "variacao": {}}
        preencher(linha["inicio"], lambda c: acumulado.get((data_inicial_efetiva, chave, c), 0.0))
        preencher(linha["fim"], lambda c: (
            acumulado.get((data_final, chave, c), 0.0) if data_final else 0.0))
        preencher(linha["variacao"], lambda c: (
            (acumulado.get((data_final, chave, c), 0.0) if data_final else 0.0)
            - acumulado.get((data_inicial_efetiva, chave, c), 0.0)))
        return linha

    linhas = [montar_linha("Contas", "contas")]
    linhas += [montar_linha(i["nome"], ("ind", i["id_indicador_recebivel"]))
               for i in dados["indicadores"]]

    total = {"rotulo": "Total", "inicio": {}, "fim": {}, "variacao": {}}
    for momento in ("inicio", "fim", "variacao"):
        for coluna in colunas:
            total[momento][coluna] = sum(l[momento][coluna] for l in linhas)

    antecipacao_por_filial = defaultdict(float)
    for r in dados["antecipacoes"]:
        antecipacao_por_filial[int(r["cod_filial"])] += float(r["valor"] or 0)

    antecipacao = {"rotulo": "Antecipação Dividendos", "inicio": {}, "fim": {}, "variacao": {}}
    preencher(antecipacao["inicio"], lambda c: 0.0)
    preencher(antecipacao["fim"], lambda c: antecipacao_por_filial.get(c, 0.0))
    preencher(antecipacao["variacao"], lambda c: antecipacao_por_filial.get(c, 0.0))

    com_dividendos = {"rotulo": "Total com Dividendos", "inicio": {}, "fim": {}, "variacao": {}}
    for momento in ("inicio", "fim", "variacao"):
        for coluna in colunas:
            com_dividendos[momento][coluna] = total[momento][coluna] + antecipacao[momento][coluna]

    dias_decorridos = (data_final - data_inicial_efetiva).days if data_final else 0
    dias_periodo = (periodo["data_fim"] - periodo["data_inicio"]).days

    projecao = {"rotulo": "Projeção", "variacao": {}}
    for coluna in colunas:
        projecao["variacao"][coluna] = (
            round(com_dividendos["variacao"][coluna], 2) / dias_decorridos * dias_periodo
            if dias_decorridos else 0.0
        )

    _arredondar_linhas(linhas, total, antecipacao, com_dividendos, projecao)

    return jsonify({
        "ok": True,
        "periodo": {
            "id_competencia": periodo["id_competencia"],
            "mes_ano": periodo["mes_ano"].isoformat(),
            "data_inicio": periodo["data_inicio"].isoformat(),
            "data_fim": periodo["data_fim"].isoformat(),
        },
        "data_inicio": data_inicial_efetiva.isoformat(),
        "data_final": data_final.isoformat() if data_final else None,
        "areas": areas,
        "colunas": colunas,
        "linhas": linhas,
        "total": total,
        "antecipacao": antecipacao,
        "total_com_dividendos": com_dividendos,
        "projecao": projecao,
        "dias_decorridos": dias_decorridos,
        "dias_periodo": dias_periodo,
    })


@financeiro_bp.route("/saldos/variacoes-filial")
@permissao_obrigatoria("FINANCEIRO", "CONSULTAR_VARIACOES_FILIAL")
def consultar_variacoes_filial():
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id_competencia, mes_ano, data_inicio, data_fim
              FROM competencia_mes
             WHERE cod_empresa = %s
             ORDER BY mes_ano DESC
        """, (cod_empresa,))
        periodos = [_periodo_json(p) for p in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

    return render_template(
        "variacoes_saldos_filial.html",
        periodos=periodos,
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session["nome_empresa"],
        url_voltar=url_for("financeiro.menu_saldos"),
    )


@financeiro_bp.route("/api/saldos", methods=["GET"])
@permissao_obrigatoria("FINANCEIRO", "CONSULTA_SALDOS")
def api_consultar_saldos():
    cod_empresa = str(session["cod_empresa"]).strip()
    id_area = request.args.get("id_area", type=int)

    if not id_area:
        return jsonify({"ok": False, "erro": "Informe id_area."}), 400

    try:
        data_inicio = datetime.strptime(request.args.get("data_inicio", ""), "%Y-%m-%d").date()
        data_fim = datetime.strptime(request.args.get("data_fim", ""), "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"ok": False, "erro": "Informe data_inicio e data_fim no formato YYYY-MM-DD."}), 400

    if data_inicio > data_fim:
        return jsonify({"ok": False, "erro": "data_inicio não pode ser maior que data_fim."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        filiais = filiais_permitidas_usuario(cur, cod_empresa, id_area)
        if not filiais:
            return jsonify({"ok": False, "erro": "Nenhuma filial disponível para esta área."}), 404

        codigos_filiais = [int(f["cod_filial"]) for f in filiais]

        # a tela só gera dia útil; a variação de cada um encosta no dia útil
        # anterior, que pode estar fora do período pedido (sexta ← segunda)
        cur.execute("""
            SELECT data FROM saldos_feriados
             WHERE cod_empresa = %s AND data BETWEEN %s AND %s
        """, (cod_empresa, data_inicio - timedelta(days=60), data_fim))
        feriados = {linha["data"] for linha in cur.fetchall()}

        dias_uteis = []
        data_atual = data_inicio
        while data_atual <= data_fim:
            if eh_dia_util(data_atual, feriados):
                dias_uteis.append((data_atual, dia_util_anterior(data_atual, feriados)))
            data_atual += timedelta(days=1)

        datas_consultadas = sorted({d for par in dias_uteis for d in par})
        if not datas_consultadas:
            return jsonify({
                "ok": True,
                "id_area": id_area,
                "periodo": {"data_inicio": data_inicio.isoformat(), "data_fim": data_fim.isoformat()},
                "filiais": [{"cod_filial": int(f["cod_filial"]), "nome_filial": f["nome_filial"]} for f in filiais],
                "dias": [],
            })

        cur.execute("""
            SELECT id_conta_bancaria, banco, apelido, ordem, espelhar_sistema
            FROM contas_bancarias
            WHERE cod_empresa = %s AND ativo = TRUE
            ORDER BY ordem, banco
        """, (cod_empresa,))
        contas = cur.fetchall()

        cur.execute("""
            SELECT id_indicador_recebivel, nome, ordem, origem_estoque
            FROM indicadores_recebiveis
            WHERE cod_empresa = %s AND ativo = TRUE
            ORDER BY ordem, nome
        """, (cod_empresa,))
        indicadores = cur.fetchall()

        cur.execute("""
            SELECT cod_filial, data, id_conta_bancaria, saldo_banco, saldo_sistema
            FROM saldos_bancarios
            WHERE cod_empresa = %s AND cod_filial = ANY(%s) AND data = ANY(%s)
        """, (cod_empresa, codigos_filiais, datas_consultadas))
        linhas_bancarios = cur.fetchall()

        cur.execute("""
            SELECT cod_filial, data, id_indicador_recebivel, valor_banco, valor_sistema
            FROM saldos_recebiveis
            WHERE cod_empresa = %s AND cod_filial = ANY(%s) AND data = ANY(%s)
        """, (cod_empresa, codigos_filiais, datas_consultadas))
        linhas_recebiveis = cur.fetchall()

        cur.execute("""
            SELECT cod_filial, data, perdas_sobras, extras, emprestimos_devolucoes, despesas, despesas_caixa
            FROM valores_informados
            WHERE cod_empresa = %s AND cod_filial = ANY(%s) AND data BETWEEN %s AND %s
        """, (cod_empresa, codigos_filiais, data_inicio, data_fim))
        linhas_informados = cur.fetchall()

        cur.execute("""
            SELECT data_inicio
            FROM competencia_mes
            WHERE cod_empresa = %s AND data_inicio BETWEEN %s AND %s
        """, (cod_empresa, data_inicio, data_fim))
        datas_inicio_competencia = {r["data_inicio"] for r in cur.fetchall()}

        # Importar valores de estoque exige tudo bloqueado em Operações ATÉ a
        # data — um dia aberto lá atrás ainda muda compras e trânsito.
        datas_bloqueadas_operacoes = {
            d for d in datas_consultadas
            if not datas_bloqueio_pendentes(cur, cod_empresa, d)
        }

        cur.execute("""
            SELECT data, id_indicador_recebivel
            FROM saldos_importacoes_estoque
            WHERE cod_empresa = %s AND id_area = %s AND data = ANY(%s)
        """, (cod_empresa, id_area, datas_consultadas))
        importados = {(r["data"], r["id_indicador_recebivel"]) for r in cur.fetchall()}
    finally:
        cur.close()
        conn.close()

    bancarios_por_dia = defaultdict(list)
    for r in linhas_bancarios:
        bancarios_por_dia[r["data"]].append(r)

    recebiveis_por_dia = defaultdict(list)
    for r in linhas_recebiveis:
        recebiveis_por_dia[r["data"]].append(r)

    informados_por_dia = defaultdict(list)
    for r in linhas_informados:
        informados_por_dia[r["data"]].append(r)

    dias = []
    for data_atual, data_anterior in dias_uteis:
        bloco = montar_bloco_dia(
            data_atual, contas, indicadores, codigos_filiais,
            bancarios_por_dia.get(data_atual, []),
            recebiveis_por_dia.get(data_atual, []),
            informados_por_dia.get(data_atual, []),
            bancarios_por_dia.get(data_anterior, []),
            recebiveis_por_dia.get(data_anterior, []),
            data_atual in datas_inicio_competencia,
        )
        # Importação dos valores de estoque (Operações): só quando a data está
        # bloqueada lá; importado = linha travada e botão "Desfazer importação".
        bloco["bloqueio_operacoes"] = data_atual in datas_bloqueadas_operacoes
        for linha in bloco["recebiveis"]:
            linha["importado"] = (data_atual, linha["id_indicador_recebivel"]) in importados
        dias.append(bloco)

    return jsonify({
        "ok": True,
        "id_area": id_area,
        "periodo": {"data_inicio": data_inicio.isoformat(), "data_fim": data_fim.isoformat()},
        "filiais": [{"cod_filial": int(f["cod_filial"]), "nome_filial": f["nome_filial"]} for f in filiais],
        "dias": dias,
    })


# =========================
# LANÇAMENTO (EDIÇÃO MANUAL — UPSERT EM LOTE)
# =========================
def validar_lancamento_data(dados):
    data_str = dados.get("data")
    try:
        return datetime.strptime(data_str, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


@financeiro_bp.route("/api/saldos/bancarios", methods=["PUT"])
@permissao_obrigatoria("FINANCEIRO", "LANCAMENTO_SALDOS")
def api_lancar_saldos_bancarios():
    cod_empresa = str(session["cod_empresa"]).strip()
    id_usuario = session["id_usuario"]
    dados = request.get_json(silent=True) or {}

    data_lancamento = validar_lancamento_data(dados)
    lancamentos = dados.get("lancamentos") or []

    if not data_lancamento:
        return jsonify({"ok": False, "erro": "Informe a data no formato YYYY-MM-DD."}), 400
    if not lancamentos:
        return jsonify({"ok": False, "erro": "Informe ao menos um lançamento."}), 400

    data_minima = data_minima_editavel(cod_empresa)
    if data_lancamento < data_minima:
        return jsonify({"ok": False, "erro": f"Data bloqueada para edição. Só é possível lançar a partir de {data_minima.strftime('%d/%m/%Y')}."}), 403

    conn = get_connection()
    cur = conn.cursor()
    try:
        filiais_ok = cod_filiais_permitidas_lancamento(cur, cod_empresa)

        # contas marcadas no cadastro para espelhar o saldo no lado do sistema
        cur.execute("""
            SELECT id_conta_bancaria FROM contas_bancarias
            WHERE cod_empresa = %s AND espelhar_sistema = TRUE
        """, (cod_empresa,))
        contas_espelhadas = {linha[0] for linha in cur.fetchall()}

        registros = []
        for item in lancamentos:
            try:
                cod_filial = int(item["cod_filial"])
                id_conta_bancaria = int(item["id_conta_bancaria"])
                saldo_banco = float(item.get("saldo_banco") or 0)
                saldo_sistema = float(item.get("saldo_sistema") or 0)
            except (KeyError, TypeError, ValueError):
                return jsonify({"ok": False, "erro": "Lançamento inválido: informe cod_filial, id_conta_bancaria, saldo_banco e saldo_sistema."}), 400

            if cod_filial not in filiais_ok:
                return jsonify({"ok": False, "erro": f"Filial {cod_filial} não permitida para este usuário."}), 403

            # nas contas marcadas o lado do sistema é espelho do banco;
            # forçado aqui para valer também em chamadas diretas à API
            if id_conta_bancaria in contas_espelhadas:
                saldo_sistema = saldo_banco

            registros.append((cod_empresa, cod_filial, data_lancamento, id_conta_bancaria, saldo_banco, saldo_sistema, id_usuario))

        execute_batch(cur, """
            INSERT INTO saldos_bancarios (
                cod_empresa, cod_filial, data, id_conta_bancaria, saldo_banco, saldo_sistema, usuario_lancamento
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cod_empresa, cod_filial, data, id_conta_bancaria)
            DO UPDATE SET
                saldo_banco = EXCLUDED.saldo_banco,
                saldo_sistema = EXCLUDED.saldo_sistema,
                usuario_lancamento = EXCLUDED.usuario_lancamento,
                atualizado_em = NOW()
        """, registros, page_size=100)
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "quantidade": len(registros)})


@financeiro_bp.route("/api/saldos/recebiveis", methods=["PUT"])
@permissao_obrigatoria("FINANCEIRO", "LANCAMENTO_SALDOS")
def api_lancar_saldos_recebiveis():
    cod_empresa = str(session["cod_empresa"]).strip()
    id_usuario = session["id_usuario"]
    dados = request.get_json(silent=True) or {}

    data_lancamento = validar_lancamento_data(dados)
    lancamentos = dados.get("lancamentos") or []

    if not data_lancamento:
        return jsonify({"ok": False, "erro": "Informe a data no formato YYYY-MM-DD."}), 400
    if not lancamentos:
        return jsonify({"ok": False, "erro": "Informe ao menos um lançamento."}), 400

    data_minima = data_minima_editavel(cod_empresa)
    if data_lancamento < data_minima:
        return jsonify({"ok": False, "erro": f"Data bloqueada para edição. Só é possível lançar a partir de {data_minima.strftime('%d/%m/%Y')}."}), 403

    conn = get_connection()
    cur = conn.cursor()
    try:
        filiais_ok = cod_filiais_permitidas_lancamento(cur, cod_empresa)

        registros = []
        for item in lancamentos:
            try:
                cod_filial = int(item["cod_filial"])
                id_indicador_recebivel = int(item["id_indicador_recebivel"])
                valor_banco = float(item.get("valor_banco") or 0)
                # Estoques e recebíveis são iguais nos dois lados: o valor do
                # sistema espelha o do banco. Forçado aqui para valer também
                # em chamadas diretas à API, não só na tela.
                valor_sistema = valor_banco
            except (KeyError, TypeError, ValueError):
                return jsonify({"ok": False, "erro": "Lançamento inválido: informe cod_filial, id_indicador_recebivel e valor_banco."}), 400

            if cod_filial not in filiais_ok:
                return jsonify({"ok": False, "erro": f"Filial {cod_filial} não permitida para este usuário."}), 403

            registros.append((cod_empresa, cod_filial, data_lancamento, id_indicador_recebivel, valor_banco, valor_sistema, id_usuario))

        # Linha importada de Operações não aceita digitação: só depois de
        # "Desfazer importação". A tela já deixa o campo readonly; aqui é a
        # trava de verdade, que vale também para chamada direta à API.
        cur.execute("""
            SELECT i.id_indicador_recebivel, af.cod_filial
              FROM saldos_importacoes_estoque i
              JOIN areas_filiais af
                ON af.cod_empresa = i.cod_empresa AND af.id_area = i.id_area
             WHERE i.cod_empresa = %s AND i.data = %s
        """, (cod_empresa, data_lancamento))
        travados = {(int(r[0]), int(r[1])) for r in cur.fetchall()}

        for registro in registros:
            if (registro[3], registro[1]) in travados:
                return jsonify({
                    "ok": False,
                    "erro": "Linha importada de Operações. Desfaça a importação antes de editar.",
                }), 403

        execute_batch(cur, """
            INSERT INTO saldos_recebiveis (
                cod_empresa, cod_filial, data, id_indicador_recebivel, valor_banco, valor_sistema, usuario_lancamento
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cod_empresa, cod_filial, data, id_indicador_recebivel)
            DO UPDATE SET
                valor_banco = EXCLUDED.valor_banco,
                valor_sistema = EXCLUDED.valor_sistema,
                usuario_lancamento = EXCLUDED.usuario_lancamento,
                atualizado_em = NOW()
        """, registros, page_size=100)
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "quantidade": len(registros)})


# =========================
# IMPORTAÇÃO DOS VALORES DE ESTOQUE (OPERAÇÕES → SALDOS)
# =========================
def _contexto_importacao_estoque(cur, cod_empresa, dados):
    """Valida o pedido de importar/desfazer e devolve o que os dois lados usam.

    Levanta ErroConsulta com a mensagem pronta quando algo não bate.
    """
    id_area = dados.get("id_area")
    origem = str(dados.get("origem") or "").strip().upper()

    try:
        id_area = int(id_area)
    except (TypeError, ValueError):
        raise ErroConsulta("Informe id_area.", 400)

    data_ref = validar_lancamento_data(dados)
    if not data_ref:
        raise ErroConsulta("Informe a data no formato YYYY-MM-DD.", 400)

    # Mesma trava de datas do lançamento manual (dois últimos dias úteis).
    data_minima = data_minima_editavel(cod_empresa)
    if data_ref < data_minima:
        raise ErroConsulta(
            f"Data bloqueada para edição. Só é possível lançar a partir de {data_minima.strftime('%d/%m/%Y')}.", 403)

    cur.execute("""
        SELECT id_indicador_recebivel, nome
          FROM indicadores_recebiveis
         WHERE cod_empresa = %s AND ativo = TRUE AND origem_estoque = %s
    """, (cod_empresa, origem))
    indicador = cur.fetchone()
    if not indicador:
        raise ErroConsulta("Nenhum indicador de recebível está ligado a esta origem de estoque.", 400)

    filiais = filiais_permitidas_usuario(cur, cod_empresa, id_area)
    if not filiais:
        raise ErroConsulta("Nenhuma filial disponível para esta área.", 403)

    codigos_filiais = [int(f["cod_filial"]) for f in filiais]

    # cod_filiais_permitidas_lancamento lê por posição (r[0]); precisa de
    # cursor comum, não do RealDictCursor usado aqui.
    cur_simples = cur.connection.cursor()
    try:
        filiais_ok = cod_filiais_permitidas_lancamento(cur_simples, cod_empresa)
    finally:
        cur_simples.close()
    if not set(codigos_filiais) <= filiais_ok:
        raise ErroConsulta("Área fora das suas permissões de lançamento.", 403)

    return {
        "id_area": id_area,
        "data": data_ref,
        "id_indicador": indicador["id_indicador_recebivel"],
        "codigos_filiais": codigos_filiais,
    }


def _gravar_valores_recebivel(cur, cod_empresa, data_ref, id_indicador, valores_por_filial, id_usuario):
    """Grava (banco = sistema, como no lançamento manual) o valor de cada filial."""
    execute_batch(cur, """
        INSERT INTO saldos_recebiveis (
            cod_empresa, cod_filial, data, id_indicador_recebivel, valor_banco, valor_sistema, usuario_lancamento
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (cod_empresa, cod_filial, data, id_indicador_recebivel)
        DO UPDATE SET
            valor_banco = EXCLUDED.valor_banco,
            valor_sistema = EXCLUDED.valor_sistema,
            usuario_lancamento = EXCLUDED.usuario_lancamento,
            atualizado_em = NOW()
    """, [
        (cod_empresa, cod_filial, data_ref, id_indicador, valor, valor, id_usuario)
        for cod_filial, valor in valores_por_filial.items()
    ], page_size=100)


@financeiro_bp.route("/api/saldos/importar-estoque", methods=["POST"])
@permissao_obrigatoria("FINANCEIRO", "LANCAMENTO_SALDOS")
def api_importar_estoque_saldos():
    """Puxa de Operações o valor de Compra de Combustível / Em Trânsito / Estoque.

    Só quando a data está BLOQUEADA em Operações (nada mais muda lá) e as
    células ainda estão zeradas — importação não sobrescreve digitação.
    """
    cod_empresa = str(session["cod_empresa"]).strip()
    id_usuario = session["id_usuario"]
    dados = request.get_json(silent=True) or {}

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        ctx = _contexto_importacao_estoque(cur, cod_empresa, dados)
        data_ref = ctx["data"]
        codigos_filiais = ctx["codigos_filiais"]

        pendentes = datas_bloqueio_pendentes(cur, cod_empresa, data_ref)
        if pendentes:
            faltam = ", ".join(d.strftime("%d/%m") for d in pendentes[:5])
            resto = f" (+{len(pendentes) - 5})" if len(pendentes) > 5 else ""
            raise ErroConsulta(
                "Para importar, tudo precisa estar bloqueado em Operações até "
                f"{data_ref.strftime('%d/%m/%Y')}. Ainda aberto: {faltam}{resto}.", 409)

        cur.execute("""
            SELECT COALESCE(SUM(ABS(valor_banco) + ABS(valor_sistema)), 0) AS soma
              FROM saldos_recebiveis
             WHERE cod_empresa = %s AND data = %s
               AND id_indicador_recebivel = %s AND cod_filial = ANY(%s)
        """, (cod_empresa, data_ref, ctx["id_indicador"], codigos_filiais))
        if float(cur.fetchone()["soma"] or 0) != 0:
            raise ErroConsulta("Já existem valores lançados nesta linha. Zere-os antes de importar.", 409)

        # A Consulta de Estoques é sempre um dia à frente do saldo: o saldo do
        # dia 10 fecha com o relatório de estoques do dia 11, que traz a
        # medição da manhã do 11 (o fechamento do 10) e as compras e o
        # trânsito do próprio dia 10.
        totais = totais_estoque_rs(cur, cod_empresa, data_ref + timedelta(days=1), codigos_filiais)
        origem = str(dados.get("origem") or "").strip().upper()
        valores = {f: round(totais[f][origem], 2) for f in codigos_filiais}

        _gravar_valores_recebivel(cur, cod_empresa, data_ref, ctx["id_indicador"], valores, id_usuario)

        cur.execute("""
            INSERT INTO saldos_importacoes_estoque
                (cod_empresa, data, id_area, id_indicador_recebivel, id_usuario)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (cod_empresa, data, id_area, id_indicador_recebivel)
            DO UPDATE SET id_usuario = EXCLUDED.id_usuario, criado_em = NOW()
        """, (cod_empresa, data_ref, ctx["id_area"], ctx["id_indicador"], id_usuario))

        conn.commit()
    except ErroConsulta as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), e.status
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "valores": valores})


@financeiro_bp.route("/api/saldos/importar-estoque", methods=["DELETE"])
@permissao_obrigatoria("FINANCEIRO", "LANCAMENTO_SALDOS")
def api_desfazer_importacao_estoque():
    """Zera a linha importada e libera o botão de importar de novo."""
    cod_empresa = str(session["cod_empresa"]).strip()
    id_usuario = session["id_usuario"]
    dados = request.get_json(silent=True) or {}

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        ctx = _contexto_importacao_estoque(cur, cod_empresa, dados)

        cur.execute("""
            DELETE FROM saldos_importacoes_estoque
             WHERE cod_empresa = %s AND data = %s
               AND id_area = %s AND id_indicador_recebivel = %s
        """, (cod_empresa, ctx["data"], ctx["id_area"], ctx["id_indicador"]))

        if cur.rowcount == 0:
            raise ErroConsulta("Esta linha não veio de importação.", 409)

        _gravar_valores_recebivel(
            cur, cod_empresa, ctx["data"], ctx["id_indicador"],
            {f: 0.0 for f in ctx["codigos_filiais"]}, id_usuario,
        )

        conn.commit()
    except ErroConsulta as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), e.status
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True})


@financeiro_bp.route("/api/saldos/valores-informados", methods=["PUT"])
@permissao_obrigatoria("FINANCEIRO", "LANCAMENTO_SALDOS")
def api_lancar_valores_informados():
    cod_empresa = str(session["cod_empresa"]).strip()
    id_usuario = session["id_usuario"]
    dados = request.get_json(silent=True) or {}

    data_lancamento = validar_lancamento_data(dados)
    lancamentos = dados.get("lancamentos") or []

    if not data_lancamento:
        return jsonify({"ok": False, "erro": "Informe a data no formato YYYY-MM-DD."}), 400
    if not lancamentos:
        return jsonify({"ok": False, "erro": "Informe ao menos um lançamento."}), 400

    data_minima = data_minima_editavel(cod_empresa)
    if data_lancamento < data_minima:
        return jsonify({"ok": False, "erro": f"Data bloqueada para edição. Só é possível lançar a partir de {data_minima.strftime('%d/%m/%Y')}."}), 403

    conn = get_connection()
    cur = conn.cursor()
    try:
        filiais_ok = cod_filiais_permitidas_lancamento(cur, cod_empresa)

        registros = []
        for item in lancamentos:
            try:
                cod_filial = int(item["cod_filial"])
                perdas_sobras = float(item.get("perdas_sobras") or 0)
                extras = float(item.get("extras") or 0)
                emprestimos_devolucoes = float(item.get("emprestimos_devolucoes") or 0)
                despesas = float(item.get("despesas") or 0)
                despesas_caixa = float(item.get("despesas_caixa") or 0)
            except (KeyError, TypeError, ValueError):
                return jsonify({"ok": False, "erro": "Lançamento inválido: informe cod_filial e os valores informados."}), 400

            if cod_filial not in filiais_ok:
                return jsonify({"ok": False, "erro": f"Filial {cod_filial} não permitida para este usuário."}), 403

            registros.append((cod_empresa, cod_filial, data_lancamento, perdas_sobras, extras, emprestimos_devolucoes, despesas, despesas_caixa, id_usuario))

        execute_batch(cur, """
            INSERT INTO valores_informados (
                cod_empresa, cod_filial, data, perdas_sobras, extras, emprestimos_devolucoes, despesas, despesas_caixa, usuario_lancamento
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cod_empresa, cod_filial, data)
            DO UPDATE SET
                perdas_sobras = EXCLUDED.perdas_sobras,
                extras = EXCLUDED.extras,
                emprestimos_devolucoes = EXCLUDED.emprestimos_devolucoes,
                despesas = EXCLUDED.despesas,
                despesas_caixa = EXCLUDED.despesas_caixa,
                usuario_lancamento = EXCLUDED.usuario_lancamento,
                atualizado_em = NOW()
        """, registros, page_size=100)
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
        conn.close()

    return jsonify({"ok": True, "quantidade": len(registros)})

# =========================
# CR — CONTAS A RECEBER
# =========================

@financeiro_bp.route("/cr/menu")
def menu_cr():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))
    id_usuario  = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()
    if tipo_global == "superusuario":
        pode_fiado   = True
        pode_cartoes = True
    else:
        pode_fiado   = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "CR_FIADO_MENU")
        pode_cartoes = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "CR_CARTOES_MENU")
    return render_template(
        "menu_cr.html",
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_empresa"),
        pode_fiado=pode_fiado,
        pode_cartoes=pode_cartoes,
    )


@financeiro_bp.route("/cr/menu-fiado")
def menu_cr_fiado():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))
    id_usuario  = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()
    if tipo_global == "superusuario":
        pode_importar = pode_consultar = pode_variacoes = pode_por_filial = pode_por_filial_cli = pode_por_cliente = True
    else:
        pode_importar      = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "FIADO_IMPORTAR")
        pode_consultar     = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "FIADO_CONSULTAR")
        pode_variacoes     = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "FIADO_VARIACOES")
        pode_por_filial    = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "FIADO_POR_FILIAL")
        pode_por_filial_cli= usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "FIADO_POR_FILIAL_CLIENTE")
        pode_por_cliente   = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "FIADO_POR_CLIENTE")
    return render_template(
        "menu_cr_fiado.html",
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_cr"),
        pode_importar=pode_importar,
        pode_consultar=pode_consultar,
        pode_variacoes=pode_variacoes,
        pode_por_filial=pode_por_filial,
        pode_por_filial_cli=pode_por_filial_cli,
        pode_por_cliente=pode_por_cliente,
    )


@financeiro_bp.route("/cr/menu-cartoes")
def menu_cr_cartoes():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))
    id_usuario  = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()
    if tipo_global == "superusuario":
        pode_importar = pode_consultar = pode_variacoes = True
    else:
        pode_importar  = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "CARTOES_IMPORTAR")
        pode_consultar = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "CARTOES_CONSULTAR")
        pode_variacoes = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "CARTOES_VARIACOES")
    return render_template(
        "menu_cr_cartoes.html",
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_cr"),
        pode_importar=pode_importar,
        pode_consultar=pode_consultar,
        pode_variacoes=pode_variacoes,
    )


@financeiro_bp.route("/cr/importar-fiado", methods=["GET", "POST"])
def cr_importar_fiado():
    return cr_fiado_impl(url_voltar=url_for("financeiro.menu_cr_fiado"))


# =========================
# CR - FIADO (impl)

def _parse_webportos_xlsx(fileobj, data_ref):
    """
    Lê arquivo xlsx do Webportos.
    Retorna (filiais, clientes):
      filiais  → {nome_filial_upper: saldo_total}
      clientes → [(nome_filial_upper, nome_cliente, saldo), ...]
    Regras:
      - 'Nota'          → Movimento <= data_ref - 1 dia
      - 'Nota Duplicata'→ Movimento <= data_ref
      - outros tipos    → ignorados
    """
    import zipfile
    import xml.etree.ElementTree as ET
    from datetime import timedelta

    limite_nota      = data_ref - timedelta(days=1)
    limite_duplicata = data_ref

    NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'

    def col_idx(ref):
        # "A1" → 0, "B1" → 1, "P1" → 15
        letters = ''.join(c for c in ref if c.isalpha())
        n = 0
        for c in letters.upper():
            n = n * 26 + (ord(c) - 64)
        return n - 1

    filial_atual  = None
    cliente_atual = None
    filiais   = defaultdict(float)
    clientes  = defaultdict(float)

    with zipfile.ZipFile(fileobj) as zf:
        # 1. Carrega tabela de strings compartilhadas
        shared = []
        if 'xl/sharedStrings.xml' in zf.namelist():
            with zf.open('xl/sharedStrings.xml') as f:
                for _, elem in ET.iterparse(f, events=('end',)):
                    if elem.tag == f'{{{NS}}}si':
                        text = ''.join(t.text or '' for t in elem.iter(f'{{{NS}}}t'))
                        shared.append(text)
                        elem.clear()

        # 2. Determina qual sheet é a ativa via workbook.xml
        ws_path = 'xl/worksheets/sheet1.xml'
        try:
            with zf.open('xl/workbook.xml') as f:
                tree = ET.parse(f)
                sheets = tree.findall(f'.//{{{NS}}}sheet')
                if sheets:
                    rid = sheets[0].get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id', 'rId1')
            with zf.open('xl/_rels/workbook.xml.rels') as f:
                tree = ET.parse(f)
                for rel in tree.getroot():
                    if rel.get('Id') == rid:
                        target = rel.get('Target', 'worksheets/sheet1.xml')
                        ws_path = f"xl/{target.lstrip('/')}"
                        break
        except Exception:
            pass

        # 3. Faz streaming da planilha linha a linha
        with zf.open(ws_path) as f:
            row_data = {}
            cur_col  = 0
            cur_type = ''
            cur_val  = None

            for event, elem in ET.iterparse(f, events=('start', 'end')):
                tag = elem.tag

                if event == 'start':
                    if tag == f'{{{NS}}}row':
                        row_data = {}
                    elif tag == f'{{{NS}}}c':
                        cur_col  = col_idx(elem.get('r', 'A1'))
                        cur_type = elem.get('t', '')
                        cur_val  = None

                elif event == 'end':
                    if tag == f'{{{NS}}}v':
                        raw = elem.text or ''
                        if cur_type == 's':
                            try:
                                cur_val = shared[int(raw)]
                            except (IndexError, ValueError):
                                cur_val = raw
                        elif cur_type == 'inlineStr':
                            pass  # handled by <t> below
                        else:
                            cur_val = raw
                        row_data[cur_col] = cur_val

                    elif tag == f'{{{NS}}}t' and cur_type == 'inlineStr':
                        cur_val = elem.text or ''
                        row_data[cur_col] = cur_val

                    elif tag == f'{{{NS}}}is':
                        # inline string: concatena todos os <t> dentro de <is>
                        cur_val = ''.join(t.text or '' for t in elem.iter(f'{{{NS}}}t'))
                        row_data[cur_col] = cur_val

                    elif tag == f'{{{NS}}}row':
                        def g(idx):
                            return str(row_data.get(idx) or '').strip()

                        c0 = g(0)

                        if c0 == 'Filial:':
                            # nome da filial pode estar em col B(1) ou C(2)
                            nova = (g(1) or g(2)).upper()
                            if nova != filial_atual:
                                cliente_atual = None
                            filial_atual = nova

                        elif c0 == 'Cliente:':
                            cliente_atual = g(2) or g(1)

                        elif filial_atual and cliente_atual:
                            tipo = g(2)
                            if tipo in ('Nota', 'Nota Duplicata'):
                                try:
                                    mov = datetime.strptime(g(3), '%d/%m/%Y').date()
                                except ValueError:
                                    mov = None
                                if mov:
                                    if tipo == 'Nota' and mov > limite_nota:
                                        pass
                                    elif tipo == 'Nota Duplicata' and mov > limite_duplicata:
                                        pass
                                    else:
                                        try:
                                            saldo = float(g(15) or 0)
                                        except ValueError:
                                            saldo = 0.0
                                        filiais[filial_atual] += saldo
                                        clientes[(filial_atual, cliente_atual)] += saldo

                        elem.clear()

    clientes_lista = [
        (fil, cli, sal)
        for (fil, cli), sal in clientes.items()
        if sal > 0
    ]
    return dict(filiais), clientes_lista


def _mapa_filiais_cadastro(cur, cod_empresa, nomes_arquivo):
    """
    Liga cada nome de filial vindo do arquivo ao nome gravado em
    filiais.nome_filial_importacao.

    O cadastro pode estar propositalmente truncado (para casar com outras
    rotinas de importação, cujo arquivo traz o nome cortado). Por isso
    aceitamos que o nome cadastrado seja PREFIXO do nome do arquivo.

    Vence sempre a correspondência mais longa. Isso é essencial: há
    cadastros em que um nome é prefixo de outro ("BONITO I" x "BONITO II"),
    e um prefixo ingênuo jogaria o saldo de BONITO II dentro de BONITO I.

    Devolve {nome_do_arquivo: nome_do_cadastro}. Nomes sem correspondência
    ficam iguais a si mesmos, e seguem aparecendo como filial sem área.
    """
    cur.execute("""
        SELECT DISTINCT UPPER(nome_filial_importacao) AS nome
        FROM filiais
        WHERE cod_empresa = %s
          AND COALESCE(nome_filial_importacao, '') <> ''
    """, (cod_empresa,))
    # do mais longo para o mais curto: o primeiro que casar é o melhor
    cadastros = sorted((r["nome"] for r in cur.fetchall()), key=len, reverse=True)

    mapa = {}
    for nome in nomes_arquivo:
        alvo = nome
        for cadastro in cadastros:
            if nome == cadastro or nome.startswith(cadastro):
                alvo = cadastro
                break
        mapa[nome] = alvo
    return mapa


@financeiro_bp.route("/cr-fiado", methods=["GET", "POST"])
def cr_fiado():
    return cr_fiado_impl(url_voltar=url_for("financeiro.menu_cr_fiado"))


def cr_fiado_impl(url_voltar):
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    erro = None
    sucesso = None

    if request.method == "POST":
        arquivo = request.files.get("arquivo")
        data_ref_str = request.form.get("data_referencia", "").strip()
        try:
            data_ref = datetime.strptime(data_ref_str, "%Y-%m-%d").date()
        except ValueError:
            erro = "Data inválida."
            arquivo = None

        if arquivo and not erro:
            try:
                import traceback
                conteudo = arquivo.read()
                filiais_saldo, clientes_lista = _parse_webportos_xlsx(io.BytesIO(conteudo), data_ref)
                del conteudo

                # Converte os nomes do arquivo para os nomes do cadastro antes
                # de gravar; assim todas as consultas seguintes casam por
                # igualdade, sem precisar repetir a regra de prefixo.
                mapa = _mapa_filiais_cadastro(cur, cod_empresa, filiais_saldo.keys())
                agrupado = defaultdict(float)
                for nome_fil, saldo in filiais_saldo.items():
                    agrupado[mapa.get(nome_fil, nome_fil)] += saldo
                filiais_saldo = dict(agrupado)
                clientes_lista = [
                    (mapa.get(nome_fil, nome_fil), cli, sal)
                    for nome_fil, cli, sal in clientes_lista
                ]

                total_geral = sum(filiais_saldo.values())

                # Upsert importação (substitui se mesma data)
                cur.execute("""
                    INSERT INTO fiado_importacoes (cod_empresa, data_referencia, nome_arquivo, total_geral)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (cod_empresa, data_referencia)
                    DO UPDATE SET nome_arquivo=EXCLUDED.nome_arquivo, total_geral=EXCLUDED.total_geral, criado_em=NOW()
                    RETURNING id
                """, (cod_empresa, data_ref, arquivo.filename, total_geral))
                id_imp = cur.fetchone()["id"]

                cur.execute("DELETE FROM fiado_filiais WHERE id_importacao=%s", (id_imp,))
                cur.executemany(
                    "INSERT INTO fiado_filiais (id_importacao, cod_empresa, nome_filial_import, saldo) VALUES (%s,%s,%s,%s)",
                    [(id_imp, cod_empresa, nome_fil, saldo) for nome_fil, saldo in filiais_saldo.items()]
                )

                cur.execute("DELETE FROM fiado_clientes WHERE id_importacao=%s", (id_imp,))
                cur.executemany(
                    "INSERT INTO fiado_clientes (id_importacao, cod_empresa, nome_filial_import, cliente, saldo) VALUES (%s,%s,%s,%s,%s)",
                    [(id_imp, cod_empresa, nome_fil, cli, sal) for nome_fil, cli, sal in clientes_lista]
                )

                conn.commit()
                sucesso = f"Importação de {data_ref.strftime('%d/%m/%Y')} salva — {len(filiais_saldo)} filiais, {len(clientes_lista)} clientes, total R$ {total_geral:,.2f}."
            except Exception as e:
                conn.rollback()
                erro = f"Erro ao processar arquivo: {e} || {traceback.format_exc()}"

    # Busca última importação
    cur.execute("""
        SELECT id, data_referencia, nome_arquivo, total_geral, criado_em
        FROM fiado_importacoes WHERE cod_empresa=%s
        ORDER BY data_referencia DESC LIMIT 1
    """, (cod_empresa,))
    ultima = cur.fetchone()

    resumo_areas = []
    if ultima:
        # Busca filiais da importação com mapeamento de área
        cur.execute("""
            SELECT ff.nome_filial_import, ff.saldo,
                   a.id_area, a.nome_area,
                   f.nome_filial
            FROM fiado_filiais ff
            LEFT JOIN filiais f
                ON UPPER(f.nome_filial_importacao) = ff.nome_filial_import
               AND f.cod_empresa = ff.cod_empresa
            LEFT JOIN areas_filiais af ON af.cod_filial = f.cod_filial AND af.cod_empresa = ff.cod_empresa
            LEFT JOIN areas a ON a.id_area = af.id_area AND a.cod_empresa = ff.cod_empresa
            WHERE ff.id_importacao = %s
            ORDER BY a.id_area NULLS LAST, ff.nome_filial_import
        """, (ultima["id"],))
        rows = cur.fetchall()

        # Agrupar por área
        por_area = defaultdict(list)
        sem_area = []
        for r in rows:
            if r["id_area"]:
                por_area[(r["id_area"], r["nome_area"])].append(r)
            else:
                sem_area.append(r)

        for (id_area, nome_area), filiais in sorted(por_area.items()):
            total_area = sum(f["saldo"] for f in filiais)
            resumo_areas.append({
                "nome_area": nome_area,
                "filiais": filiais,
                "total": total_area,
            })
        if sem_area:
            resumo_areas.append({
                "nome_area": "Sem Área",
                "filiais": sem_area,
                "total": sum(f["saldo"] for f in sem_area),
            })

    cur.close()
    conn.close()

    return render_template(
        "cr_fiado.html",
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_voltar,
        ultima=ultima,
        resumo_areas=resumo_areas,
        erro=erro,
        sucesso=sucesso,
        hoje=date.today().isoformat(),
    )

@financeiro_bp.route("/cr/consultas")
def cr_consultas():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    data_sel_str = request.args.get("data_referencia", "")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Todas as datas disponíveis
    cur.execute("""
        SELECT id, data_referencia, total_geral
        FROM fiado_importacoes
        WHERE cod_empresa = %s
        ORDER BY data_referencia
    """, (cod_empresa,))
    importacoes = cur.fetchall()

    datas = [r["data_referencia"] for r in importacoes]
    ids_por_data = {r["data_referencia"]: r["id"] for r in importacoes}

    # Data selecionada (default: mais recente)
    data_sel = None
    if data_sel_str:
        try:
            data_sel = datetime.strptime(data_sel_str, "%Y-%m-%d").date()
        except ValueError:
            pass
    if data_sel is None and datas:
        data_sel = datas[-1]

    # Busca áreas e filiais do sistema
    cur.execute("""
        SELECT a.id_area, a.nome_area,
               f.nome_filial_importacao AS nome_import,
               f.nome_filial,
               af.ordem
        FROM areas a
        JOIN areas_filiais af ON af.id_area = a.id_area AND af.cod_empresa = a.cod_empresa
        JOIN filiais f ON f.cod_filial = af.cod_filial AND f.cod_empresa = af.cod_empresa
        WHERE a.cod_empresa = %s AND a.ativo = TRUE
        ORDER BY a.id_area, af.ordem
    """, (cod_empresa,))
    mapa_filiais = cur.fetchall()

    # Para cada filial, busca saldo em todas as importações
    # Retorna {nome_import_upper: {data: saldo}}
    if importacoes:
        ids_todos = [r["id"] for r in importacoes]
        cur.execute("""
            SELECT ff.id_importacao, fi.data_referencia,
                   UPPER(ff.nome_filial_import) AS nome_import,
                   ff.saldo
            FROM fiado_filiais ff
            JOIN fiado_importacoes fi ON fi.id = ff.id_importacao
            WHERE ff.id_importacao = ANY(%s)
        """, (ids_todos,))
        rows_saldo = cur.fetchall()
    else:
        rows_saldo = []

    # Monta pivot: {nome_import_upper: {data: saldo}}
    pivot = defaultdict(lambda: defaultdict(float))
    for r in rows_saldo:
        pivot[r["nome_import"]][r["data_referencia"]] += float(r["saldo"])

    # Monta resumo por área com linha por filial e colunas = datas
    resumo_areas = []
    for id_area in sorted(set(f["id_area"] for f in mapa_filiais)):
        filiais_area = [f for f in mapa_filiais if f["id_area"] == id_area]
        nome_area = filiais_area[0]["nome_area"]
        linhas = []
        total_por_data = defaultdict(float)
        for f in filiais_area:
            nome_up = (f["nome_import"] or "").upper()
            saldos = {d: pivot[nome_up].get(d, None) for d in datas}
            for d, v in saldos.items():
                if v:
                    total_por_data[d] += v
            linhas.append({
                "nome": f["nome_filial"],
                "saldos": saldos,
                "saldo_sel": pivot[nome_up].get(data_sel) if data_sel else None,
            })
        resumo_areas.append({
            "nome_area": nome_area,
            "filiais": linhas,
            "total_por_data": dict(total_por_data),
            "total_sel": sum(total_por_data.get(data_sel, 0) for _ in [1]) if data_sel else 0,
        })
        # recalc total_sel corretamente
        resumo_areas[-1]["total_sel"] = total_por_data.get(data_sel, 0) if data_sel else 0

    cur.close()
    conn.close()

    return render_template(
        "cr_consultas.html",
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_cr_fiado"),
        datas=datas,
        data_sel=data_sel,
        resumo_areas=resumo_areas,
        hoje=date.today().isoformat(),
    )

@financeiro_bp.route("/cr/variacoes")
def cr_variacoes():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    hoje = date.today()

    data_ini_str = request.args.get("data_ini", (hoje - timedelta(days=30)).isoformat())
    data_fin_str = request.args.get("data_fin", hoje.isoformat())
    filtro_area  = request.args.get("area", "todas")

    try:
        data_ini = datetime.strptime(data_ini_str, "%Y-%m-%d").date()
        data_fin = datetime.strptime(data_fin_str, "%Y-%m-%d").date()
    except ValueError:
        data_ini = hoje - timedelta(days=30)
        data_fin = hoje

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    # Áreas e filiais do sistema, na ordem definida
    cur.execute("""
        SELECT a.id_area, a.nome_area,
               f.nome_filial_importacao AS nome_import,
               f.nome_filial,
               af.ordem
        FROM areas a
        JOIN areas_filiais af ON af.id_area = a.id_area AND af.cod_empresa = a.cod_empresa
        JOIN filiais f ON f.cod_filial = af.cod_filial AND f.cod_empresa = af.cod_empresa
        WHERE a.cod_empresa = %s AND a.ativo = TRUE
        ORDER BY a.id_area, af.ordem
    """, (cod_empresa,))
    todas_filiais = cur.fetchall()

    # IDs de área disponíveis
    ids_area = sorted(set(f["id_area"] for f in todas_filiais))

    # Aplica filtro de área
    if filtro_area != "todas":
        try:
            id_area_sel = int(filtro_area)
            todas_filiais = [f for f in todas_filiais if f["id_area"] == id_area_sel]
        except ValueError:
            pass

    # Importações no período
    cur.execute("""
        SELECT id, data_referencia
        FROM fiado_importacoes
        WHERE cod_empresa = %s AND data_referencia BETWEEN %s AND %s
        ORDER BY data_referencia
    """, (cod_empresa, data_ini, data_fin))
    importacoes = cur.fetchall()
    datas = [r["data_referencia"] for r in importacoes]
    ids_imp = [r["id"] for r in importacoes]

    # Saldos por importação
    pivot = defaultdict(lambda: defaultdict(float))  # {data: {nome_import_upper: saldo}}
    if ids_imp:
        cur.execute("""
            SELECT fi.data_referencia,
                   UPPER(ff.nome_filial_import) AS nome_import,
                   ff.saldo
            FROM fiado_filiais ff
            JOIN fiado_importacoes fi ON fi.id = ff.id_importacao
            WHERE ff.id_importacao = ANY(%s)
        """, (ids_imp,))
        for r in cur.fetchall():
            pivot[r["data_referencia"]][r["nome_import"]] += float(r["saldo"])

    cur.close()
    conn.close()

    # Monta estrutura de áreas com filiais
    areas = []
    por_id = defaultdict(list)
    for f in todas_filiais:
        por_id[f["id_area"]].append(f)

    for id_area in sorted(por_id):
        filiais_area = por_id[id_area]
        nome_area    = filiais_area[0]["nome_area"]
        nomes_import = [(f["nome_filial"], (f["nome_import"] or "").upper()) for f in filiais_area]
        areas.append({
            "id_area":    id_area,
            "nome_area":  nome_area,
            "filiais":    nomes_import,   # [(nome_exib, nome_import_upper), ...]
        })

    return render_template(
        "cr_variacoes.html",
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_cr_fiado"),
        datas=datas,
        areas=areas,
        ids_area=ids_area,
        pivot=pivot,          # {data: {nome_import_upper: saldo}}
        filtro_area=filtro_area,
        data_ini=data_ini,
        data_fin=data_fin,
        hoje=hoje.isoformat(),
    )

@financeiro_bp.route("/cr/por-filial-cliente")
def cr_por_filial_cliente():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    # Última importação
    cur.execute("""
        SELECT id, data_referencia, total_geral
        FROM fiado_importacoes
        WHERE cod_empresa = %s
        ORDER BY data_referencia DESC LIMIT 1
    """, (cod_empresa,))
    ultima = cur.fetchone()

    areas = []
    if ultima:
        # Áreas e filiais na ordem do sistema
        cur.execute("""
            SELECT a.id_area, a.nome_area,
                   f.nome_filial_importacao AS nome_import,
                   f.nome_filial,
                   af.ordem
            FROM areas a
            JOIN areas_filiais af ON af.id_area = a.id_area AND af.cod_empresa = a.cod_empresa
            JOIN filiais f ON f.cod_filial = af.cod_filial AND f.cod_empresa = af.cod_empresa
            WHERE a.cod_empresa = %s AND a.ativo = TRUE
            ORDER BY a.id_area, af.ordem
        """, (cod_empresa,))
        mapa = cur.fetchall()

        # Clientes da última importação
        cur.execute("""
            SELECT UPPER(nome_filial_import) AS nome_import, cliente, saldo
            FROM fiado_clientes
            WHERE id_importacao = %s AND saldo > 0
            ORDER BY nome_filial_import, saldo DESC
        """, (ultima["id"],))
        rows_cli = cur.fetchall()

        # Agrupa clientes por filial
        cli_por_filial = defaultdict(list)
        for r in rows_cli:
            cli_por_filial[r["nome_import"]].append({
                "cliente": r["cliente"],
                "saldo":   float(r["saldo"]),
            })

        # Monta estrutura por área
        por_area = defaultdict(list)
        for f in mapa:
            por_area[f["id_area"]].append(f)

        for id_area in sorted(por_area):
            filiais_area = por_area[id_area]
            filiais_out  = []
            for f in filiais_area:
                nome_up  = (f["nome_import"] or "").upper()
                clientes = cli_por_filial.get(nome_up, [])
                if not clientes:
                    continue
                total_fil = sum(c["saldo"] for c in clientes)
                filiais_out.append({
                    "nome":     f["nome_filial"],
                    "clientes": clientes,
                    "total":    total_fil,
                })
            if filiais_out:
                areas.append({
                    "nome_area": filiais_area[0]["nome_area"],
                    "filiais":   filiais_out,
                })

    cur.close()
    conn.close()

    return render_template(
        "cr_por_filial_cliente.html",
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_cr_fiado"),
        ultima=ultima,
        areas=areas,
    )


# =========================
# CR — POR FILIAL (ranking)
# =========================

@financeiro_bp.route("/cr/por-filial")
def cr_por_filial():
    if "id_usuario" not in session or "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    filtro_area = request.args.get("area", "")  # "" = todas

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    # última importação
    cur.execute("""
        SELECT id, data_referencia, total_geral
        FROM fiado_importacoes WHERE cod_empresa=%s
        ORDER BY data_referencia DESC LIMIT 1
    """, (cod_empresa,))
    ultima = cur.fetchone()

    filiais = []
    areas   = []

    if ultima:
        # todas as áreas para o filtro
        cur.execute("""
            SELECT DISTINCT a.id_area, a.nome_area
            FROM fiado_filiais ff
            JOIN filiais f ON UPPER(f.nome_filial_importacao) = ff.nome_filial_import
                          AND f.cod_empresa = ff.cod_empresa
            JOIN areas_filiais af ON af.cod_filial = f.cod_filial AND af.cod_empresa = f.cod_empresa
            JOIN areas a ON a.id_area = af.id_area AND a.cod_empresa = f.cod_empresa
            WHERE ff.id_importacao = %s
            ORDER BY a.nome_area
        """, (ultima["id"],))
        areas = cur.fetchall()

        area_filter = ""
        if filtro_area:
            area_filter = "AND a.id_area = %(area)s"

        cur.execute(f"""
            SELECT
                f.cod_filial,
                COALESCE(f.nome_filial, ff.nome_filial_import) AS nome_filial,
                ff.saldo,
                a.id_area,
                a.nome_area
            FROM fiado_filiais ff
            LEFT JOIN filiais f ON UPPER(f.nome_filial_importacao) = ff.nome_filial_import
                               AND f.cod_empresa = ff.cod_empresa
            LEFT JOIN areas_filiais af ON af.cod_filial = f.cod_filial AND af.cod_empresa = f.cod_empresa
            LEFT JOIN areas a ON a.id_area = af.id_area AND a.cod_empresa = f.cod_empresa
            WHERE ff.id_importacao = %(id_imp)s
            {area_filter}
            ORDER BY ff.saldo DESC
        """, {"id_imp": ultima["id"], "area": filtro_area or None})
        filiais = cur.fetchall()

    cur.close()
    conn.close()

    return render_template(
        "cr_por_filial.html",
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_cr_fiado"),
        ultima=ultima,
        filiais=filiais,
        areas=areas,
        filtro_area=filtro_area,
    )


# =========================
# CR — POR CLIENTE (ranking geral)
# =========================

@financeiro_bp.route("/cr/por-cliente")
def cr_por_cliente():
    if "id_usuario" not in session or "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT id, data_referencia, total_geral
        FROM fiado_importacoes
        WHERE cod_empresa = %s
        ORDER BY data_referencia DESC LIMIT 1
    """, (cod_empresa,))
    ultima = cur.fetchone()

    clientes = []
    if ultima:
        # Mapa nome_filial_import → nome_filial amigável
        cur.execute("""
            SELECT UPPER(f.nome_filial_importacao) AS nome_import, f.nome_filial
            FROM filiais f
            WHERE f.cod_empresa = %s
        """, (cod_empresa,))
        mapa_filial = {r["nome_import"]: r["nome_filial"] for r in cur.fetchall()}

        cur.execute("""
            SELECT UPPER(nome_filial_import) AS nome_import, cliente, saldo
            FROM fiado_clientes
            WHERE id_importacao = %s AND saldo > 0
            ORDER BY saldo DESC
        """, (ultima["id"],))
        for r in cur.fetchall():
            clientes.append({
                "cliente":      r["cliente"],
                "nome_filial":  mapa_filial.get(r["nome_import"], r["nome_import"].title()),
                "saldo":        float(r["saldo"]),
            })

    cur.close()
    conn.close()

    return render_template(
        "cr_por_cliente.html",
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_cr_fiado"),
        ultima=ultima,
        clientes=clientes,
    )


# =========================
# CR — CARTÕES
# =========================

def _parse_cartoes_xlsx(fileobj, data_ref):
    """
    Lê xlsx de Movimentação de Cartões.
    - Filial: col[0]="Filial:", nome em col[2]
    - Linhas de detalhe: col[13] numérico = linha válida, col[12]=Total Líquido
    - Sem filtro de data (igual ao VBA ResumirCartoes)
    Retorna {nome_filial_upper: total_liquido}
    """
    import openpyxl

    wb = openpyxl.load_workbook(fileobj, data_only=True)
    ws = wb.active
    filial_atual = None
    filiais = defaultdict(float)

    for row in ws.iter_rows(values_only=True):
        c0 = str(row[0] or "").strip()
        if c0 == "Filial:":
            filial_atual = str(row[2] or "").strip().upper()
            continue
        if not filial_atual:
            continue
        # col[1] = administradora não vazia (VBA: admin <> "")
        admin = str(row[1] or "").strip()
        if not admin:
            continue
        # col[12] = Total Líquido
        liquido = row[12] if len(row) > 12 else None
        if not isinstance(liquido, (int, float)) or liquido == 0:
            continue
        filiais[filial_atual] += float(liquido)

    return dict(filiais)


@financeiro_bp.route("/cr/cartoes/importar", methods=["GET", "POST"])
def cartoes_importar():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    erro = sucesso = None

    if request.method == "POST":
        arquivo = request.files.get("arquivo")
        data_ref_str = request.form.get("data_referencia", "").strip()
        try:
            data_ref = datetime.strptime(data_ref_str, "%Y-%m-%d").date()
        except ValueError:
            erro = "Data inválida."
            arquivo = None

        if arquivo and not erro:
            try:
                conteudo = arquivo.read()
                filiais_saldo = _parse_cartoes_xlsx(io.BytesIO(conteudo), data_ref)
                total_geral = sum(filiais_saldo.values())

                cur.execute("""
                    INSERT INTO cartoes_importacoes (cod_empresa, data_referencia, nome_arquivo, total_geral)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (cod_empresa, data_referencia)
                    DO UPDATE SET nome_arquivo=EXCLUDED.nome_arquivo, total_geral=EXCLUDED.total_geral, criado_em=NOW()
                    RETURNING id
                """, (cod_empresa, data_ref, arquivo.filename, total_geral))
                id_imp = cur.fetchone()["id"]

                cur.execute("DELETE FROM cartoes_filiais WHERE id_importacao=%s", (id_imp,))
                for nome, saldo in filiais_saldo.items():
                    cur.execute("""
                        INSERT INTO cartoes_filiais (id_importacao, cod_empresa, nome_filial_import, saldo)
                        VALUES (%s,%s,%s,%s)
                    """, (id_imp, cod_empresa, nome, saldo))
                conn.commit()
                sucesso = f"Importação de {data_ref.strftime('%d/%m/%Y')} salva — {len(filiais_saldo)} filiais, total R$ {total_geral:,.2f}."
            except Exception as e:
                conn.rollback()
                erro = f"Erro ao processar arquivo: {e}"

    cur.execute("""
        SELECT id, data_referencia, nome_arquivo, total_geral
        FROM cartoes_importacoes WHERE cod_empresa=%s
        ORDER BY data_referencia DESC LIMIT 1
    """, (cod_empresa,))
    ultima = cur.fetchone()

    resumo_areas = []
    if ultima:
        cur.execute("""
            SELECT cf.nome_filial_import, cf.saldo,
                   a.id_area, a.nome_area, f.nome_filial
            FROM cartoes_filiais cf
            LEFT JOIN filiais f ON UPPER(f.nome_filial_importacao)=cf.nome_filial_import AND f.cod_empresa=cf.cod_empresa
            LEFT JOIN areas_filiais af ON af.cod_filial=f.cod_filial AND af.cod_empresa=cf.cod_empresa
            LEFT JOIN areas a ON a.id_area=af.id_area AND a.cod_empresa=cf.cod_empresa
            WHERE cf.id_importacao=%s
            ORDER BY a.id_area NULLS LAST, cf.nome_filial_import
        """, (ultima["id"],))
        rows = cur.fetchall()
        por_area = defaultdict(list)
        sem_area = []
        for r in rows:
            if r["id_area"]:
                por_area[(r["id_area"], r["nome_area"])].append(r)
            else:
                sem_area.append(r)
        for (id_area, nome_area), filiais in sorted(por_area.items()):
            total_area = sum(float(f["saldo"]) for f in filiais)
            resumo_areas.append({"nome_area": nome_area, "filiais": filiais, "total": total_area})
        if sem_area:
            resumo_areas.append({"nome_area": "Sem Área", "filiais": sem_area, "total": sum(float(f["saldo"]) for f in sem_area)})

    cur.close(); conn.close()
    return render_template("cartoes_importar.html",
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_cr_cartoes"),
        ultima=ultima, resumo_areas=resumo_areas,
        erro=erro, sucesso=sucesso, hoje=date.today().isoformat())


@financeiro_bp.route("/cr/cartoes/consultar")
def cartoes_consultar():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    data_sel_str = request.args.get("data_referencia", "")
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT id, data_referencia, total_geral FROM cartoes_importacoes WHERE cod_empresa=%s ORDER BY data_referencia", (cod_empresa,))
    importacoes = cur.fetchall()
    datas = [r["data_referencia"] for r in importacoes]
    ids_todos = [r["id"] for r in importacoes]

    data_sel = None
    if data_sel_str:
        try: data_sel = datetime.strptime(data_sel_str, "%Y-%m-%d").date()
        except ValueError: pass
    if data_sel is None and datas:
        data_sel = datas[-1]

    cur.execute("""
        SELECT a.id_area, a.nome_area,
               f.nome_filial_importacao AS nome_import, f.nome_filial, af.ordem
        FROM areas a
        JOIN areas_filiais af ON af.id_area=a.id_area AND af.cod_empresa=a.cod_empresa
        JOIN filiais f ON f.cod_filial=af.cod_filial AND f.cod_empresa=af.cod_empresa
        WHERE a.cod_empresa=%s AND a.ativo=TRUE ORDER BY a.id_area, af.ordem
    """, (cod_empresa,))
    mapa = cur.fetchall()

    pivot = defaultdict(lambda: defaultdict(float))
    if ids_todos:
        cur.execute("""
            SELECT fi.data_referencia, UPPER(cf.nome_filial_import) AS nome_import, cf.saldo
            FROM cartoes_filiais cf JOIN cartoes_importacoes fi ON fi.id=cf.id_importacao
            WHERE cf.id_importacao=ANY(%s)
        """, (ids_todos,))
        for r in cur.fetchall():
            pivot[r["data_referencia"]][r["nome_import"]] += float(r["saldo"])

    resumo_areas = []
    for id_area in sorted(set(f["id_area"] for f in mapa)):
        filiais_area = [f for f in mapa if f["id_area"] == id_area]
        linhas = []
        total_por_data = defaultdict(float)
        for f in filiais_area:
            nome_up = (f["nome_import"] or "").upper()
            saldos = {d: pivot[d].get(nome_up) for d in datas}
            for d, v in saldos.items():
                if v: total_por_data[d] += v
            linhas.append({"nome": f["nome_filial"], "saldos": saldos, "saldo_sel": pivot[data_sel].get(nome_up) if data_sel else None})
        resumo_areas.append({"nome_area": filiais_area[0]["nome_area"], "filiais": linhas,
            "total_por_data": dict(total_por_data), "total_sel": total_por_data.get(data_sel, 0) if data_sel else 0})

    cur.close(); conn.close()
    return render_template("cartoes_consultar.html",
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_cr_cartoes"),
        datas=datas, data_sel=data_sel, resumo_areas=resumo_areas,
        hoje=date.today().isoformat())


@financeiro_bp.route("/cr/cartoes/variacoes")
def cartoes_variacoes():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    hoje = date.today()
    data_ini_str = request.args.get("data_ini", (hoje - timedelta(days=30)).isoformat())
    data_fin_str = request.args.get("data_fin", hoje.isoformat())
    filtro_area  = request.args.get("area", "todas")
    try:
        data_ini = datetime.strptime(data_ini_str, "%Y-%m-%d").date()
        data_fin = datetime.strptime(data_fin_str, "%Y-%m-%d").date()
    except ValueError:
        data_ini = hoje - timedelta(days=30); data_fin = hoje

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT a.id_area, a.nome_area, f.nome_filial_importacao AS nome_import, f.nome_filial, af.ordem
        FROM areas a
        JOIN areas_filiais af ON af.id_area=a.id_area AND af.cod_empresa=a.cod_empresa
        JOIN filiais f ON f.cod_filial=af.cod_filial AND f.cod_empresa=af.cod_empresa
        WHERE a.cod_empresa=%s AND a.ativo=TRUE ORDER BY a.id_area, af.ordem
    """, (cod_empresa,))
    todas_filiais = cur.fetchall()
    ids_area = sorted(set(f["id_area"] for f in todas_filiais))
    if filtro_area != "todas":
        try:
            id_a = int(filtro_area)
            todas_filiais = [f for f in todas_filiais if f["id_area"] == id_a]
        except ValueError: pass

    cur.execute("""
        SELECT id, data_referencia FROM cartoes_importacoes
        WHERE cod_empresa=%s AND data_referencia BETWEEN %s AND %s ORDER BY data_referencia
    """, (cod_empresa, data_ini, data_fin))
    importacoes = cur.fetchall()
    datas = [r["data_referencia"] for r in importacoes]
    ids_imp = [r["id"] for r in importacoes]

    pivot = defaultdict(lambda: defaultdict(float))
    if ids_imp:
        cur.execute("""
            SELECT fi.data_referencia, UPPER(cf.nome_filial_import) AS nome_import, cf.saldo
            FROM cartoes_filiais cf JOIN cartoes_importacoes fi ON fi.id=cf.id_importacao
            WHERE cf.id_importacao=ANY(%s)
        """, (ids_imp,))
        for r in cur.fetchall():
            pivot[r["data_referencia"]][r["nome_import"]] += float(r["saldo"])

    areas = []
    por_id = defaultdict(list)
    for f in todas_filiais: por_id[f["id_area"]].append(f)
    for id_area in sorted(por_id):
        fl = por_id[id_area]
        areas.append({"id_area": id_area, "nome_area": fl[0]["nome_area"],
                      "filiais": [(f["nome_filial"], (f["nome_import"] or "").upper()) for f in fl]})

    cur.close(); conn.close()
    return render_template("cartoes_variacoes.html",
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_cr_cartoes"),
        datas=datas, areas=areas, ids_area=ids_area,
        pivot=pivot, filtro_area=filtro_area,
        data_ini=data_ini, data_fin=data_fin, hoje=hoje.isoformat())


# =========================================================
# CRÉDITO — CADASTRO E ANÁLISE DE CRÉDITO DE NOVO CLIENTE
# =========================================================
#
# Workflow em duas mãos: o analista cadastra a solicitação e dá o parecer
# (status PENDENTE); quem tem alçada aprova ou reprova com observação. As duas
# telas leem a mesma tabela `credito_analises` — não existe cópia do cadastro
# em lugar nenhum.

# Listas de opções do formulário. Ficam aqui porque tela e gravação precisam da
# mesma lista — no banco são `character varying` livres, seguindo a convenção do
# projeto (nada de enum do Postgres).
CREDITO_OPCOES = {
    "tipo_cliente": [("PF", "Pessoa Física"), ("PJ", "Pessoa Jurídica")],
    "finalidade": [
        ("COMBUSTIVEL", "Combustível"),
        ("LUBRIFICANTES", "Lubrificantes"),
        ("OUTROS", "Outros"),
    ],
    "documentacao_financeira": [
        ("SIM", "Sim"),
        ("NAO", "Não"),
        ("DISPENSADA", "Dispensada conforme alçada"),
    ],
    "biro_situacao": [
        ("SEM_RESTRICAO", "Sem restrição"),
        ("COM_RESTRICAO", "Com restrição"),
    ],
    "protestos": [("NAO", "Não"), ("SIM", "Sim")],
    "situacao_documento": [("REGULAR", "Regular"), ("IRREGULAR", "Irregular")],
    "referencia_comercial": [
        ("FAVORAVEL", "Favorável"),
        ("ATENCAO", "Atenção"),
        ("NAO_NECESSARIA", "Não necessária"),
    ],
    "classificacao_risco": [
        ("BAIXO", "Baixo risco"),
        ("MEDIO", "Médio risco"),
        ("ALTO", "Alto risco"),
    ],
    "parecer_credito": [
        ("APROVAR", "Aprovar"),
        ("APROVAR_LIMITE_MENOR", "Aprovar com limite menor"),
        ("APROVAR_CONDICAO", "Aprovar com condição"),
        ("REPROVAR", "Reprovar"),
    ],
    "decisao": [
        ("APROVADO", "Aprovado"),
        ("APROVADO_COM_CONDICAO", "Aprovado com condição"),
        ("REPROVADO", "Reprovado"),
    ],
}

CREDITO_STATUS_ROTULO = {
    "PENDENTE": "Pendente",
    "APROVADO": "Aprovado",
    "APROVADO_COM_CONDICAO": "Aprovado com condição",
    "REPROVADO": "Reprovado",
}

# Campos que a tela de cadastro grava, na ordem do formulário em papel.
CREDITO_CAMPOS_TEXTO = [
    "tipo_cliente", "nome_razao_social", "cpf_cnpj", "telefone", "cidade_uf",
    "profissao_atividade", "tempo_atividade", "responsavel_nome",
    "responsavel_cpf", "finalidade", "finalidade_outros", "forma_cobranca",
    "documentacao_financeira", "biro_situacao", "protestos",
    "situacao_documento", "referencia_comercial", "observacao_relevante",
    "classificacao_risco", "parecer_credito", "justificativa",
]

CREDITO_CAMPOS_NUMERICOS = [
    "consumo_mensal_estimado", "limite_solicitado", "renda_faturamento",
    "endividamento", "protestos_valor", "limite_recomendado",
]

CREDITO_CAMPOS_INTEIROS = [
    "prazo_solicitado_dias", "biro_score", "prazo_recomendado_dias",
]


def _credito_numero(valor):
    """Aceita o que o usuário digitar: '1.500,00', '1500.00', vazio."""
    texto = str(valor or "").strip()
    if not texto:
        return None
    texto = texto.replace("R$", "").replace(" ", "")
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return float(texto)
    except ValueError:
        return None


def _credito_inteiro(valor):
    numero = _credito_numero(valor)
    return int(numero) if numero is not None else None


def _credito_dados_do_form(form):
    dados = {c: (form.get(c) or "").strip() or None for c in CREDITO_CAMPOS_TEXTO}
    dados.update({c: _credito_numero(form.get(c)) for c in CREDITO_CAMPOS_NUMERICOS})
    dados.update({c: _credito_inteiro(form.get(c)) for c in CREDITO_CAMPOS_INTEIROS})
    return dados


def _credito_pode(acao):
    if str(session.get("tipo_global") or "").strip().lower() == "superusuario":
        return True
    return usuario_tem_permissao(
        session.get("id_usuario"),
        str(session.get("cod_empresa") or "").strip(),
        "FINANCEIRO",
        acao,
    )


# ---------------------------------------
# MENU CRÉDITO
# ---------------------------------------
@financeiro_bp.route("/credito/menu")
def menu_credito():
    if "id_usuario" not in session or "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    pode_cadastrar = _credito_pode("CREDITO_CADASTRAR")
    pode_aprovar = _credito_pode("CREDITO_APROVAR")

    # O menu abre com qualquer uma das duas.
    if not (pode_cadastrar or pode_aprovar):
        flash("Você não tem acesso ao módulo de Crédito.", "error")
        return redirect(url_for("financeiro.menu_empresa"))

    return render_template(
        "menu_credito.html",
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_empresa"),
        texto_voltar="← Voltar",
        pode_cadastrar=pode_cadastrar,
        pode_aprovar=pode_aprovar,
    )


# ---------------------------------------
# CADASTRAR ANÁLISE DE CRÉDITO — LISTA
# ---------------------------------------
@financeiro_bp.route("/credito/analises")
@permissao_obrigatoria(
    "FINANCEIRO", "CREDITO_CADASTRAR", redirecionar_para="financeiro.menu_credito"
)
def credito_analises():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()
    status_sel = (request.args.get("status") or "").strip()
    busca = (request.args.get("busca") or "").strip()

    filtros = ["cod_empresa = %s"]
    params = [cod_empresa]

    if status_sel:
        filtros.append("status = %s")
        params.append(status_sel)

    if busca:
        filtros.append("(nome_razao_social ILIKE %s OR cpf_cnpj ILIKE %s)")
        params += [f"%{busca}%", f"%{busca}%"]

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute(f"""
            SELECT *
            FROM credito_analises
            WHERE {' AND '.join(filtros)}
            ORDER BY id_analise DESC
        """, params)
        analises = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    return render_template(
        "credito_analises.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_credito"),
        texto_voltar="← Voltar",
        analises=analises,
        status_sel=status_sel,
        busca=busca,
        status_rotulo=CREDITO_STATUS_ROTULO,
        formatar_numero_br=formatar_numero_br,
    )


# ---------------------------------------
# CADASTRAR ANÁLISE DE CRÉDITO — FORMULÁRIO
# ---------------------------------------
@financeiro_bp.route("/credito/analises/nova", methods=["GET", "POST"])
@financeiro_bp.route("/credito/analises/<int:id_analise>", methods=["GET", "POST"])
@permissao_obrigatoria(
    "FINANCEIRO", "CREDITO_CADASTRAR", redirecionar_para="financeiro.menu_credito"
)
def credito_analise_form(id_analise=None):
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        analise = None

        if id_analise:
            cur.execute("""
                SELECT * FROM credito_analises
                WHERE id_analise = %s AND cod_empresa = %s
            """, (id_analise, cod_empresa))
            analise = cur.fetchone()

            if not analise:
                flash("Análise não encontrada.", "error")
                return redirect(url_for("financeiro.credito_analises"))

        if request.method == "POST":
            # Depois de decidida, a análise não volta a ser editada — o que
            # valeu para a decisão fica registrado como estava.
            if analise and analise["status"] != "PENDENTE":
                flash("Análise já decidida não pode ser alterada.", "error")
                return redirect(url_for(
                    "financeiro.credito_analise_form", id_analise=id_analise
                ))

            dados = _credito_dados_do_form(request.form)

            if not dados["nome_razao_social"]:
                flash("Informe o nome / razão social.", "error")
            else:
                colunas = list(dados.keys())
                valores = [dados[c] for c in colunas]

                if analise:
                    sets = ", ".join(f"{c} = %s" for c in colunas)
                    cur.execute(f"""
                        UPDATE credito_analises
                        SET {sets}, atualizado_em = NOW()
                        WHERE id_analise = %s AND cod_empresa = %s
                    """, valores + [id_analise, cod_empresa])
                    conn.commit()
                    flash("Análise atualizada com sucesso.", "success")
                else:
                    nomes = ", ".join(colunas)
                    marcadores = ", ".join(["%s"] * len(colunas))
                    cur.execute(f"""
                        INSERT INTO credito_analises (
                            cod_empresa, {nomes},
                            id_usuario_analista, nome_analista, data_analise,
                            status, criado_em, atualizado_em
                        )
                        VALUES (%s, {marcadores}, %s, %s, %s, 'PENDENTE', NOW(), NOW())
                        RETURNING id_analise
                    """, [cod_empresa] + valores + [
                        session.get("id_usuario"),
                        session.get("nome_usuario") or session.get("usuario"),
                        date.today(),
                    ])
                    id_analise = cur.fetchone()["id_analise"]
                    conn.commit()
                    flash("Análise cadastrada e enviada para aprovação.", "success")

                return redirect(url_for(
                    "financeiro.credito_analise_form", id_analise=id_analise
                ))

            cur.execute("""
                SELECT * FROM credito_analises
                WHERE id_analise = %s AND cod_empresa = %s
            """, (id_analise or 0, cod_empresa))
            analise = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    return render_template(
        "credito_analise_form.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.credito_analises"),
        texto_voltar="← Voltar",
        analise=analise,
        opcoes=CREDITO_OPCOES,
        status_rotulo=CREDITO_STATUS_ROTULO,
    )


# ---------------------------------------
# APROVAR ANÁLISE DE CRÉDITO — LISTA
# ---------------------------------------
@financeiro_bp.route("/credito/aprovacoes")
@permissao_obrigatoria(
    "FINANCEIRO", "CREDITO_APROVAR", redirecionar_para="financeiro.menu_credito"
)
def credito_aprovacoes():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    # A fila abre nas pendentes; as decididas ficam a um clique de distância.
    status_sel = (request.args.get("status") or "PENDENTE").strip()

    filtros = ["cod_empresa = %s"]
    params = [cod_empresa]

    if status_sel != "TODAS":
        filtros.append("status = %s")
        params.append(status_sel)

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute(f"""
            SELECT *
            FROM credito_analises
            WHERE {' AND '.join(filtros)}
            ORDER BY id_analise DESC
        """, params)
        analises = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    return render_template(
        "credito_aprovacoes.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.menu_credito"),
        texto_voltar="← Voltar",
        analises=analises,
        status_sel=status_sel,
        status_rotulo=CREDITO_STATUS_ROTULO,
        formatar_numero_br=formatar_numero_br,
    )


# ---------------------------------------
# APROVAR ANÁLISE DE CRÉDITO — DECISÃO
# ---------------------------------------
@financeiro_bp.route("/credito/aprovacoes/<int:id_analise>", methods=["GET", "POST"])
@permissao_obrigatoria(
    "FINANCEIRO", "CREDITO_APROVAR", redirecionar_para="financeiro.menu_credito"
)
def credito_decisao(id_analise):
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT * FROM credito_analises
            WHERE id_analise = %s AND cod_empresa = %s
        """, (id_analise, cod_empresa))
        analise = cur.fetchone()

        if not analise:
            flash("Análise não encontrada.", "error")
            return redirect(url_for("financeiro.credito_aprovacoes"))

        if request.method == "POST":
            decisao = (request.form.get("decisao") or "").strip()
            observacao = (request.form.get("observacao_decisao") or "").strip()

            validas = [v for v, _ in CREDITO_OPCOES["decisao"]]

            if decisao not in validas:
                flash("Escolha aprovar ou reprovar.", "error")
            elif not observacao:
                # O motivo é obrigatório nos dois sentidos: é ele que explica a
                # decisão depois, quando ninguém mais lembra do caso.
                flash("Informe a observação justificando a decisão.", "error")
            else:
                limite = _credito_numero(request.form.get("limite_aprovado"))
                prazo = _credito_inteiro(request.form.get("prazo_aprovado_dias"))

                if decisao == "REPROVADO":
                    limite = None
                    prazo = None

                cur.execute("""
                    UPDATE credito_analises
                    SET status = %s,
                        limite_aprovado = %s,
                        prazo_aprovado_dias = %s,
                        observacao_decisao = %s,
                        id_usuario_aprovador = %s,
                        nome_aprovador = %s,
                        data_decisao = %s,
                        atualizado_em = NOW()
                    WHERE id_analise = %s AND cod_empresa = %s
                """, (
                    decisao,
                    limite,
                    prazo,
                    observacao,
                    session.get("id_usuario"),
                    session.get("nome_usuario") or session.get("usuario"),
                    date.today(),
                    id_analise,
                    cod_empresa,
                ))
                conn.commit()
                flash("Decisão registrada com sucesso.", "success")
                return redirect(url_for("financeiro.credito_aprovacoes"))

            cur.execute("""
                SELECT * FROM credito_analises
                WHERE id_analise = %s AND cod_empresa = %s
            """, (id_analise, cod_empresa))
            analise = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    return render_template(
        "credito_decisao.html",
        empresa_ativa=cod_empresa,
        nome_empresa_ativa=session.get("nome_empresa", ""),
        url_voltar=url_for("financeiro.credito_aprovacoes"),
        texto_voltar="← Voltar",
        analise=analise,
        opcoes=CREDITO_OPCOES,
        status_rotulo=CREDITO_STATUS_ROTULO,
        formatar_numero_br=formatar_numero_br,
    )
