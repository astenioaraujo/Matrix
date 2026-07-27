from datetime import date
from flask import Blueprint, render_template, redirect, url_for, session, flash, request, jsonify
from psycopg2.extras import RealDictCursor
from security_helpers import usuario_tem_permissao
from db import get_connection

canivete_bp = Blueprint("canivete", __name__, url_prefix="/canivete")


def _checar_login():
    if "id_usuario" not in session or "cod_empresa" not in session:
        return redirect(url_for("auth.index"))
    return None


def _tipo_global():
    return str(session.get("tipo_global") or "").strip().lower()


def _tem_perm(id_usuario, cod_empresa, opcao):
    return _tipo_global() == "superusuario" or usuario_tem_permissao(
        id_usuario, cod_empresa, "CANIVETE", opcao
    )


# -----------------------------------------------------------
# MENU CANIVETE
# -----------------------------------------------------------

@canivete_bp.route("/menu")
def menu_canivete():
    r = _checar_login()
    if r:
        return r

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()

    if not _tem_perm(id_usuario, cod_empresa, "MENU"):
        flash("Você não tem permissão para acessar o Canivete Suíço.", "error")
        return redirect(url_for("sistema.selecionar_sistema"))

    session["sistema_ativo"] = "canivete"

    return render_template(
        "menu_canivete.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema"),
        pode_financas_pessoais=_tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_MENU"),
        pode_agenda=_tem_perm(id_usuario, cod_empresa, "AGENDA"),
    )


# -----------------------------------------------------------
# FINANÇAS PESSOAIS — MENU
# -----------------------------------------------------------

@canivete_bp.route("/financas-pessoais")
def menu_financas_pessoais():
    r = _checar_login()
    if r:
        return r

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()

    if not _tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_MENU"):
        flash("Você não tem permissão para acessar Finanças Pessoais.", "error")
        return redirect(url_for("canivete.menu_canivete"))

    return render_template(
        "canivete/financas_pessoais/menu.html",
        nome_usuario=session.get("nome_usuario"),
        url_voltar=url_for("canivete.menu_canivete"),
        pode_lancar=_tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_LANCAR"),
        pode_consultar=_tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_CONSULTAR"),
        pode_configuracoes=_tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_CONFIGURACOES"),
    )


# -----------------------------------------------------------
# FINANÇAS PESSOAIS — LANÇAR
# -----------------------------------------------------------

@canivete_bp.route("/financas-pessoais/lancar", methods=["GET", "POST"])
def fp_lancar():
    r = _checar_login()
    if r:
        return r

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()

    if not _tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_LANCAR"):
        flash("Sem permissão.", "error")
        return redirect(url_for("canivete.menu_financas_pessoais"))

    hoje = date.today()
    mes_sel = int(request.args.get("mes") or request.form.get("mes") or hoje.month)
    ano_sel = int(request.args.get("ano") or request.form.get("ano") or hoje.year)

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    try:
        # Salvar novo lançamento
        if request.method == "POST" and request.form.get("acao") == "incluir":
            data_lancamento = request.form.get("data_lancamento")
            descricao = (request.form.get("descricao") or "").strip()
            valor_raw = (request.form.get("valor") or "0").replace(".", "").replace(",", ".")
            id_classificacao = request.form.get("id_classificacao") or None

            try:
                valor = float(valor_raw)
            except ValueError:
                valor = 0.0

            id_conta_bancaria = request.form.get("id_conta_bancaria") or None

            if descricao and data_lancamento:
                cur.execute("""
                    INSERT INTO fp_lancamentos (id_usuario, data, descricao, valor, id_classificacao, id_conta_bancaria)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (id_usuario, data_lancamento, descricao, valor, id_classificacao, id_conta_bancaria))
                novo_id = cur.fetchone()["id"]
                conn.commit()

                if is_ajax:
                    cur.execute("""
                        SELECT l.id, l.data, l.descricao, l.valor,
                               c.nome AS classificacao, l.id_classificacao,
                               b.nome AS conta_bancaria, l.id_conta_bancaria
                        FROM fp_lancamentos l
                        LEFT JOIN fp_classificacoes c ON c.id = l.id_classificacao
                        LEFT JOIN fp_contas_bancarias b ON b.id = l.id_conta_bancaria
                        WHERE l.id = %s
                    """, (novo_id,))
                    row = cur.fetchone()
                    cur.close()
                    conn.close()
                    return jsonify({
                        "ok": True,
                        "id": row["id"],
                        "data": row["data"].strftime("%d/%m/%Y"),
                        "data_iso": row["data"].isoformat(),
                        "descricao": row["descricao"],
                        "valor": float(row["valor"]),
                        "id_classificacao": row["id_classificacao"],
                        "classificacao": row["classificacao"] or "",
                        "id_conta_bancaria": row["id_conta_bancaria"],
                        "conta_bancaria": row["conta_bancaria"] or "—",
                    })

                flash("Lançamento incluído.", "success")

        # Alterar classificação
        if request.method == "POST" and request.form.get("acao") == "classificar":
            id_lanc = request.form.get("id_lanc")
            id_classificacao = request.form.get("id_classificacao") or None
            if id_lanc:
                cur.execute("""
                    UPDATE fp_lancamentos SET id_classificacao = %s
                    WHERE id = %s AND id_usuario = %s
                """, (id_classificacao, id_lanc, id_usuario))
                conn.commit()
            if is_ajax:
                cur.close()
                conn.close()
                return jsonify({"ok": True})
            proxima_url = request.form.get("proxima_url")
            if proxima_url:
                cur.close()
                conn.close()
                return redirect(proxima_url)

        # Excluir lançamento
        if request.method == "POST" and request.form.get("acao") == "excluir":
            id_excluir = request.form.get("id_excluir")
            if id_excluir:
                cur.execute("DELETE FROM fp_lancamentos WHERE id = %s AND id_usuario = %s",
                            (id_excluir, id_usuario))
                conn.commit()
            if is_ajax:
                cur.close()
                conn.close()
                return jsonify({"ok": True})
            flash("Lançamento excluído.", "success")

        # Buscar lançamentos do mês/ano
        cur.execute("""
            SELECT l.id, l.data, l.descricao, l.valor,
                   c.nome AS classificacao, l.id_classificacao,
                   b.nome AS conta_bancaria, l.id_conta_bancaria
            FROM fp_lancamentos l
            LEFT JOIN fp_classificacoes c ON c.id = l.id_classificacao
            LEFT JOIN fp_contas_bancarias b ON b.id = l.id_conta_bancaria
            WHERE l.id_usuario = %s
              AND EXTRACT(MONTH FROM l.data) = %s
              AND EXTRACT(YEAR  FROM l.data) = %s
            ORDER BY l.data, l.id
        """, (id_usuario, mes_sel, ano_sel))
        lancamentos = cur.fetchall() or []

        # Classificações ativas
        cur.execute("""
            SELECT id, nome FROM fp_classificacoes
            WHERE id_usuario = %s AND ativo = TRUE
            ORDER BY nome
        """, (id_usuario,))
        classificacoes = cur.fetchall() or []

        # Contas bancárias ativas
        cur.execute("""
            SELECT id, nome FROM fp_contas_bancarias
            WHERE id_usuario = %s AND ativo = TRUE
            ORDER BY nome
        """, (id_usuario,))
        contas_bancarias = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    anos = list(range(hoje.year - 3, hoje.year + 2))
    meses = list(range(1, 13))
    nomes_meses = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                   "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]

    return render_template(
        "canivete/financas_pessoais/lancar.html",
        nome_usuario=session.get("nome_usuario"),
        url_voltar=url_for("canivete.menu_financas_pessoais"),
        lancamentos=lancamentos,
        classificacoes=classificacoes,
        contas_bancarias=contas_bancarias,
        mes_sel=mes_sel,
        ano_sel=ano_sel,
        anos=anos,
        meses=meses,
        nomes_meses=nomes_meses,
        hoje=hoje.strftime("%Y-%m-%d"),
    )


# -----------------------------------------------------------
# FINANÇAS PESSOAIS — CONSULTAR
# -----------------------------------------------------------

@canivete_bp.route("/financas-pessoais/consultar")
def fp_consultar():
    r = _checar_login()
    if r:
        return r

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()

    if not _tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_CONSULTAR"):
        flash("Sem permissão.", "error")
        return redirect(url_for("canivete.menu_financas_pessoais"))

    hoje = date.today()
    mes_sel = int(request.args.get("mes") or hoje.month)
    ano_sel = int(request.args.get("ano") or hoje.year)
    filtro = request.args.get("filtro", "todos")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT l.id, l.data, l.descricao, l.valor,
                   COALESCE(c.nome, '(Sem classificação)') AS classificacao,
                   COALESCE(l.id_classificacao::text, '') AS id_classificacao,
                   COALESCE(b.nome, '—') AS conta
            FROM fp_lancamentos l
            LEFT JOIN fp_classificacoes c ON c.id = l.id_classificacao
            LEFT JOIN fp_contas_bancarias b ON b.id = l.id_conta_bancaria
            WHERE l.id_usuario = %s
              AND EXTRACT(MONTH FROM l.data) = %s
              AND EXTRACT(YEAR  FROM l.data) = %s
            ORDER BY l.data, l.id
        """, (id_usuario, mes_sel, ano_sel))
        lancamentos_raw = cur.fetchall() or []

        import json as _json
        from collections import defaultdict as _dd
        grupos_lanc = _dd(list)
        for lx in lancamentos_raw:
            grupos_lanc[lx["classificacao"]].append({
                "id": lx["id"],
                "data": lx["data"].strftime("%d/%m/%Y"),
                "descricao": lx["descricao"],
                "valor": float(lx["valor"]),
                "conta": lx["conta"],
                "id_classificacao": lx["id_classificacao"],
            })
        lancamentos_json = _json.dumps(dict(grupos_lanc), ensure_ascii=False)

        cur.execute("""
            SELECT id, nome FROM fp_classificacoes
            WHERE id_usuario = %s AND ativo = TRUE
            ORDER BY nome
        """, (id_usuario,))
        classificacoes_consulta = cur.fetchall() or []

        cur.execute("""
            SELECT COALESCE(SUM(valor), 0) AS total
            FROM fp_lancamentos
            WHERE id_usuario = %s
              AND EXTRACT(MONTH FROM data) = %s
              AND EXTRACT(YEAR  FROM data) = %s
        """, (id_usuario, mes_sel, ano_sel))
        total = cur.fetchone()["total"]

        cur.execute("""
            SELECT c.nome AS classificacao,
                   COALESCE(c.valor_orcado, 0) AS orcado,
                   COALESCE(SUM(l.valor), 0) AS realizado,
                   c.ajustar_ao_real
            FROM fp_classificacoes c
            LEFT JOIN fp_lancamentos l
                ON l.id_classificacao = c.id
               AND l.id_usuario = %s
               AND EXTRACT(MONTH FROM l.data) = %s
               AND EXTRACT(YEAR  FROM l.data) = %s
            WHERE c.id_usuario = %s
              AND c.ativo = TRUE
            GROUP BY c.id, c.nome, c.valor_orcado, c.ajustar_ao_real

            UNION ALL

            SELECT '(Sem classificação)' AS classificacao,
                   0 AS orcado,
                   COALESCE(SUM(valor), 0) AS realizado,
                   FALSE AS ajustar_ao_real
            FROM fp_lancamentos
            WHERE id_usuario = %s
              AND id_classificacao IS NULL
              AND EXTRACT(MONTH FROM data) = %s
              AND EXTRACT(YEAR  FROM data) = %s
            HAVING COALESCE(SUM(valor), 0) <> 0

            ORDER BY classificacao
        """, (id_usuario, mes_sel, ano_sel, id_usuario, id_usuario, mes_sel, ano_sel))
        totais_classificacao = cur.fetchall() or []

        total_orcado = sum(r["orcado"] for r in totais_classificacao)

    finally:
        cur.close()
        conn.close()

    anos = list(range(hoje.year - 3, hoje.year + 2))
    meses = list(range(1, 13))
    nomes_meses = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                   "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]

    return render_template(
        "canivete/financas_pessoais/consultar.html",
        nome_usuario=session.get("nome_usuario"),
        url_voltar=url_for("canivete.menu_financas_pessoais"),
        filtro=filtro,
        lancamentos_json=lancamentos_json,
        classificacoes_consulta=classificacoes_consulta,
        total=total,
        totais_classificacao=totais_classificacao,
        total_orcado=total_orcado,
        mes_sel=mes_sel,
        ano_sel=ano_sel,
        anos=anos,
        meses=meses,
        nomes_meses=nomes_meses,
    )


# -----------------------------------------------------------
# FINANÇAS PESSOAIS — CONFIGURAÇÕES (classificações)
# -----------------------------------------------------------

@canivete_bp.route("/financas-pessoais/configuracoes", methods=["GET", "POST"])
def fp_configuracoes():
    r = _checar_login()
    if r:
        return r

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()

    if not _tem_perm(id_usuario, cod_empresa, "FINANCAS_PESSOAIS_CONFIGURACOES"):
        flash("Sem permissão.", "error")
        return redirect(url_for("canivete.menu_financas_pessoais"))

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            acao = request.form.get("acao")
            tabela = request.form.get("tabela", "classificacoes")

            if tabela == "classificacoes":
                if acao == "incluir":
                    nome = (request.form.get("nome") or "").strip()
                    if nome:
                        cur.execute("INSERT INTO fp_classificacoes (id_usuario, nome) VALUES (%s, %s)", (id_usuario, nome))
                        conn.commit()
                        flash("Classificação incluída.", "success")
                elif acao == "excluir":
                    id_excluir = request.form.get("id_excluir")
                    if id_excluir:
                        cur.execute("DELETE FROM fp_classificacoes WHERE id = %s AND id_usuario = %s", (id_excluir, id_usuario))
                        conn.commit()
                        flash("Classificação excluída.", "success")
                elif acao == "editar":
                    id_editar = request.form.get("id_editar")
                    nome = (request.form.get("nome_editar") or "").strip()
                    if id_editar and nome:
                        cur.execute("UPDATE fp_classificacoes SET nome = %s, atualizado_em = NOW() WHERE id = %s AND id_usuario = %s", (nome, id_editar, id_usuario))
                        conn.commit()
                        flash("Classificação atualizada.", "success")

            elif tabela == "orcamento":
                is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
                for key, val in request.form.items():
                    if key.startswith("orcado_"):
                        id_class = key.replace("orcado_", "")
                        try:
                            valor = float((val or "0").replace(".", "").replace(",", "."))
                        except ValueError:
                            valor = 0.0
                        ajustar = request.form.get(f"ajustar_{id_class}") == "1"
                        cur.execute("""
                            UPDATE fp_classificacoes SET valor_orcado = %s, ajustar_ao_real = %s
                            WHERE id = %s AND id_usuario = %s
                        """, (valor, ajustar, id_class, id_usuario))
                conn.commit()
                if is_ajax:
                    cur.close()
                    conn.close()
                    return jsonify({"ok": True})
                flash("Orçamento salvo.", "success")

            elif tabela == "contas":
                if acao == "incluir":
                    nome = (request.form.get("nome") or "").strip()
                    if nome:
                        cur.execute("INSERT INTO fp_contas_bancarias (id_usuario, nome) VALUES (%s, %s)", (id_usuario, nome))
                        conn.commit()
                        flash("Conta bancária incluída.", "success")
                elif acao == "excluir":
                    id_excluir = request.form.get("id_excluir")
                    if id_excluir:
                        cur.execute("DELETE FROM fp_contas_bancarias WHERE id = %s AND id_usuario = %s", (id_excluir, id_usuario))
                        conn.commit()
                        flash("Conta bancária excluída.", "success")
                elif acao == "editar":
                    id_editar = request.form.get("id_editar")
                    nome = (request.form.get("nome_editar") or "").strip()
                    if id_editar and nome:
                        cur.execute("UPDATE fp_contas_bancarias SET nome = %s, atualizado_em = NOW() WHERE id = %s AND id_usuario = %s", (nome, id_editar, id_usuario))
                        conn.commit()
                        flash("Conta bancária atualizada.", "success")

        cur.execute("SELECT id, nome, ativo, valor_orcado, ajustar_ao_real FROM fp_classificacoes WHERE id_usuario = %s ORDER BY nome", (id_usuario,))
        classificacoes = cur.fetchall() or []

        cur.execute("SELECT id, nome, ativo FROM fp_contas_bancarias WHERE id_usuario = %s ORDER BY nome", (id_usuario,))
        contas_bancarias = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "canivete/financas_pessoais/configuracoes.html",
        nome_usuario=session.get("nome_usuario"),
        url_voltar=url_for("canivete.menu_financas_pessoais"),
        classificacoes=classificacoes,
        contas_bancarias=contas_bancarias,
    )


# -----------------------------------------------------------
# AGENDA PESSOAL
# -----------------------------------------------------------

import json as _json_agenda
from datetime import date, timedelta

DIAS_PT = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
MESES_PT = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
            "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]


def _easter(ano: int) -> date:
    """Algoritmo de Meeus/Jones/Butcher para a Páscoa."""
    a = ano % 19
    b, c = divmod(ano, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(114 + h + l - 7 * m, 31)
    return date(ano, month, day + 1)


_FERIADOS_FIXOS = {
    (1, 1): "Ano Novo", (4, 21): "Tiradentes", (5, 1): "Trab.",
    (9, 7): "Independência", (10, 12): "N. Sra. Aparecida",
    (11, 2): "Finados", (11, 15): "Proclamação", (11, 20): "Consciência Negra",
    (12, 25): "Natal",
}

def _feriados(anos) -> dict:
    """Retorna dict {data_iso: nome_abreviado} de feriados nacionais brasileiros."""
    feriados = {}
    for ano in anos:
        for (m, d), nome in _FERIADOS_FIXOS.items():
            feriados[date(ano, m, d).isoformat()] = nome
        pascoa = _easter(ano)
        feriados[(pascoa - timedelta(days=48)).isoformat()] = "Carnaval"
        feriados[(pascoa - timedelta(days=47)).isoformat()] = "Carnaval"
        feriados[(pascoa - timedelta(days=2)).isoformat()]  = "Sexta Santa"
        feriados[pascoa.isoformat()]                        = "Páscoa"
        feriados[(pascoa + timedelta(days=60)).isoformat()] = "Corpus Christi"
    return feriados


@canivete_bp.route("/agenda")
def agenda():
    r = _checar_login()
    if r:
        return r
    id_usuario = session["id_usuario"]

    hoje = date.today()
    ref_str = request.args.get("ref", hoje.isoformat())
    try:
        ref = date.fromisoformat(ref_str)
    except ValueError:
        ref = hoje

    # início: segunda da semana anterior à ref
    inicio = ref - timedelta(days=ref.weekday()) - timedelta(weeks=1)
    # fim: ~6 meses à frente (26 semanas)
    fim = inicio + timedelta(weeks=26) - timedelta(days=1)

    # montar lista de semanas contínuas
    semanas = []
    cur_seg = inicio
    while cur_seg <= fim:
        semanas.append([cur_seg + timedelta(days=i) for i in range(7)])
        cur_seg += timedelta(weeks=1)

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT data, turno, conteudo, quebrado, slots, concluido
        FROM agenda_blocos
        WHERE id_usuario=%s AND data BETWEEN %s AND %s
    """, (id_usuario, inicio, fim))
    rows = cur.fetchall()

    # tarefas recorrentes (uma por semana)
    cur.execute("""
        SELECT semana_inicio, itens
        FROM agenda_recorrentes
        WHERE id_usuario=%s AND semana_inicio BETWEEN %s AND %s
    """, (id_usuario, inicio, fim))
    rec_rows = cur.fetchall()
    cur.close(); conn.close()

    recorrentes = {}
    for r3 in rec_rows:
        raw = r3["itens"]
        try:
            itens = _json_agenda.loads(raw) if isinstance(raw, str) else (raw or [])
        except Exception:
            itens = []
        recorrentes[r3["semana_inicio"].isoformat()] = itens

    blocos = {}
    for r2 in rows:
        key = (r2["data"].isoformat(), r2["turno"])
        try:
            slots = _json_agenda.loads(r2["slots"] or "[]") if isinstance(r2["slots"], str) else (r2["slots"] or [])
        except Exception:
            slots = []
        blocos[key] = {
            "conteudo":  r2["conteudo"] or "",
            "quebrado":  r2["quebrado"],
            "slots":     slots,
            "concluido": r2["concluido"],
        }

    prev_ref = (ref - timedelta(weeks=4)).isoformat()
    next_ref = (ref + timedelta(weeks=4)).isoformat()

    anos_range = set(range(inicio.year, fim.year + 1))
    feriados = _feriados(anos_range)

    return render_template(
        "canivete/agenda.html",
        semanas=semanas,
        blocos=blocos,
        hoje=hoje.isoformat(),
        ref=ref.isoformat(),
        prev_ref=prev_ref,
        next_ref=next_ref,
        hoje_ref=hoje.isoformat(),
        dias_pt=DIAS_PT, meses_pt=MESES_PT,
        feriados=feriados,
        recorrentes=recorrentes,
        url_voltar=url_for("canivete.menu_canivete"),
    )


@canivete_bp.route("/agenda/recorrente/salvar", methods=["POST"])
def agenda_recorrente_salvar():
    """Salva a lista completa de itens da semana: [{texto, concluido}, ...]"""
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]

    semana = request.form.get("semana", "")
    try:
        itens = _json_agenda.loads(request.form.get("itens", "[]"))
    except Exception:
        itens = []

    payload = _json_agenda.dumps(itens)
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO agenda_recorrentes (id_usuario, semana_inicio, itens)
        VALUES (%s, %s, %s)
        ON CONFLICT (id_usuario, semana_inicio)
        DO UPDATE SET itens = EXCLUDED.itens
    """, (id_usuario, semana, payload))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True})


@canivete_bp.route("/agenda/salvar", methods=["POST"])
def agenda_salvar():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]

    data_str = request.form.get("data", "")
    turno    = request.form.get("turno", "")
    conteudo = request.form.get("conteudo", "")

    if turno not in ("manha", "tarde", "noturno") or not data_str:
        return jsonify({"ok": False, "erro": "parâmetros inválidos"})

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO agenda_blocos (id_usuario, data, turno, conteudo)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (id_usuario, data, turno)
        DO UPDATE SET conteudo=%s, atualizado_em=NOW()
    """, (id_usuario, data_str, turno, conteudo, conteudo))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@canivete_bp.route("/agenda/concluir", methods=["POST"])
def agenda_concluir():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]

    data_str  = request.form.get("data", "")
    turno     = request.form.get("turno", "")
    concluido = request.form.get("concluido") == "1"

    if turno not in ("manha", "tarde", "noturno") or not data_str:
        return jsonify({"ok": False})

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO agenda_blocos (id_usuario, data, turno, concluido)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (id_usuario, data, turno)
        DO UPDATE SET concluido=%s, atualizado_em=NOW()
    """, (id_usuario, data_str, turno, concluido, concluido))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@canivete_bp.route("/agenda/quebrar", methods=["POST"])
def agenda_quebrar():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]

    data_str = request.form.get("data", "")
    turno    = request.form.get("turno", "")
    quebrado = request.form.get("quebrado") == "1"

    if turno not in ("manha", "tarde", "noturno") or not data_str:
        return jsonify({"ok": False})

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO agenda_blocos (id_usuario, data, turno, quebrado)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (id_usuario, data, turno)
        DO UPDATE SET quebrado=%s, atualizado_em=NOW()
    """, (id_usuario, data_str, turno, quebrado, quebrado))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@canivete_bp.route("/agenda/salvar-slots", methods=["POST"])
def agenda_salvar_slots():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]

    data_str  = request.form.get("data", "")
    turno     = request.form.get("turno", "")
    slots_str = request.form.get("slots", "[]")

    try:
        slots = _json_agenda.loads(slots_str)
    except Exception:
        return jsonify({"ok": False, "erro": "slots inválidos"})

    if turno not in ("manha", "tarde", "noturno") or not data_str:
        return jsonify({"ok": False})

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO agenda_blocos (id_usuario, data, turno, quebrado, slots)
        VALUES (%s,%s,%s,TRUE,%s)
        ON CONFLICT (id_usuario, data, turno)
        DO UPDATE SET quebrado=TRUE, slots=%s, atualizado_em=NOW()
    """, (id_usuario, data_str, turno, _json_agenda.dumps(slots), _json_agenda.dumps(slots)))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})
