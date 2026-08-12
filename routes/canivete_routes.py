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
import calendar as _calendar_mod
import re as _re_agenda
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


def _primeira_seg_do_mes(ano: int, mes: int) -> date:
    """
    Segunda-feira da 1ª semana do mês.
    A semana que contém o dia 1 só conta como 1ª semana se o dia 1 cair
    entre segunda e quarta. De quinta em diante, a 1ª semana é a seguinte.
    """
    d1 = date(ano, mes, 1)
    seg = d1 - timedelta(days=d1.weekday())
    if d1.weekday() >= 3:          # quinta, sexta, sábado ou domingo
        seg += timedelta(weeks=1)
    return seg


def _mes_seguinte(ano: int, mes: int):
    return (ano + 1, 1) if mes == 12 else (ano, mes + 1)


def _total_semanas_do_mes(ano: int, mes: int) -> int:
    """
    Quantas semanas o mês tem. A última linha só conta como semana própria
    se tiver pelo menos 2 dias do mês — ou seja, se o mês seguinte começar
    de quarta em diante. Caso contrário aquela linha pertence só ao mês novo.
    """
    ini = _primeira_seg_do_mes(ano, mes)
    ultimo = date(ano, mes, _calendar_mod.monthrange(ano, mes)[1])
    seg_ultimo = ultimo - timedelta(days=ultimo.weekday())

    dias_no_ultimo = (ultimo - max(seg_ultimo, date(ano, mes, 1))).days + 1
    semanas_ate = (seg_ultimo - ini).days // 7
    return semanas_ate + 1 if dias_no_ultimo >= 2 else semanas_ate


def _semanas_do_row(seg: date):
    """
    Devolve [(indice, total), ...] para cada mês de que esta linha faz parte.
    Uma linha de virada participa dos dois meses: pode ser a última semana
    de um (S5) e a primeira do seguinte (S1).
    """
    dom = seg + timedelta(days=6)          # domingo da mesma linha
    ant = date(seg.year - 1, 12, 1) if seg.month == 1 else date(seg.year, seg.month - 1, 1)

    candidatos = []
    for ano, mes in [(ant.year, ant.month), (seg.year, seg.month), (dom.year, dom.month)]:
        if (ano, mes) not in candidatos:
            candidatos.append((ano, mes))

    resultado = []
    for ano, mes in candidatos:
        ini = _primeira_seg_do_mes(ano, mes)
        if ini > seg:
            continue
        total  = _total_semanas_do_mes(ano, mes)
        indice = (seg - ini).days // 7 + 1
        if 1 <= indice <= total:
            resultado.append((indice, total))
    return resultado


def _indice_principal(seg: date) -> int:
    """Índice da semana no mês que tem mais dias nesta linha (usado como padrão)."""
    dias = [seg + timedelta(days=i) for i in range(7)]
    meses = {}
    for d in dias:
        meses[(d.year, d.month)] = meses.get((d.year, d.month), 0) + 1
    ano, mes = max(meses, key=lambda k: meses[k])
    ini = _primeira_seg_do_mes(ano, mes)
    if ini > seg:
        return 1
    return (seg - ini).days // 7 + 1


def _semana_efetiva(codigo: str, total: int):
    """
    Converte um código S1..S5 no índice real da semana.
    Códigos além do total de semanas do mês caem na última semana.
    Devolve None para códigos que não sejam do tipo S.
    """
    c = (codigo or "").strip().upper()
    if len(c) == 2 and c[0] == "S" and c[1].isdigit() and 1 <= int(c[1]) <= 5:
        return min(int(c[1]), total)
    return None


def _codigo_casa_linha(codigo: str, dias, pares) -> bool:
    """
    Diz se a tarefa deve aparecer nesta linha da agenda.

    S       — toda semana, sem exceção.
    S1..S5  — semana do mês (ver _semana_efetiva).
    D1..D31 — dia do mês: aparece na semana que contém aquele dia.
              Se o mês não tiver o dia (D31 em fevereiro), o código é
              preservado e a tarefa cai na semana do último dia do mês.
    Branco  — não repete.
    """
    c = (codigo or "").strip().upper()
    if not c:
        return False

    if c == "S":
        return True

    if c[0] == "D":
        try:
            n = int(c[1:])
        except ValueError:
            return False
        for ano, mes in {(d.year, d.month) for d in dias}:
            ultimo = _calendar_mod.monthrange(ano, mes)[1]
            alvo   = min(n, ultimo)        # D31 em fevereiro vira o dia 28/29
            if any(d.year == ano and d.month == mes and d.day == alvo
                   for d in dias):
                return True
        return False

    return any(_semana_efetiva(c, total) == indice for indice, total in pares)


def _codigo_dia_casa(codigo: str, dia) -> bool:
    """
    D  — repete todo dia.
    DU — repete só em dias úteis (segunda a sexta).
    Branco — não repete (só aparece no dia em que foi criada).
    """
    c = (codigo or "").strip().upper()
    if c == "D":
        return True
    if c == "DU":
        return dia.weekday() < 5   # 0=Seg .. 4=Sex
    return False


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

    # início: segunda da semana que contém ref
    inicio = ref - timedelta(days=ref.weekday())
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
        SELECT id, texto, codigo, ordem, semana_inicio, semana_fim
        FROM agenda_rec_tarefas
        WHERE id_usuario=%s
        ORDER BY ordem, id
    """, (id_usuario,))
    tarefas = cur.fetchall()

    cur.execute("""
        SELECT e.id_tarefa, e.semana_inicio, e.concluido
        FROM agenda_rec_execucoes e
        JOIN agenda_rec_tarefas t ON t.id = e.id_tarefa
        WHERE t.id_usuario=%s AND e.semana_inicio BETWEEN %s AND %s
    """, (id_usuario, inicio, fim))
    execs = {
        (e["id_tarefa"], e["semana_inicio"].isoformat()): e["concluido"]
        for e in cur.fetchall()
    }

    # programação do dia (uma caixa única, sempre a de hoje)
    cur.execute("""
        SELECT id, texto, codigo, ordem, dia_inicio, dia_fim
        FROM agenda_dia_tarefas
        WHERE id_usuario=%s
        ORDER BY ordem, id
    """, (id_usuario,))
    tarefas_dia = cur.fetchall()

    cur.execute("""
        SELECT id_tarefa, concluido
        FROM agenda_dia_execucoes
        WHERE id_tarefa = ANY(%s) AND dia=%s
    """, ([t["id"] for t in tarefas_dia] or [-1], hoje))
    execs_dia = {e["id_tarefa"]: e["concluido"] for e in cur.fetchall()}

    # projetos (3ª coluna) — pode carregar as recorrências do mês sozinho
    projetos, competencia = _dados_projetos(cur, id_usuario, hoje)

    conn.commit()
    cur.close(); conn.close()

    # distribuir as tarefas nas semanas certas de cada mês
    recorrentes  = {}
    indice_semana = {}
    for sem in semanas:
        seg = sem[0]
        chave = seg.isoformat()
        indice_semana[chave] = _indice_principal(seg)

        pares = _semanas_do_row(seg)
        linha = []
        for t in tarefas:
            ini_t = t["semana_inicio"]
            fim_t = t["semana_fim"]

            if ini_t and seg < ini_t:
                continue                       # ainda não nasceu
            if fim_t and seg > fim_t:
                continue                       # já parou de repetir

            # aparece sempre na semana onde foi criada
            mostra = (ini_t == seg)
            # e nas semanas seguintes que casam com o código
            if not mostra and seg > (ini_t or seg):
                mostra = _codigo_casa_linha(t["codigo"], sem, pares)

            if mostra:
                linha.append({
                    "id":        t["id"],
                    "texto":     t["texto"] or "",
                    "codigo":    t["codigo"] or "",
                    "concluido": execs.get((t["id"], chave), False),
                })
        recorrentes[chave] = linha

    # Programação do dia: sempre a lista de HOJE (data real do servidor).
    # Não recorrentes (código em branco) só aparecem no dia em que foram
    # criadas; D repete todo dia; DU só em dias úteis (seg-sex).
    programacao_hoje = []
    for t in tarefas_dia:
        ini_t, fim_t = t["dia_inicio"], t["dia_fim"]
        if ini_t and hoje < ini_t:
            continue
        if fim_t and hoje > fim_t:
            continue
        mostra = (ini_t == hoje)
        if not mostra and hoje > ini_t:
            mostra = _codigo_dia_casa(t["codigo"], hoje)
        if mostra:
            programacao_hoje.append({
                "id":        t["id"],
                "texto":     t["texto"] or "",
                "codigo":    t["codigo"] or "",
                "concluido": execs_dia.get(t["id"], False),
            })

    blocos = {}
    for r2 in rows:
        key = (r2["data"].isoformat(), r2["turno"])
        try:
            slots = _json_agenda.loads(r2["slots"] or "[]") if isinstance(r2["slots"], str) else (r2["slots"] or [])
        except Exception:
            slots = []
        # o que foi salvo fora de ordem antes desta correção também sai ordenado
        slots.sort(key=lambda x: (x or {}).get("hora") or "99:99")
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
        semana_atual=(hoje - timedelta(days=hoje.weekday())).isoformat(),
        ref=ref.isoformat(),
        prev_ref=prev_ref,
        next_ref=next_ref,
        hoje_ref=hoje.isoformat(),
        dias_pt=DIAS_PT, meses_pt=MESES_PT,
        feriados=feriados,
        recorrentes=recorrentes,
        indice_semana=indice_semana,
        programacao_hoje=programacao_hoje,
        projetos=projetos,
        competencia_projetos=competencia.isoformat(),
        journal_ultimas=JOURNAL_ULTIMAS,
        url_voltar=url_for("canivete.menu_canivete"),
    )


_RE_CODIGO = _re_agenda.compile(r"^(S[1-5]?|D(?:[1-9]|[12][0-9]|3[01]))$")

ERRO_CODIGO = ("Código inválido. Use S para toda semana, S1–S5 para semana "
               "do mês, ou D1–D31 para dia do mês. Em branco não repete.")

_RE_CODIGO_DIA = _re_agenda.compile(r"^(D|DU)$")

ERRO_CODIGO_DIA = ("Código inválido. Use D para repetir todo dia, "
                    "DU para repetir só em dias úteis. Em branco não repete.")


def _norm_codigo_dia(codigo: str):
    """Mesma ideia de _norm_codigo, mas para a programação do dia (D / DU)."""
    c = (codigo or "").strip().upper().replace(" ", "")
    if not c:
        return "", None
    if _RE_CODIGO_DIA.match(c):
        return c, None
    return None, ERRO_CODIGO_DIA


def _norm_codigo(codigo: str):
    """
    Devolve (codigo, erro).
    Branco é válido e significa "não repete". Formato inválido devolve erro.
    """
    c = (codigo or "").strip().upper().replace(" ", "")
    if not c:
        return "", None
    if _RE_CODIGO.match(c):
        return c, None
    return None, ERRO_CODIGO


@canivete_bp.route("/agenda/recorrente/criar", methods=["POST"])
def agenda_recorrente_criar():
    """Cria uma tarefa recorrente e devolve o id."""
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]

    texto  = request.form.get("texto", "")
    semana = request.form.get("semana", "")
    codigo, erro = _norm_codigo(request.form.get("codigo", ""))
    if erro:
        return jsonify({"ok": False, "erro": erro}), 400

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO agenda_rec_tarefas (id_usuario, texto, codigo, semana_inicio, ordem)
        VALUES (%s, %s, %s, %s,
                COALESCE((SELECT MAX(ordem)+1 FROM agenda_rec_tarefas WHERE id_usuario=%s), 0))
        RETURNING id
    """, (id_usuario, texto, codigo, semana, id_usuario))
    novo_id = cur.fetchone()[0]
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True, "id": novo_id, "codigo": codigo})


@canivete_bp.route("/agenda/recorrente/salvar", methods=["POST"])
def agenda_recorrente_salvar():
    """
    Atualiza texto e código de uma tarefa recorrente.

    Mudar o código vale da semana editada para a frente: a série antiga é
    encerrada na semana anterior e nasce uma nova a partir daqui. Assim o
    passado fica intacto. Código em branco simplesmente para de repetir.
    """
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]

    id_tarefa = request.form.get("id", "")
    texto     = request.form.get("texto", "")
    semana    = request.form.get("semana", "")
    codigo, erro = _norm_codigo(request.form.get("codigo", ""))
    if erro:
        return jsonify({"ok": False, "erro": erro}), 400

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT codigo, semana_inicio FROM agenda_rec_tarefas
        WHERE id=%s AND id_usuario=%s
    """, (id_tarefa, id_usuario))
    atual = cur.fetchone()
    if not atual:
        cur.close(); conn.close()
        return jsonify({"ok": False}), 404

    try:
        seg = date.fromisoformat(semana)
    except ValueError:
        seg = atual["semana_inicio"]

    mudou_codigo = (atual["codigo"] or "") != codigo
    novo_id = None

    if mudou_codigo and atual["semana_inicio"] and seg > atual["semana_inicio"]:
        # encerra a série antiga na semana anterior e abre uma nova aqui
        cur.execute("""
            UPDATE agenda_rec_tarefas SET texto=%s, semana_fim=%s
            WHERE id=%s AND id_usuario=%s
        """, (texto, seg - timedelta(weeks=1), id_tarefa, id_usuario))
        cur.execute("""
            INSERT INTO agenda_rec_tarefas
                (id_usuario, texto, codigo, semana_inicio, ordem)
            VALUES (%s, %s, %s, %s,
                    COALESCE((SELECT MAX(ordem)+1 FROM agenda_rec_tarefas
                              WHERE id_usuario=%s), 0))
            RETURNING id
        """, (id_usuario, texto, codigo, seg, id_usuario))
        novo_id = cur.fetchone()["id"]
    else:
        cur.execute("""
            UPDATE agenda_rec_tarefas SET texto=%s, codigo=%s
            WHERE id=%s AND id_usuario=%s
        """, (texto, codigo, id_tarefa, id_usuario))

    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True, "codigo": codigo,
                    "recarregar": mudou_codigo, "novo_id": novo_id})


@canivete_bp.route("/agenda/recorrente/excluir", methods=["POST"])
def agenda_recorrente_excluir():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM agenda_rec_tarefas WHERE id=%s AND id_usuario=%s",
                (request.form.get("id", ""), id_usuario))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True})


@canivete_bp.route("/agenda/recorrente/concluir", methods=["POST"])
def agenda_recorrente_concluir():
    """Marca/desmarca a execução de uma tarefa numa semana específica."""
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]

    id_tarefa = request.form.get("id", "")
    semana    = request.form.get("semana", "")
    concluido = request.form.get("concluido", "0") == "1"

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO agenda_rec_execucoes (id_tarefa, semana_inicio, concluido)
        SELECT %s, %s, %s
        WHERE EXISTS (SELECT 1 FROM agenda_rec_tarefas WHERE id=%s AND id_usuario=%s)
        ON CONFLICT (id_tarefa, semana_inicio)
        DO UPDATE SET concluido = EXCLUDED.concluido
    """, (id_tarefa, semana, concluido, id_tarefa, id_usuario))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# PROGRAMAÇÃO DO DIA — mesma ideia das recorrentes, mas por dia em vez de
# semana (código D / DU em vez de S1-S5 / D1-D31).
# ---------------------------------------------------------------------------

@canivete_bp.route("/agenda/dia/criar", methods=["POST"])
def agenda_dia_criar():
    """Cria uma tarefa da programação do dia e devolve o id."""
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]

    texto  = request.form.get("texto", "")
    dia    = request.form.get("dia", "")
    codigo, erro = _norm_codigo_dia(request.form.get("codigo", ""))
    if erro:
        return jsonify({"ok": False, "erro": erro}), 400

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO agenda_dia_tarefas (id_usuario, texto, codigo, dia_inicio, ordem)
        VALUES (%s, %s, %s, %s,
                COALESCE((SELECT MAX(ordem)+1 FROM agenda_dia_tarefas WHERE id_usuario=%s), 0))
        RETURNING id
    """, (id_usuario, texto, codigo, dia, id_usuario))
    novo_id = cur.fetchone()[0]
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True, "id": novo_id, "codigo": codigo})


@canivete_bp.route("/agenda/dia/salvar", methods=["POST"])
def agenda_dia_salvar():
    """
    Atualiza texto e código de uma tarefa da programação do dia.

    Mudar o código vale do dia editado para a frente: a série antiga é
    encerrada no dia anterior e nasce uma nova a partir daqui — mesmo
    princípio usado nas recorrentes semanais.
    """
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]

    id_tarefa = request.form.get("id", "")
    texto     = request.form.get("texto", "")
    dia_str   = request.form.get("dia", "")
    codigo, erro = _norm_codigo_dia(request.form.get("codigo", ""))
    if erro:
        return jsonify({"ok": False, "erro": erro}), 400

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT codigo, dia_inicio FROM agenda_dia_tarefas
        WHERE id=%s AND id_usuario=%s
    """, (id_tarefa, id_usuario))
    atual = cur.fetchone()
    if not atual:
        cur.close(); conn.close()
        return jsonify({"ok": False}), 404

    try:
        dia = date.fromisoformat(dia_str)
    except ValueError:
        dia = atual["dia_inicio"]

    mudou_codigo = (atual["codigo"] or "") != codigo
    novo_id = None

    if mudou_codigo and atual["dia_inicio"] and dia > atual["dia_inicio"]:
        # encerra a série antiga no dia anterior e abre uma nova aqui
        cur.execute("""
            UPDATE agenda_dia_tarefas SET texto=%s, dia_fim=%s
            WHERE id=%s AND id_usuario=%s
        """, (texto, dia - timedelta(days=1), id_tarefa, id_usuario))
        cur.execute("""
            INSERT INTO agenda_dia_tarefas
                (id_usuario, texto, codigo, dia_inicio, ordem)
            VALUES (%s, %s, %s, %s,
                    COALESCE((SELECT MAX(ordem)+1 FROM agenda_dia_tarefas
                              WHERE id_usuario=%s), 0))
            RETURNING id
        """, (id_usuario, texto, codigo, dia, id_usuario))
        novo_id = cur.fetchone()["id"]
    else:
        cur.execute("""
            UPDATE agenda_dia_tarefas SET texto=%s, codigo=%s
            WHERE id=%s AND id_usuario=%s
        """, (texto, codigo, id_tarefa, id_usuario))

    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True, "codigo": codigo,
                    "recarregar": mudou_codigo, "novo_id": novo_id})


@canivete_bp.route("/agenda/dia/excluir", methods=["POST"])
def agenda_dia_excluir():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM agenda_dia_tarefas WHERE id=%s AND id_usuario=%s",
                (request.form.get("id", ""), id_usuario))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok": True})


@canivete_bp.route("/agenda/dia/concluir", methods=["POST"])
def agenda_dia_concluir():
    """Marca/desmarca a execução de uma tarefa da programação do dia."""
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]

    id_tarefa = request.form.get("id", "")
    dia       = request.form.get("dia", "")
    concluido = request.form.get("concluido", "0") == "1"

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO agenda_dia_execucoes (id_tarefa, dia, concluido)
        SELECT %s, %s, %s
        WHERE EXISTS (SELECT 1 FROM agenda_dia_tarefas WHERE id=%s AND id_usuario=%s)
        ON CONFLICT (id_tarefa, dia)
        DO UPDATE SET concluido = EXCLUDED.concluido
    """, (id_tarefa, dia, concluido, id_tarefa, id_usuario))
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

    # sempre em ordem de horário — quem digitou fora de ordem (16:00 depois
    # das 17:00) não precisa se preocupar. Sem hora vai para o fim.
    slots.sort(key=lambda s: (s or {}).get("hora") or "99:99")

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


# ─────────────────────────────────────────────────────────────
#  PROJETOS DA AGENDA
#
#  Terceira coluna da agenda. Diferente de Recorrentes e da
#  Programação do Dia, não fica preso à altura das linhas da
#  grade — é um painel próprio, ao lado da tabela.
#
#  Cada projeto tem 3 blocos: metas, recorrências mensais e
#  tarefas eventuais. Tudo por USUÁRIO (não tem cod_empresa).
# ─────────────────────────────────────────────────────────────

def _competencia_atual(hoje=None):
    """Competência = 1º dia do mês corrente."""
    h = hoje or date.today()
    return date(h.year, h.month, 1)


def _projeto_do_usuario(cur, id_projeto, id_usuario) -> bool:
    cur.execute("SELECT 1 FROM agenda_projetos WHERE id=%s AND id_usuario=%s",
                (id_projeto, id_usuario))
    return cur.fetchone() is not None


def _carregar_recorrencias(cur, id_projeto, competencia) -> int:
    """
    Cria as execuções da competência para as recorrências ativas do projeto.
    Idempotente (ON CONFLICT DO NOTHING). Devolve quantas nasceram.
    """
    cur.execute("""
        INSERT INTO agenda_proj_rec_execucoes (id_recorrencia, competencia)
        SELECT r.id, %s
        FROM agenda_proj_recorrencias r
        WHERE r.id_projeto=%s AND r.ativo AND COALESCE(TRIM(r.texto),'') <> ''
        ON CONFLICT (id_recorrencia, competencia) DO NOTHING
    """, (competencia, id_projeto))
    n = cur.rowcount or 0
    cur.execute("UPDATE agenda_projetos SET competencia=%s WHERE id=%s",
                (competencia, id_projeto))
    return n


def _tem_pendencia_anterior(cur, id_projeto, competencia) -> bool:
    """Sobrou recorrência pendente de competência anterior à informada?"""
    cur.execute("""
        SELECT 1
        FROM agenda_proj_rec_execucoes e
        JOIN agenda_proj_recorrencias r ON r.id = e.id_recorrencia
        WHERE r.id_projeto=%s AND e.competencia < %s AND e.situacao='pendente'
        LIMIT 1
    """, (id_projeto, competencia))
    return cur.fetchone() is not None


def _tarefas_bloco(tarefas, id_projeto, bloco):
    """Metas ou eventuais de um projeto, com a data da conclusão (para o filtro)."""
    return [
        {
            "id":        t["id"],
            "texto":     t["texto"] or "",
            "concluido": t["concluido"],
            "cancelado": t["cancelado"],
            # data de saída da lista: serve ao filtro por período do 👁
            "quando":    (t["cancelado_em"] or t["concluido_em"]).date().isoformat()
                         if (t["cancelado_em"] or t["concluido_em"]) else "",
        }
        for t in tarefas
        if t["id_projeto"] == id_projeto and t["bloco"] == bloco
    ]


def _dados_projetos(cur, id_usuario, hoje=None):
    """
    Monta a lista de projetos com os 3 blocos preenchidos.

    Na virada do mês as recorrências do novo mês entram sozinhas — mas só
    se o mês anterior estiver zerado (tudo concluído ou cancelado). Se ficou
    pendência, o projeto marca `pode_carregar` e o usuário decide quando
    carregar pelo botão.
    """
    comp = _competencia_atual(hoje)

    cur.execute("""
        SELECT id, nome, ordem, competencia
        FROM agenda_projetos
        WHERE id_usuario=%s AND ativo
        ORDER BY ordem, id
    """, (id_usuario,))
    projetos = cur.fetchall()
    if not projetos:
        return [], comp

    # carga automática do mês, projeto a projeto
    for p in projetos:
        if p["competencia"] == comp:
            continue
        if not _tem_pendencia_anterior(cur, p["id"], comp):
            _carregar_recorrencias(cur, p["id"], comp)
            p["competencia"] = comp

    ids = [p["id"] for p in projetos]

    cur.execute("""
        SELECT id, id_projeto, bloco, texto, concluido, concluido_em,
               cancelado, cancelado_em, ordem
        FROM agenda_proj_tarefas
        WHERE id_projeto = ANY(%s)
        ORDER BY ordem, id
    """, (ids,))
    tarefas = cur.fetchall()

    cur.execute("""
        SELECT r.id, r.id_projeto, r.texto, r.ordem,
               e.id AS id_execucao, e.competencia, e.situacao
        FROM agenda_proj_recorrencias r
        JOIN agenda_proj_rec_execucoes e ON e.id_recorrencia = r.id
        WHERE r.id_projeto = ANY(%s)
          AND (e.competencia >= %s OR e.situacao = 'pendente')
        ORDER BY e.competencia, r.ordem, r.id
    """, (ids, comp.replace(year=comp.year - 1)))
    execucoes = cur.fetchall()

    # modelos das recorrências (para a engrenagem)
    cur.execute("""
        SELECT id, id_projeto, texto, ordem
        FROM agenda_proj_recorrencias
        WHERE id_projeto = ANY(%s) AND ativo
        ORDER BY ordem, id
    """, (ids,))
    modelos = cur.fetchall()

    saida = []
    for p in projetos:
        recs = [
            {
                "id_execucao":    e["id_execucao"],
                "id_recorrencia": e["id"],
                "texto":       e["texto"] or "",
                "situacao":    e["situacao"],
                "competencia": e["competencia"].isoformat(),
                "quando":      e["competencia"].isoformat(),
                "atrasada":    e["competencia"] < comp,
            }
            for e in execucoes if e["id_projeto"] == p["id"]
        ]
        pendentes_atras = any(r["atrasada"] and r["situacao"] == "pendente" for r in recs)
        saida.append({
            "id":    p["id"],
            "nome":  p["nome"],
            "metas":     _tarefas_bloco(tarefas, p["id"], "meta"),
            "eventuais": _tarefas_bloco(tarefas, p["id"], "eventual"),
            "recorrencias": recs,
            "modelos": [
                {"id": m["id"], "texto": m["texto"] or ""}
                for m in modelos if m["id_projeto"] == p["id"]
            ],
            # botão só faz sentido quando o mês ainda não entrou
            "pode_carregar":   p["competencia"] != comp,
            "trava_pendencia": pendentes_atras,
        })
    return saida, comp


@canivete_bp.route("/agenda/projeto/criar", methods=["POST"])
def agenda_projeto_criar():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    nome = (request.form.get("nome") or "").strip()
    if not nome:
        return jsonify({"ok": False, "erro": "Informe o nome do projeto."})

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        INSERT INTO agenda_projetos (id_usuario, nome, ordem, competencia)
        VALUES (%s, %s,
                COALESCE((SELECT MAX(ordem)+1 FROM agenda_projetos WHERE id_usuario=%s), 10),
                %s)
        RETURNING id
    """, (id_usuario, nome, id_usuario, _competencia_atual()))
    novo = cur.fetchone()["id"]
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "id": novo})


@canivete_bp.route("/agenda/projeto/renomear", methods=["POST"])
def agenda_projeto_renomear():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    id_projeto = request.form.get("id")
    nome = (request.form.get("nome") or "").strip()
    if not id_projeto or not nome:
        return jsonify({"ok": False, "erro": "Informe o nome do projeto."})

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("UPDATE agenda_projetos SET nome=%s WHERE id=%s AND id_usuario=%s",
                (nome, id_projeto, id_usuario))
    ok = cur.rowcount > 0
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": ok})


@canivete_bp.route("/agenda/projeto/excluir", methods=["POST"])
def agenda_projeto_excluir():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    id_projeto = request.form.get("id")

    conn = get_connection()
    cur  = conn.cursor()
    # inativa em vez de apagar: preserva o histórico das recorrências
    cur.execute("UPDATE agenda_projetos SET ativo=FALSE WHERE id=%s AND id_usuario=%s",
                (id_projeto, id_usuario))
    ok = cur.rowcount > 0
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": ok})


@canivete_bp.route("/agenda/projeto/tarefa/criar", methods=["POST"])
def agenda_projeto_tarefa_criar():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    id_projeto = request.form.get("id_projeto")
    bloco      = (request.form.get("bloco") or "").strip()
    texto      = (request.form.get("texto") or "").strip()
    if bloco not in ("meta", "eventual"):
        return jsonify({"ok": False, "erro": "bloco inválido"})

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    if not _projeto_do_usuario(cur, id_projeto, id_usuario):
        cur.close(); conn.close()
        return jsonify({"ok": False}), 403
    cur.execute("""
        INSERT INTO agenda_proj_tarefas (id_projeto, bloco, texto, ordem)
        VALUES (%s, %s, %s,
                COALESCE((SELECT MAX(ordem)+1 FROM agenda_proj_tarefas
                          WHERE id_projeto=%s AND bloco=%s), 10))
        RETURNING id
    """, (id_projeto, bloco, texto, id_projeto, bloco))
    novo = cur.fetchone()["id"]
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "id": novo})


@canivete_bp.route("/agenda/projeto/tarefa/salvar", methods=["POST"])
def agenda_projeto_tarefa_salvar():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    id_tarefa  = request.form.get("id")
    texto      = (request.form.get("texto") or "").strip()

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        UPDATE agenda_proj_tarefas t SET texto=%s
        FROM agenda_projetos p
        WHERE t.id=%s AND p.id=t.id_projeto AND p.id_usuario=%s
    """, (texto, id_tarefa, id_usuario))
    ok = cur.rowcount > 0
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": ok})


@canivete_bp.route("/agenda/projeto/tarefa/concluir", methods=["POST"])
def agenda_projeto_tarefa_concluir():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    id_tarefa  = request.form.get("id")
    concluido  = request.form.get("concluido") == "1"

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        UPDATE agenda_proj_tarefas t
        SET concluido=%s, concluido_em = CASE WHEN %s THEN now() ELSE NULL END
        FROM agenda_projetos p
        WHERE t.id=%s AND p.id=t.id_projeto AND p.id_usuario=%s
        RETURNING t.id_projeto, t.texto
    """, (concluido, concluido, id_tarefa, id_usuario))
    linha = cur.fetchone()
    ok = linha is not None
    # concluir vira ocorrência no journal do projeto; reabrir não desfaz
    if ok and concluido:
        _journal_anotar_conclusao(cur, linha[0], linha[1])
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": ok})


@canivete_bp.route("/agenda/projeto/tarefa/cancelar", methods=["POST"])
def agenda_projeto_tarefa_cancelar():
    """Cancela (ou reabre) uma meta / tarefa eventual — não apaga."""
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    id_tarefa  = request.form.get("id")
    cancelado  = request.form.get("cancelado") == "1"

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        UPDATE agenda_proj_tarefas t
        SET cancelado=%s,
            cancelado_em = CASE WHEN %s THEN now() ELSE NULL END,
            concluido    = CASE WHEN %s THEN FALSE ELSE t.concluido END
        FROM agenda_projetos p
        WHERE t.id=%s AND p.id=t.id_projeto AND p.id_usuario=%s
    """, (cancelado, cancelado, cancelado, id_tarefa, id_usuario))
    ok = cur.rowcount > 0
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": ok})


@canivete_bp.route("/agenda/projeto/tarefa/excluir", methods=["POST"])
def agenda_projeto_tarefa_excluir():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    id_tarefa  = request.form.get("id")

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        DELETE FROM agenda_proj_tarefas t
        USING agenda_projetos p
        WHERE t.id=%s AND p.id=t.id_projeto AND p.id_usuario=%s
    """, (id_tarefa, id_usuario))
    ok = cur.rowcount > 0
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": ok})


@canivete_bp.route("/agenda/projeto/item/mandar-para-o-fim", methods=["POST"])
def agenda_projeto_item_para_o_fim():
    """
    Manda a primeira linha do bloco para o fim da fila (a seta ▼).
    Serve aos três blocos: metas, recorrências mensais e eventuais.
    """
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    tipo = (request.form.get("tipo") or "").strip()
    id_item = request.form.get("id")
    if tipo not in ("tarefa", "recorrencia"):
        return jsonify({"ok": False, "erro": "tipo inválido"})

    conn = get_connection()
    cur  = conn.cursor()
    if tipo == "tarefa":
        cur.execute("""
            UPDATE agenda_proj_tarefas t
            SET ordem = COALESCE((SELECT MAX(o.ordem) FROM agenda_proj_tarefas o
                                  WHERE o.id_projeto=t.id_projeto AND o.bloco=t.bloco), 0) + 10
            FROM agenda_projetos p
            WHERE t.id=%s AND p.id=t.id_projeto AND p.id_usuario=%s
        """, (id_item, id_usuario))
    else:
        cur.execute("""
            UPDATE agenda_proj_recorrencias r
            SET ordem = COALESCE((SELECT MAX(o.ordem) FROM agenda_proj_recorrencias o
                                  WHERE o.id_projeto=r.id_projeto), 0) + 10
            FROM agenda_projetos p
            WHERE r.id=%s AND p.id=r.id_projeto AND p.id_usuario=%s
        """, (id_item, id_usuario))
    ok = cur.rowcount > 0
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": ok})


@canivete_bp.route("/agenda/projeto/recorrencias/salvar", methods=["POST"])
def agenda_projeto_recorrencias_salvar():
    """
    Grava a lista inteira de recorrências mensais do projeto (a janela da
    engrenagem manda tudo). O que sumiu da lista é inativado — as execuções
    já lançadas continuam de pé.
    """
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    id_projeto = request.form.get("id_projeto")
    try:
        itens = _json_agenda.loads(request.form.get("itens") or "[]")
    except Exception:
        return jsonify({"ok": False, "erro": "itens inválidos"})

    conn = get_connection()
    cur  = conn.cursor()
    if not _projeto_do_usuario(cur, id_projeto, id_usuario):
        cur.close(); conn.close()
        return jsonify({"ok": False}), 403

    mantidos = []
    for ordem, it in enumerate(itens, start=1):
        texto = (it.get("texto") or "").strip()
        if not texto:
            continue
        rid = it.get("id")
        if rid:
            cur.execute("""
                UPDATE agenda_proj_recorrencias
                SET texto=%s, ordem=%s, ativo=TRUE
                WHERE id=%s AND id_projeto=%s
            """, (texto, ordem * 10, rid, id_projeto))
            if cur.rowcount:
                mantidos.append(int(rid))
                continue
        cur.execute("""
            INSERT INTO agenda_proj_recorrencias (id_projeto, texto, ordem)
            VALUES (%s,%s,%s) RETURNING id
        """, (id_projeto, texto, ordem * 10))
        mantidos.append(cur.fetchone()[0])

    cur.execute("""
        UPDATE agenda_proj_recorrencias SET ativo=FALSE
        WHERE id_projeto=%s AND NOT (id = ANY(%s))
    """, (id_projeto, mantidos or [-1]))

    # recorrência nova entra já no mês corrente, se o mês já foi carregado
    cur.execute("SELECT competencia FROM agenda_projetos WHERE id=%s", (id_projeto,))
    comp_projeto = cur.fetchone()[0]
    comp = _competencia_atual()
    if comp_projeto == comp:
        cur.execute("""
            INSERT INTO agenda_proj_rec_execucoes (id_recorrencia, competencia)
            SELECT r.id, %s FROM agenda_proj_recorrencias r
            WHERE r.id_projeto=%s AND r.ativo
            ON CONFLICT (id_recorrencia, competencia) DO NOTHING
        """, (comp, id_projeto))
    # execução pendente de recorrência inativada não faz mais sentido
    cur.execute("""
        DELETE FROM agenda_proj_rec_execucoes e
        USING agenda_proj_recorrencias r
        WHERE e.id_recorrencia=r.id AND r.id_projeto=%s
          AND NOT r.ativo AND e.situacao='pendente'
    """, (id_projeto,))

    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True})


@canivete_bp.route("/agenda/projeto/recorrencia/situacao", methods=["POST"])
def agenda_projeto_recorrencia_situacao():
    """Concluir, cancelar ou reabrir a execução do mês."""
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    id_exec    = request.form.get("id")
    situacao   = (request.form.get("situacao") or "").strip()
    if situacao not in ("pendente", "concluida", "cancelada"):
        return jsonify({"ok": False, "erro": "situação inválida"})

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        UPDATE agenda_proj_rec_execucoes e SET situacao=%s
        FROM agenda_proj_recorrencias r, agenda_projetos p
        WHERE e.id=%s AND r.id=e.id_recorrencia
          AND p.id=r.id_projeto AND p.id_usuario=%s
        RETURNING r.id_projeto, r.texto
    """, (situacao, id_exec, id_usuario))
    linha = cur.fetchone()
    ok = linha is not None
    # só a conclusão vira ocorrência no journal — cancelar/reabrir não
    if ok and situacao == "concluida":
        _journal_anotar_conclusao(cur, linha[0], linha[1])
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": ok})


@canivete_bp.route("/agenda/projeto/recorrencias/carregar", methods=["POST"])
def agenda_projeto_recorrencias_carregar():
    """
    Carga manual das recorrências do mês. Só passa se o mês anterior estiver
    zerado — pendência velha tem que ser concluída ou cancelada antes.
    """
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    id_projeto = request.form.get("id_projeto")

    conn = get_connection()
    cur  = conn.cursor()
    if not _projeto_do_usuario(cur, id_projeto, id_usuario):
        cur.close(); conn.close()
        return jsonify({"ok": False}), 403

    comp = _competencia_atual()
    if _tem_pendencia_anterior(cur, id_projeto, comp):
        cur.close(); conn.close()
        return jsonify({"ok": False,
                        "erro": "Conclua ou cancele as recorrências pendentes "
                                "do mês anterior antes de carregar o mês."})
    n = _carregar_recorrencias(cur, id_projeto, comp)
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok": True, "criadas": n})


# ─── JOURNAL DO PROJETO ──────────────────────────────────────────────────────
#  Equivalente ao Project Journal de Projetos, mas por usuário e por projeto
#  da agenda. A tela pede as últimas N ocorrências (limite=0 traz tudo).
#  'editavel' vem do banco, no mesmo critério do UPDATE/DELETE: calcular em
#  Python usaria o fuso da máquina, que difere do UTC do banco.

JOURNAL_ULTIMAS = 5
JOURNAL_MARCA_CONCLUIDA = "✔"


def _journal_anotar_conclusao(cur, id_projeto, texto):
    """
    Toda tarefa concluída (meta, recorrência ou eventual) vira uma linha no
    journal do projeto, no registro do dia: se ainda não existe registro para
    hoje, abre um; se já existe, acrescenta no fim.

    A data é a do banco (CURRENT_DATE), a mesma que decide o 'editável' — em
    Python viria do fuso da máquina. Repetir a conclusão (desmarcar e marcar
    de novo) não duplica a linha.
    """
    texto = (texto or "").strip()
    if not texto:
        return
    linha = JOURNAL_MARCA_CONCLUIDA + " " + texto

    cur.execute("""
        UPDATE agenda_proj_journal
        SET descricao = descricao || E'\n' || %s, atualizado_em = now()
        WHERE id = (SELECT id FROM agenda_proj_journal
                    WHERE id_projeto=%s AND data=CURRENT_DATE
                    ORDER BY id LIMIT 1)
          AND descricao NOT LIKE '%%' || %s || '%%'
    """, (linha, id_projeto, linha))
    if cur.rowcount:
        return

    # rowcount 0 = ou não há registro hoje (abre um), ou a linha já está lá
    cur.execute("""
        INSERT INTO agenda_proj_journal (id_projeto, data, descricao)
        SELECT %s, CURRENT_DATE, %s
        WHERE NOT EXISTS (SELECT 1 FROM agenda_proj_journal
                          WHERE id_projeto=%s AND data=CURRENT_DATE)
    """, (id_projeto, linha, id_projeto))


def _journal_registros(cur, id_projeto, limite):
    sql = """
        SELECT id, data, descricao,
               (criado_em::date = CURRENT_DATE) AS editavel
        FROM agenda_proj_journal
        WHERE id_projeto = %s
        ORDER BY data DESC, id DESC
    """
    params = [id_projeto]
    if limite:
        sql += " LIMIT %s"
        params.append(limite)
    cur.execute(sql, params)
    return [
        {
            "id": r["id"],
            "data": r["data"].isoformat(),
            "data_br": r["data"].strftime("%d/%m/%Y"),
            "descricao": r["descricao"],
            "editavel": bool(r["editavel"]),
        }
        for r in cur.fetchall()
    ]


def _journal_resposta(cur, id_projeto, limite):
    cur.execute("SELECT COUNT(*) AS n FROM agenda_proj_journal WHERE id_projeto=%s",
                (id_projeto,))
    total = cur.fetchone()["n"]
    return {"ok": True, "total": total, "limite": limite,
            "registros": _journal_registros(cur, id_projeto, limite)}


def _journal_limite():
    """0 = tudo; qualquer outro valor cai no padrão de últimas ocorrências."""
    try:
        limite = int(request.form.get("limite") or JOURNAL_ULTIMAS)
    except ValueError:
        limite = JOURNAL_ULTIMAS
    return 0 if limite <= 0 else limite


@canivete_bp.route("/agenda/projeto/journal/listar", methods=["POST"])
def agenda_projeto_journal_listar():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    id_projeto = request.form.get("id_projeto")

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if not _projeto_do_usuario(cur, id_projeto, id_usuario):
            return jsonify({"ok": False}), 403
        return jsonify(_journal_resposta(cur, id_projeto, _journal_limite()))
    finally:
        cur.close(); conn.close()


@canivete_bp.route("/agenda/projeto/journal/salvar", methods=["POST"])
def agenda_projeto_journal_salvar():
    """Inclui (sem id) ou edita (com id) uma ocorrência."""
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    id_projeto = request.form.get("id_projeto")
    id_reg     = (request.form.get("id") or "").strip()
    data_str   = (request.form.get("data") or "").strip()
    descricao  = (request.form.get("descricao") or "").strip()

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if not _projeto_do_usuario(cur, id_projeto, id_usuario):
            return jsonify({"ok": False}), 403
        if not data_str or not descricao:
            return jsonify({"ok": False, "erro": "Preencha a data e a descrição."})

        if id_reg:
            # só edita no mesmo dia em que o registro foi criado — a condição
            # vai no próprio UPDATE para não depender do que a tela mandou
            cur.execute("""
                UPDATE agenda_proj_journal
                SET data=%s, descricao=%s, atualizado_em=now()
                WHERE id=%s AND id_projeto=%s
                  AND criado_em::date = CURRENT_DATE
            """, (data_str, descricao, id_reg, id_projeto))
            if not cur.rowcount:
                return jsonify({"ok": False,
                                "erro": "Só é possível editar registros criados hoje."})
        else:
            cur.execute("""
                INSERT INTO agenda_proj_journal (id_projeto, data, descricao)
                VALUES (%s, %s, %s)
            """, (id_projeto, data_str, descricao))
        conn.commit()
        return jsonify(_journal_resposta(cur, id_projeto, _journal_limite()))
    finally:
        cur.close(); conn.close()


@canivete_bp.route("/agenda/projeto/journal/excluir", methods=["POST"])
def agenda_projeto_journal_excluir():
    r = _checar_login()
    if r:
        return jsonify({"ok": False}), 401
    id_usuario = session["id_usuario"]
    id_projeto = request.form.get("id_projeto")
    id_reg     = request.form.get("id")

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if not _projeto_do_usuario(cur, id_projeto, id_usuario):
            return jsonify({"ok": False}), 403
        # só apaga no mesmo dia em que o registro foi criado
        cur.execute("""
            DELETE FROM agenda_proj_journal
            WHERE id=%s AND id_projeto=%s AND criado_em::date = CURRENT_DATE
        """, (id_reg, id_projeto))
        if not cur.rowcount:
            return jsonify({"ok": False,
                            "erro": "Só é possível excluir registros criados hoje."})
        conn.commit()
        return jsonify(_journal_resposta(cur, id_projeto, _journal_limite()))
    finally:
        cur.close(); conn.close()
