from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify
from psycopg2.extras import RealDictCursor
from db import get_connection
from security_helpers import permissao_obrigatoria, usuario_tem_permissao
from datetime import date
from collections import defaultdict

# Os mesmos formatadores/heatmap do painel de Vendas — a tela de gerentes é o
# mesmo grid, só que recortado por filial.
from routes.vendas_routes import (
    _agregar_diarias_sintetico,
    _detalhe_mes_filial,
    cor_excel_51,
    formatar_numero_br,
    obter_nome_mes_abrev,
)
from services.vendas_produtos import FILTRO_SQL_COMBUSTIVEL

# Mesma regra de Vistorias: superusuário vê todas as filiais; os demais, só as
# de usuarios_filiais (o chefe de pista avalia e consulta a sua).
from routes.vistorias_routes import cod_filiais_vistorias_usuario as cod_filiais_avaliacoes_usuario

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
        pode_criar_avaliacoes = True
        pode_executar_avaliacoes = True
        pode_consultar_avaliacoes = True
        pode_configurar_avaliacoes = True
        pode_performance_gerentes = True
        pode_importar_abastecimentos = True
        pode_consultar_abastecimentos = True
    else:
        pode_criar_avaliacoes = usuario_tem_permissao(
            id_usuario, cod_empresa, "PERFORMANCES", "CRIAR_AVALIACOES"
        )

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

        pode_performance_gerentes = usuario_tem_permissao(
            id_usuario,
            cod_empresa,
            "PERFORMANCES",
            "PERFORMANCE_GERENTES",
        )

        pode_importar_abastecimentos = usuario_tem_permissao(
            id_usuario, cod_empresa, "PERFORMANCES", "IMPORTAR_ABASTECIMENTOS"
        )
        pode_consultar_abastecimentos = usuario_tem_permissao(
            id_usuario, cod_empresa, "PERFORMANCES", "CONSULTAR_ABASTECIMENTOS"
        )

    pode_avaliacoes = (
        pode_criar_avaliacoes
        or pode_executar_avaliacoes
        or pode_consultar_avaliacoes
        or pode_configurar_avaliacoes
    )

    return render_template(
        "menu_performances.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema"),
        texto_voltar="← Voltar",
        pode_avaliacoes=pode_avaliacoes,
        pode_performance_gerentes=pode_performance_gerentes,
        pode_abastecimentos=(
            pode_importar_abastecimentos or pode_consultar_abastecimentos
        ),
    )


# ---------------------------------------
# MENU PERFORMANCE EM ABASTECIMENTOS
# ---------------------------------------
@performances_bp.route("/abastecimentos/menu")
@permissao_obrigatoria(
    "PERFORMANCES",
    "MENU",
    redirecionar_para="sistema.selecionar_sistema",
)
def menu_performance_abastecimentos():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_menu_abastecimentos = True
        pode_importar_abastecimentos = True
        pode_consultar_abastecimentos = True
    else:
        pode_menu_abastecimentos = usuario_tem_permissao(
            id_usuario, cod_empresa, "PERFORMANCES", "MENU_ABASTECIMENTOS"
        )
        pode_importar_abastecimentos = usuario_tem_permissao(
            id_usuario, cod_empresa, "PERFORMANCES", "IMPORTAR_ABASTECIMENTOS"
        )
        pode_consultar_abastecimentos = usuario_tem_permissao(
            id_usuario, cod_empresa, "PERFORMANCES", "CONSULTAR_ABASTECIMENTOS"
        )

    # O menu abre com a opção do próprio submenu ou com qualquer uma das duas
    # de dentro — quem já tinha acesso às telas não precisa da permissão nova.
    if not (
        pode_menu_abastecimentos
        or pode_importar_abastecimentos
        or pode_consultar_abastecimentos
    ):
        flash("Você não tem acesso à performance em abastecimentos.", "erro")
        return redirect(url_for("performances.menu_performances"))

    return render_template(
        "menu_performance_abastecimentos.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("performances.menu_performances"),
        texto_voltar="← Voltar",
        pode_importar_abastecimentos=pode_importar_abastecimentos,
        pode_consultar_abastecimentos=pode_consultar_abastecimentos,
    )


# ---------------------------------------
# MENU AVALIAÇÕES DE FUNCIONÁRIOS
# ---------------------------------------
@performances_bp.route("/avaliacoes/menu")
@permissao_obrigatoria(
    "PERFORMANCES",
    "MENU",
    redirecionar_para="sistema.selecionar_sistema",
)
def menu_avaliacoes_funcionarios():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_criar_avaliacoes = True
        pode_executar_avaliacoes = True
        pode_consultar_avaliacoes = True
        pode_configurar_avaliacoes = True
    else:
        pode_criar_avaliacoes = usuario_tem_permissao(
            id_usuario, cod_empresa, "PERFORMANCES", "CRIAR_AVALIACOES"
        )
        pode_executar_avaliacoes = usuario_tem_permissao(
            id_usuario, cod_empresa, "PERFORMANCES", "EXECUTAR_AVALIACOES"
        )
        pode_consultar_avaliacoes = usuario_tem_permissao(
            id_usuario, cod_empresa, "PERFORMANCES", "CONSULTAR_AVALIACOES"
        )
        pode_configurar_avaliacoes = usuario_tem_permissao(
            id_usuario, cod_empresa, "PERFORMANCES", "CONFIGURAR_AVALIACOES"
        )

    # O menu abre com qualquer uma das quatro; sem nenhuma, volta ao menu do módulo.
    if not (pode_criar_avaliacoes or pode_executar_avaliacoes or pode_consultar_avaliacoes or pode_configurar_avaliacoes):
        flash("Você não tem acesso às avaliações de funcionários.", "erro")
        return redirect(url_for("performances.menu_performances"))

    return render_template(
        "menu_avaliacoes_funcionarios.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("performances.menu_performances"),
        texto_voltar="← Voltar",
        pode_criar_avaliacoes=pode_criar_avaliacoes,
        pode_executar_avaliacoes=pode_executar_avaliacoes,
        pode_consultar_avaliacoes=pode_consultar_avaliacoes,
        pode_configurar_avaliacoes=pode_configurar_avaliacoes,
    )


# ---------------------------------------
# MENU PERFORMANCE DE GERENTES
# ---------------------------------------
@performances_bp.route("/gerentes/menu")
@permissao_obrigatoria(
    "PERFORMANCES",
    "PERFORMANCE_GERENTES",
    redirecionar_para="performances.menu_performances",
)
def menu_performance_gerentes():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_vendas = True
        pode_acompanhamento = True
    else:
        pode_vendas = usuario_tem_permissao(
            session["id_usuario"],
            str(session["cod_empresa"]).strip(),
            "PERFORMANCES",
            "GERENTES_VENDAS",
        )
        pode_acompanhamento = usuario_tem_permissao(
            session["id_usuario"],
            str(session["cod_empresa"]).strip(),
            "PERFORMANCES",
            "ACOMPANHAMENTO_METAS",
        )

    pode_consultar_metas, pode_informar_metas = permissoes_metas()

    return render_template(
        "menu_performance_gerentes.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("performances.menu_performances"),
        texto_voltar="← Voltar",
        pode_vendas=pode_vendas,
        pode_acompanhamento=pode_acompanhamento,
        pode_metas=pode_consultar_metas or pode_informar_metas,
    )


# ---------------------------------------
# PERFORMANCE DE GERENTES - VENDAS
# ---------------------------------------
def cod_filiais_gerente(cur, cod_empresa):
    """Filiais que o gerente enxerga: as de `usuarios_filiais`.

    Superusuário vê todas — é o único caso em que a tela mostra mais de um
    posto.
    """
    if str(session.get("tipo_global") or "").strip().lower() == "superusuario":
        cur.execute("""
            SELECT cod_filial, nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND ativo = TRUE
            ORDER BY cod_filial
        """, (cod_empresa,))
        return cur.fetchall() or []

    cur.execute("""
        SELECT f.cod_filial, f.nome_filial
        FROM usuarios_filiais uf
        JOIN filiais f
          ON f.cod_empresa = uf.cod_empresa
         AND f.cod_filial = uf.cod_filial
        WHERE uf.id_usuario = %s
          AND uf.cod_empresa = %s
          AND uf.ativo = TRUE
          AND f.ativo = TRUE
        ORDER BY f.cod_filial
    """, (session.get("id_usuario"), cod_empresa))
    return cur.fetchall() or []


def normalizar_combustivel(descricao):
    """Junta as variações do mesmo produto que vêm da importação.

    Em EMP010 convivem "GASOLINA COMUM FROTA" e "GASOLINA COMUM FROTA." — o
    ponto final é ruído do arquivo, não outro combustível.
    """
    texto = str(descricao or "").strip().upper()
    while texto.endswith("."):
        texto = texto[:-1].strip()
    return texto


def _heatmap_colunas(linhas, chave_valores):
    """Escala de cor por grupo de colunas (postos e combustíveis têm ordens de
    grandeza diferentes; uma escala só apagaria o grupo menor)."""
    valores = [
        float(v)
        for linha in linhas
        for v in linha[chave_valores]
        if v not in (None, 0, 0.0, "")
    ]

    if not valores:
        for linha in linhas:
            linha[chave_valores + "_cores"] = ["" for _ in linha[chave_valores]]
        return

    minimo = min(valores)
    maximo = max(valores)

    for linha in linhas:
        linha[chave_valores + "_cores"] = [
            "" if v in (None, 0, 0.0, "") else cor_excel_51(float(v), minimo, maximo)
            for v in linha[chave_valores]
        ]


@performances_bp.route("/gerentes/vendas")
@permissao_obrigatoria(
    "PERFORMANCES",
    "GERENTES_VENDAS",
    redirecionar_para="performances.menu_performance_gerentes",
)
def gerentes_vendas():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        filiais_disponiveis = cod_filiais_gerente(cur, cod_empresa)

        # A tela é sempre de UM posto — é o que a meta compara. Quem tem mais
        # de um escolhe; sem escolha (ou posto fora da lista de acesso do
        # usuário), abre o primeiro. Não existe "Todos".
        cod_filial_sel = (request.args.get("cod_filial") or "").strip()
        filiais = [
            f for f in filiais_disponiveis
            if str(f["cod_filial"]) == cod_filial_sel
        ] or filiais_disponiveis[:1]
        cod_filial_sel = str(filiais[0]["cod_filial"]) if filiais else ""

        # Setas ← → (quem enxerga mais de um posto): vizinhos na lista de acesso.
        cods_disp = [str(f["cod_filial"]) for f in filiais_disponiveis]
        pos = cods_disp.index(cod_filial_sel) if cod_filial_sel in cods_disp else -1
        filial_anterior = cods_disp[pos - 1] if pos > 0 else None
        filial_proxima = cods_disp[pos + 1] if 0 <= pos < len(cods_disp) - 1 else None

        cods = [int(f["cod_filial"]) for f in filiais]

        registros_qtd = []
        registros_comb = []
        registros_metas = []
        projecao = None

        if cods:
            # Quantidades mensais por posto — a MESMA função de Vendas →
            # Consultar Vendas Sintéticas, que já projeta o mês corrente
            # (acumulado ÷ dias com venda × dias do mês). Antes lia
            # vendas_unidades_sintetico, que depende da importação do painel:
            # sem ela o mês corrente não aparecia, e as colunas de combustível
            # mostravam só o acumulado — a meta seria comparada com meio mês.
            regs_qtd, _, _, projecao = _agregar_diarias_sintetico(cur, cod_empresa)
            registros_qtd = [r for r in regs_qtd if int(r["cod_filial"]) in cods]

            # Quantidades por combustível — vêm das vendas diárias importadas,
            # com o mesmo filtro de produto do sintético (sem isso a soma dos
            # combustíveis não fecharia com o TOTAL).
            cur.execute(f"""
                SELECT
                    EXTRACT(YEAR FROM data)::int  AS ano,
                    EXTRACT(MONTH FROM data)::int AS mes,
                    descricao,
                    SUM(quantidade) AS quantidade
                FROM vendas_diarias
                WHERE cod_empresa = %s
                  AND cod_filial = ANY(%s)
                  AND COALESCE(TRIM(descricao), '') <> ''
                  {FILTRO_SQL_COMBUSTIVEL}
                GROUP BY 1, 2, 3
            """, (cod_empresa, cods))
            registros_comb = cur.fetchall() or []

            # O mês corrente dos combustíveis é projetado pelo mesmo fator.
            if projecao and projecao["projetado"]:
                fator = projecao["dias_mes"] / projecao["dia_base"]
                for r in registros_comb:
                    if (int(r["ano"]), int(r["mes"])) == (projecao["ano"], projecao["mes"]):
                        r["quantidade"] = float(r["quantidade"] or 0) * fator

            cur.execute("""
                SELECT cod_filial, ano, mes, meta_quantidade
                FROM vendas_metas
                WHERE cod_empresa = %s
                  AND cod_filial = ANY(%s)
            """, (cod_empresa, cods))
            registros_metas = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    grade = montar_grade_gerente(filiais, registros_qtd, registros_comb, registros_metas)

    return render_template(
        "gerentes_vendas.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("performances.menu_performance_gerentes"),
        texto_voltar="← Voltar",
        filiais=filiais,
        filiais_disponiveis=filiais_disponiveis,
        cod_filial_sel=cod_filial_sel,
        filial_anterior=filial_anterior,
        filial_proxima=filial_proxima,
        grade=grade,
        projecao=projecao if projecao and projecao["projetado"] else None,
        formatar_numero_br=formatar_numero_br,
    )


@performances_bp.route("/gerentes/vendas/detalhe")
@permissao_obrigatoria(
    "PERFORMANCES",
    "GERENTES_VENDAS",
    redirecionar_para="performances.menu_performance_gerentes",
)
def gerentes_vendas_detalhe():
    """Vendas dia a dia de um posto no mês — todos os combustíveis (clique no
    TOTAL) ou um só (clique na coluna do combustível). Mesma grade, média e
    projeção da janela de Consultar Vendas Sintéticas."""
    cod_empresa = str(session["cod_empresa"]).strip()
    ano = request.args.get("ano", type=int)
    mes = request.args.get("mes", type=int)
    cod_filial = request.args.get("filial", type=int)
    produto = normalizar_combustivel(request.args.get("produto")) or None

    if not (ano and mes and cod_filial):
        return {"erro": "Parâmetros incompletos."}, 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # A trava de posto vale aqui também, não só na tela: o gerente não
        # abre o detalhe de um posto que não é dele trocando o parâmetro.
        permitidas = {
            int(f["cod_filial"]): f["nome_filial"]
            for f in cod_filiais_gerente(cur, cod_empresa)
        }
        if cod_filial not in permitidas:
            return {"erro": "Você não tem acesso a esse posto."}, 403

        return _detalhe_mes_filial(
            cur, cod_empresa, "qtd", ano, mes, cod_filial, permitidas,
            normalizar=normalizar_combustivel, produto=produto,
        )
    finally:
        cur.close()
        conn.close()


def montar_grade_gerente(filiais, registros_qtd, registros_comb, registros_metas=()):
    """Grid mensal: colunas dos postos do gerente, colunas por combustível e
    TOTAL no fim. O TOTAL é a soma dos postos — é a venda do mês; as colunas de
    combustível abrem essa mesma venda por produto (só existem a partir do
    período em que as vendas diárias começaram a ser importadas)."""
    mapa_qtd = {}
    for r in registros_qtd:
        mapa_qtd[(int(r["ano"]), int(r["mes"]), int(r["cod_filial"]))] = float(
            r["quantidade_vendida"] or 0
        )

    mapa_comb = defaultdict(float)
    for r in registros_comb:
        produto = normalizar_combustivel(r["descricao"])
        mapa_comb[(int(r["ano"]), int(r["mes"]), produto)] += float(r["quantidade"] or 0)

    combustiveis = sorted({chave[2] for chave in mapa_comb})

    mapa_meta = {
        (int(r["ano"]), int(r["mes"]), int(r["cod_filial"])): float(r["meta_quantidade"] or 0)
        for r in registros_metas
    }

    # O mês corrente entra mesmo antes da primeira importação, se já tem meta —
    # é a linha que o gerente vai acompanhar. Mês futuro não entra.
    hoje = date.today()
    meses = sorted(
        {(int(r["ano"]), int(r["mes"])) for r in registros_qtd}
        | {(chave[0], chave[1]) for chave in mapa_comb}
        | {(a, m) for (a, m, _) in mapa_meta if (a, m) <= (hoje.year, hoje.month)}
    )[-24:]

    linhas = []
    totais_filial = defaultdict(float)
    totais_comb = defaultdict(float)
    serie_filial = defaultdict(list)
    serie_comb = defaultdict(list)

    for ano, mes in meses:
        valores_filiais = []
        total_mes = 0.0

        for filial in filiais:
            cod_filial = int(filial["cod_filial"])
            valor = mapa_qtd.get((ano, mes, cod_filial))
            valores_filiais.append(valor)

            if valor not in (None, 0, 0.0, ""):
                total_mes += valor
                totais_filial[cod_filial] += valor
                serie_filial[cod_filial].append(valor)

        valores_comb = []
        for produto in combustiveis:
            valor = mapa_comb.get((ano, mes, produto))
            valores_comb.append(valor)

            if valor not in (None, 0, 0.0, ""):
                totais_comb[produto] += valor
                serie_comb[produto].append(valor)

        # Meta do mês: soma das metas dos postos em tela. O percentual compara
        # só a venda dos postos que têm meta — posto sem meta somando na venda
        # inflaria o atingido.
        meta_mes = 0.0
        venda_com_meta = 0.0
        for filial in filiais:
            cod_filial = int(filial["cod_filial"])
            meta = mapa_meta.get((ano, mes, cod_filial))
            if meta:
                meta_mes += meta
                venda_com_meta += mapa_qtd.get((ano, mes, cod_filial)) or 0.0

        # Sem venda ainda (mês recém-aberto) o percentual fica em branco, em
        # vez de um 0% vermelho que não diz nada.
        pct_meta = venda_com_meta / meta_mes * 100 if meta_mes and venda_com_meta else None

        linhas.append({
            "periodo": f"{obter_nome_mes_abrev(mes)}/{str(ano)[-2:]}",
            "ano": ano,
            "mes": mes,
            "valores_filiais": valores_filiais,
            "valores_comb": valores_comb,
            "total": total_mes,
            "meta": meta_mes or None,
            "pct_meta": pct_meta,
            "status_meta": status_meta(pct_meta),
        })

    _heatmap_colunas(linhas, "valores_filiais")
    _heatmap_colunas(linhas, "valores_comb")

    # A coluna TOTAL tem escala própria: é a soma dos postos, uma ordem de
    # grandeza acima das colunas de combustível.
    for linha in linhas:
        linha["totais"] = [linha["total"]]
    _heatmap_colunas(linhas, "totais")
    for linha in linhas:
        linha["total_cor"] = linha.pop("totais_cores")[0]
        linha.pop("totais")

    def media(serie):
        serie = serie[-12:]
        return sum(serie) / len(serie) if serie else 0.0

    linha_total = {
        "rotulo": "TOTAL",
        "valores_filiais": [totais_filial[int(f["cod_filial"])] for f in filiais],
        "valores_comb": [totais_comb[p] for p in combustiveis],
        "total": sum(totais_filial.values()),
    }

    linha_med_12m = {
        "rotulo": "MED 12 M",
        "valores_filiais": [media(serie_filial[int(f["cod_filial"])]) for f in filiais],
        "valores_comb": [media(serie_comb[p]) for p in combustiveis],
    }
    linha_med_12m["total"] = sum(linha_med_12m["valores_filiais"])

    return {
        "combustiveis": combustiveis,
        "linhas": linhas,
        "linha_total": linha_total,
        "linha_med_12m": linha_med_12m,
    }

# ---------------------------------------
# METAS DE VENDAS (por posto e mês, em quantidade)
# ---------------------------------------
NOMES_MESES = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
]

# Faixas do percentual da meta: 120% ou mais é a supermeta; a partir de 100%
# bate a meta; de 90% a 100% está perto; abaixo de 90%, longe.
META_SUPERMETA = 120.0
META_VERDE = 100.0
META_AMARELO = 90.0


def status_meta(pct):
    if pct is None:
        return ""
    if pct >= META_SUPERMETA:
        return "supermeta"
    if pct >= META_VERDE:
        return "verde"
    if pct >= META_AMARELO:
        return "amarelo"
    return "vermelho"


def permissoes_metas():
    """(pode_consultar, pode_informar). O submenu abre com qualquer uma."""
    if str(session.get("tipo_global") or "").strip().lower() == "superusuario":
        return True, True

    id_usuario = session.get("id_usuario")
    cod_empresa = str(session.get("cod_empresa") or "").strip()
    return (
        usuario_tem_permissao(id_usuario, cod_empresa, "PERFORMANCES", "METAS_CONSULTAR"),
        usuario_tem_permissao(id_usuario, cod_empresa, "PERFORMANCES", "METAS_INFORMAR"),
    )


def _filiais_ativas(cur, cod_empresa):
    cur.execute("""
        SELECT cod_filial, nome_filial
        FROM filiais
        WHERE cod_empresa = %s
          AND ativo = TRUE
        ORDER BY cod_filial
    """, (cod_empresa,))
    return cur.fetchall() or []


def _parse_meta(texto):
    """'80.000', '80000', '80.000,50' → float. Vazio → None. Inválido → ValueError."""
    texto = str(texto or "").strip().replace(" ", "")
    if not texto:
        return None
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif texto.count(".") >= 1 and len(texto.rsplit(".", 1)[1]) == 3:
        # "80.000" é milhar, não decimal.
        texto = texto.replace(".", "")
    return float(texto)


@performances_bp.route("/gerentes/metas")
@permissao_obrigatoria(
    "PERFORMANCES",
    "PERFORMANCE_GERENTES",
    redirecionar_para="performances.menu_performances",
)
def menu_metas():
    pode_consultar, pode_informar = permissoes_metas()
    if not (pode_consultar or pode_informar):
        flash("Você não tem acesso às metas de vendas.", "erro")
        return redirect(url_for("performances.menu_performance_gerentes"))

    return render_template(
        "menu_metas_vendas.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("performances.menu_performance_gerentes"),
        texto_voltar="← Voltar",
        pode_consultar=pode_consultar,
        pode_informar=pode_informar,
    )


@performances_bp.route("/gerentes/metas/consultar")
@permissao_obrigatoria(
    "PERFORMANCES",
    "METAS_CONSULTAR",
    redirecionar_para="performances.menu_metas",
)
def consultar_metas():
    cod_empresa = str(session["cod_empresa"]).strip()
    hoje = date.today()

    try:
        ano = int(request.args.get("ano") or 0)
    except ValueError:
        ano = 0

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        filiais = _filiais_ativas(cur, cod_empresa)

        cur.execute("""
            SELECT DISTINCT ano
            FROM vendas_metas
            WHERE cod_empresa = %s
            ORDER BY ano
        """, (cod_empresa,))
        anos = [int(r["ano"]) for r in cur.fetchall()]

        if not ano:
            ano = hoje.year if (hoje.year in anos or not anos) else anos[-1]
        if ano not in anos:
            anos = sorted(set(anos) | {ano})

        cur.execute("""
            SELECT cod_filial, mes, meta_quantidade
            FROM vendas_metas
            WHERE cod_empresa = %s
              AND ano = %s
        """, (cod_empresa, ano))
        registros = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    mapa = {(int(r["mes"]), int(r["cod_filial"])): float(r["meta_quantidade"]) for r in registros}

    linhas = []
    for mes in range(1, 13):
        valores = [mapa.get((mes, int(f["cod_filial"]))) for f in filiais]
        linhas.append({
            "mes": mes,
            "periodo": f"{obter_nome_mes_abrev(mes)}/{str(ano)[-2:]}",
            "valores": valores,
            "total": sum(v for v in valores if v) or None,
        })

    # Mapa de calor coluna por coluna: cada posto se compara com ele mesmo ao
    # longo dos meses — uma escala só deixaria os postos pequenos todos vermelhos.
    for i in range(len(filiais)):
        coluna = [linha["valores"][i] for linha in linhas if linha["valores"][i]]
        minimo, maximo = (min(coluna), max(coluna)) if coluna else (0, 0)
        for linha in linhas:
            v = linha["valores"][i]
            linha.setdefault("cores", []).append(
                cor_excel_51(v, minimo, maximo) if v else ""
            )

    totais_mes = [linha["total"] for linha in linhas if linha["total"]]
    for linha in linhas:
        linha["total_cor"] = (
            cor_excel_51(linha["total"], min(totais_mes), max(totais_mes))
            if linha["total"] else ""
        )

    linha_total = {
        "valores": [
            sum(linha["valores"][i] or 0 for linha in linhas) or None
            for i in range(len(filiais))
        ],
        "total": sum(totais_mes) or None,
    }

    _, pode_informar = permissoes_metas()

    return render_template(
        "consultar_metas_vendas.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("performances.menu_metas"),
        texto_voltar="← Voltar",
        filiais=filiais,
        anos=anos,
        ano=ano,
        linhas=linhas,
        linha_total=linha_total,
        pode_informar=pode_informar,
        formatar_numero_br=formatar_numero_br,
    )


@performances_bp.route("/gerentes/metas/informar", methods=["GET", "POST"])
@permissao_obrigatoria(
    "PERFORMANCES",
    "METAS_INFORMAR",
    redirecionar_para="performances.menu_metas",
)
def informar_metas():
    cod_empresa = str(session["cod_empresa"]).strip()
    hoje = date.today()
    fonte = request.form if request.method == "POST" else request.args

    try:
        ano = int(fonte.get("ano") or hoje.year)
        mes = int(fonte.get("mes") or hoje.month)
        if not 1 <= mes <= 12:
            raise ValueError
    except ValueError:
        ano, mes = hoje.year, hoje.month

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        filiais = _filiais_ativas(cur, cod_empresa)

        if request.method == "POST" and not (
            request.form.get("ano") and request.form.get("mes")
        ):
            # Sem mês/ano no corpo, o padrão seria o mês corrente — e um POST
            # vazio apagaria as metas dele inteiro.
            flash("Mês/ano não informados — nada foi gravado.", "erro")
        elif request.method == "POST":
            # Valida tudo antes de gravar: um valor ruim no meio não pode deixar
            # o mês gravado pela metade.
            metas = {}
            erros = []
            for f in filiais:
                cod_filial = int(f["cod_filial"])
                campo = f"meta_{cod_filial}"
                # Só mexe no posto cujo campo veio no formulário: posto ausente
                # (filial ativada depois que a tela abriu, envio truncado) fica
                # como está, em vez de ter a meta apagada.
                if campo not in request.form:
                    continue
                bruto = request.form.get(campo)
                try:
                    valor = _parse_meta(bruto)
                    if valor is not None and valor < 0:
                        raise ValueError
                except ValueError:
                    erros.append(f"{cod_filial} — {f['nome_filial']}: “{bruto}”")
                    continue
                metas[cod_filial] = valor

            if erros:
                flash("Valores inválidos, nada foi gravado: " + "; ".join(erros), "erro")
            else:
                gravadas = removidas = 0
                for cod_filial, valor in metas.items():
                    if valor:
                        cur.execute("""
                            INSERT INTO vendas_metas
                                (cod_empresa, cod_filial, ano, mes, meta_quantidade)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (cod_empresa, cod_filial, ano, mes)
                            DO UPDATE SET meta_quantidade = EXCLUDED.meta_quantidade,
                                          atualizado_em = now()
                        """, (cod_empresa, cod_filial, ano, mes, valor))
                        gravadas += 1
                    else:
                        # Campo vazio (ou zero) = posto sem meta no mês.
                        cur.execute("""
                            DELETE FROM vendas_metas
                            WHERE cod_empresa = %s AND cod_filial = %s
                              AND ano = %s AND mes = %s
                        """, (cod_empresa, cod_filial, ano, mes))
                        removidas += cur.rowcount
                conn.commit()

                msg = f"Metas de {NOMES_MESES[mes - 1]}/{ano} gravadas: {gravadas} posto(s)."
                if removidas:
                    msg += f" {removidas} meta(s) removida(s)."
                flash(msg, "success")
                return redirect(url_for("performances.informar_metas", ano=ano, mes=mes))

        cur.execute("""
            SELECT cod_filial, meta_quantidade
            FROM vendas_metas
            WHERE cod_empresa = %s AND ano = %s AND mes = %s
        """, (cod_empresa, ano, mes))
        metas_mes = {int(r["cod_filial"]): float(r["meta_quantidade"]) for r in cur.fetchall()}

        # Referências para quem digita: a meta do mês anterior e a venda do
        # mesmo mês no ano passado.
        ano_ant, mes_ant = (ano, mes - 1) if mes > 1 else (ano - 1, 12)
        cur.execute("""
            SELECT cod_filial, meta_quantidade
            FROM vendas_metas
            WHERE cod_empresa = %s AND ano = %s AND mes = %s
        """, (cod_empresa, ano_ant, mes_ant))
        metas_ant = {int(r["cod_filial"]): float(r["meta_quantidade"]) for r in cur.fetchall()}

        cur.execute("""
            SELECT cod_filial, quantidade_vendida
            FROM vendas_unidades_sintetico
            WHERE cod_empresa = %s AND ano = %s AND mes = %s
        """, (cod_empresa, ano - 1, mes))
        vendas_ano_ant = {int(r["cod_filial"]): float(r["quantidade_vendida"] or 0) for r in cur.fetchall()}
    finally:
        cur.close()
        conn.close()

    # No POST com erro, devolve o que foi digitado em vez do que está no banco.
    digitado = request.form if request.method == "POST" else {}

    linhas = []
    for f in filiais:
        cod_filial = int(f["cod_filial"])
        meta = metas_mes.get(cod_filial)
        linhas.append({
            "cod_filial": cod_filial,
            "nome_filial": f["nome_filial"],
            "valor": digitado.get(f"meta_{cod_filial}") if digitado else (
                formatar_numero_br(meta, 0) if meta else ""
            ),
            "meta_anterior": metas_ant.get(cod_filial),
            "venda_ano_anterior": vendas_ano_ant.get(cod_filial),
        })

    return render_template(
        "informar_metas_vendas.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("performances.menu_metas"),
        texto_voltar="← Voltar",
        ano=ano,
        mes=mes,
        anos=list(range(hoje.year - 2, hoje.year + 3)),
        nomes_meses=NOMES_MESES,
        rotulo_mes_anterior=f"{obter_nome_mes_abrev(mes_ant)}/{str(ano_ant)[-2:]}",
        rotulo_ano_anterior=f"{obter_nome_mes_abrev(mes)}/{str(ano - 1)[-2:]}",
        linhas=linhas,
        ja_informado=bool(metas_mes),
        formatar_numero_br=formatar_numero_br,
    )


# ---------------------------------------
# ACOMPANHAMENTO DE METAS (ranking dos postos no mês)
# ---------------------------------------
@performances_bp.route("/gerentes/acompanhamento-metas")
@permissao_obrigatoria(
    "PERFORMANCES",
    "ACOMPANHAMENTO_METAS",
    redirecionar_para="performances.menu_performance_gerentes",
)
def acompanhamento_metas():
    """Um mês, uma linha por posto: vendido × meta × % atingido, do maior
    percentual para o menor. A venda é a MESMA da tela de Vendas do gerente
    (`_agregar_diarias_sintetico`), com o mês corrente projetado — senão o
    ranking daqui e o % de lá divergiriam."""
    cod_empresa = str(session["cod_empresa"]).strip()
    hoje = date.today()

    try:
        ano = int(request.args.get("ano") or hoje.year)
        mes = int(request.args.get("mes") or hoje.month)
        if not 1 <= mes <= 12:
            raise ValueError
    except ValueError:
        ano, mes = hoje.year, hoje.month

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        filiais = cod_filiais_gerente(cur, cod_empresa)
        cods = [int(f["cod_filial"]) for f in filiais]

        regs_qtd, _, _, projecao = _agregar_diarias_sintetico(cur, cod_empresa)

        cur.execute("""
            SELECT cod_filial, meta_quantidade
            FROM vendas_metas
            WHERE cod_empresa = %s
              AND ano = %s
              AND mes = %s
        """, (cod_empresa, ano, mes))
        mapa_meta = {int(r["cod_filial"]): float(r["meta_quantidade"]) for r in cur.fetchall()}

        cur.execute("""
            SELECT DISTINCT ano FROM vendas_metas WHERE cod_empresa = %s
        """, (cod_empresa,))
        anos = sorted({int(r["ano"]) for r in cur.fetchall()} | {hoje.year, ano})
    finally:
        cur.close()
        conn.close()

    # A venda que vem da função já é projetada no último mês com venda; o
    # realizado é a mesma conta desfeita pelo fator.
    projetado = bool(
        projecao and projecao["projetado"]
        and (projecao["ano"], projecao["mes"]) == (ano, mes)
    )
    fator = projecao["dias_mes"] / projecao["dia_base"] if projetado else 1.0

    mapa_venda = {
        int(r["cod_filial"]): float(r["quantidade_vendida"] or 0)
        for r in regs_qtd
        if (int(r["ano"]), int(r["mes"])) == (ano, mes)
    }

    linhas = []
    for filial in filiais:
        cod = int(filial["cod_filial"])
        venda = mapa_venda.get(cod)
        meta = mapa_meta.get(cod)
        if not venda and not meta:
            continue
        pct = venda / meta * 100 if meta and venda else None
        linhas.append({
            "cod_filial": cod,
            "nome_filial": filial["nome_filial"],
            "realizado": venda / fator if venda else None,
            "projecao": venda if projetado else None,
            "meta": meta,
            "pct": pct,
            "falta": meta - venda if meta and (venda or 0) < meta else None,
            "status_meta": status_meta(pct),
        })

    # Maior percentual primeiro; posto sem meta (ou sem venda) vai para o fim.
    linhas.sort(key=lambda l: (l["pct"] is None, -(l["pct"] or 0), l["cod_filial"]))

    # Mapa de calor de 51 faixas no % atingido — os postos entre si.
    pcts = [l["pct"] for l in linhas if l["pct"] is not None]
    for posicao, linha in enumerate(linhas, start=1):
        linha["posicao"] = posicao if linha["pct"] is not None else None
        linha["pct_cor"] = (
            cor_excel_51(linha["pct"], min(pcts), max(pcts))
            if linha["pct"] is not None else ""
        )

    # Total: só os postos que têm meta, como na tela do gerente — posto sem
    # meta somando na venda inflaria o atingido.
    com_meta = [l for l in linhas if l["meta"]]
    venda_total = sum((l["projecao"] if projetado else l["realizado"]) or 0 for l in com_meta)
    meta_total = sum(l["meta"] for l in com_meta)
    pct_total = venda_total / meta_total * 100 if meta_total and venda_total else None
    total = {
        "realizado": sum(l["realizado"] or 0 for l in com_meta) or None,
        "projecao": venda_total if projetado else None,
        "meta": meta_total or None,
        "pct": pct_total,
        "falta": meta_total - venda_total if meta_total and venda_total < meta_total else None,
        "status_meta": status_meta(pct_total),
    }

    return render_template(
        "acompanhamento_metas.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("performances.menu_performance_gerentes"),
        texto_voltar="← Voltar",
        ano=ano,
        mes=mes,
        anos=anos,
        nomes_meses=NOMES_MESES[:12],
        linhas=linhas,
        total=total,
        projetado=projetado,
        projecao=projecao if projetado else None,
        sem_meta=not mapa_meta,
        formatar_numero_br=formatar_numero_br,
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
# CRIAR AVALIAÇÕES (por filial e mês) + VÍNCULO DE FUNCIONÁRIOS
#--------------------------------------------------------------
# A avaliação é criada por filial e mês (performances_avaliacoes_filiais) com
# um checklist de Configurar Avaliações. Os funcionários são VINCULADOS a ela, e
# o vínculo é a própria execução (performances_execucoes, uma por funcionário),
# que é o que o chefe de pista preenche em Executar. Quem cria enxerga a
# empresa inteira; Executar e Consultar só as filiais do usuário
# (usuarios_filiais), como em Vistorias.

def _ano_mes_sel(origem=None):
    origem = origem if origem is not None else request.args
    hoje = date.today()
    try:
        ano = int(origem.get("ano") or hoje.year)
    except ValueError:
        ano = hoje.year
    try:
        mes = int(origem.get("mes") or hoje.month)
    except ValueError:
        mes = hoje.month
    if not 1 <= mes <= 12:
        mes = hoje.month
    return ano, mes


def _nome_executor():
    return (
        session.get("nome_usuario")
        or session.get("usuario")
        or f"Usuário {session.get('id_usuario')}"
    )


# Execução "respondida" = algum item com nota. É o que trava excluir/desvincular
# sem confirmação: apagar ali é perder o que o chefe de pista já preencheu.
SQL_RESPONDIDA = """
    EXISTS (
        SELECT 1
        FROM performances_execucao_itens i
        WHERE i.id_execucao = e.id_execucao
          AND COALESCE(i.nota_item, 0) > 0
    )
"""


def _vincular_funcionario(cur, af, id_funcionario, nome):
    """Cria a execução do funcionário na avaliação da filial, já com os itens
    do checklist. O executor fica em branco: é gravado por quem preencher."""
    cur.execute("""
        INSERT INTO performances_execucoes (
            cod_empresa, id_avaliacao, id_avaliacao_filial, cod_filial,
            data_avaliacao, id_funcionario, nome_avaliado, status,
            criado_em, atualizado_em
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'ABERTA', NOW(), NOW())
        RETURNING id_execucao
    """, (
        af["cod_empresa"], af["id_avaliacao"], af["id_avaliacao_filial"],
        af["cod_filial"], date(af["ano"], af["mes"], 1), id_funcionario, nome,
    ))
    id_execucao = cur.fetchone()["id_execucao"]

    cur.execute("""
        INSERT INTO performances_execucao_itens (
            id_execucao, id_item, sequencia, titulo, detalhamento,
            nota_item, criado_em, atualizado_em
        )
        SELECT %s, id_item, sequencia, titulo, detalhamento, 0, NOW(), NOW()
        FROM performances_avaliacao_itens
        WHERE id_avaliacao = %s
          AND ativo = TRUE
        ORDER BY sequencia
    """, (id_execucao, af["id_avaliacao"]))
    return id_execucao


def _apagar_execucoes(cur, ids_execucao):
    if not ids_execucao:
        return
    cur.execute(
        "DELETE FROM performances_execucao_itens WHERE id_execucao = ANY(%s)",
        (list(ids_execucao),),
    )
    cur.execute(
        "DELETE FROM performances_execucoes WHERE id_execucao = ANY(%s)",
        (list(ids_execucao),),
    )


def _avaliacao_filial(cur, cod_empresa, id_avaliacao_filial, travar=False):
    cur.execute(f"""
        SELECT af.*, f.nome_filial, a.codigo_avaliacao, a.descricao AS avaliacao_descricao,
               a.versao, c.descricao AS cargo_descricao
        FROM performances_avaliacoes_filiais af
        LEFT JOIN filiais f
          ON f.cod_empresa = af.cod_empresa
         AND f.cod_filial = af.cod_filial
        LEFT JOIN performances_avaliacoes a ON a.id_avaliacao = af.id_avaliacao
        LEFT JOIN cargos c ON c.id = af.id_cargo
        WHERE af.id_avaliacao_filial = %s
          AND af.cod_empresa = %s
        {"FOR UPDATE OF af" if travar else ""}
    """, (id_avaliacao_filial, cod_empresa))
    return cur.fetchone()


@performances_bp.route("/avaliacoes/criar", methods=["GET", "POST"])
@permissao_obrigatoria(
    "PERFORMANCES",
    "CRIAR_AVALIACOES",
    redirecionar_para="performances.menu_avaliacoes_funcionarios",
)
def criar_avaliacoes():
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            ano, mes = _ano_mes_sel(request.form)
            id_avaliacao = int(request.form.get("id_avaliacao") or 0)
            id_cargo = int(request.form.get("id_cargo") or 0) or None
            cod_filiais = sorted({
                int(x) for x in request.form.getlist("cod_filial") if str(x).isdigit()
            })

            if not cod_filiais or not id_avaliacao:
                flash("Escolha as filiais e o checklist.", "error")
                return redirect(url_for("performances.criar_avaliacoes", ano=ano, mes=mes))

            cur.execute("""
                SELECT 1 FROM performances_avaliacoes
                WHERE id_avaliacao = %s AND cod_empresa = %s AND status = 'ATIVO'
            """, (id_avaliacao, cod_empresa))
            if not cur.fetchone():
                flash("Checklist não encontrado.", "error")
                return redirect(url_for("performances.criar_avaliacoes", ano=ano, mes=mes))

            cur.execute("""
                SELECT cod_filial, nome_filial FROM filiais
                WHERE cod_empresa = %s AND ativo = TRUE AND cod_filial = ANY(%s)
                ORDER BY cod_filial
            """, (cod_empresa, cod_filiais))
            filiais_validas = cur.fetchall() or []

            criadas, ja_existiam, vinculados = 0, [], 0

            for fil in filiais_validas:
                cur.execute("""
                    INSERT INTO performances_avaliacoes_filiais
                        (cod_empresa, cod_filial, ano, mes, id_avaliacao, id_cargo, criado_por)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (cod_empresa, cod_filial, ano, mes, id_avaliacao) DO NOTHING
                    RETURNING *
                """, (cod_empresa, fil["cod_filial"], ano, mes, id_avaliacao,
                      id_cargo, session.get("id_usuario")))
                af = cur.fetchone()

                if not af:
                    ja_existiam.append(f"{fil['cod_filial']} - {fil['nome_filial']}")
                    continue

                criadas += 1

                if not id_cargo:
                    continue

                # Funcionários ativos do cargo na filial que ainda não têm este
                # checklist no mês (inclusive em execução antiga, sem cabeçalho).
                cur.execute("""
                    SELECT f.id, f.nome
                    FROM funcionarios f
                    WHERE f.cod_empresa = %s
                      AND f.cod_filial = %s
                      AND f.id_cargo = %s
                      AND f.ativo = TRUE
                      AND NOT EXISTS (
                          SELECT 1 FROM performances_execucoes e
                          WHERE e.cod_empresa = f.cod_empresa
                            AND e.id_funcionario = f.id
                            AND e.id_avaliacao = %s
                            AND e.data_avaliacao >= %s
                            AND e.data_avaliacao < %s
                      )
                    ORDER BY f.nome
                """, (
                    cod_empresa, fil["cod_filial"], id_cargo, id_avaliacao,
                    date(ano, mes, 1),
                    date(ano + 1, 1, 1) if mes == 12 else date(ano, mes + 1, 1),
                ))
                for func in cur.fetchall() or []:
                    _vincular_funcionario(cur, af, func["id"], func["nome"])
                    vinculados += 1

            conn.commit()

            if criadas:
                flash(
                    f"{criadas} avaliação(ões) criada(s), {vinculados} funcionário(s) vinculado(s).",
                    "success",
                )
            if ja_existiam:
                flash(
                    "Já existia avaliação com esse checklist no mês em: "
                    + ", ".join(ja_existiam) + ". Use Vincular para mexer nelas.",
                    "error",
                )
            return redirect(url_for("performances.criar_avaliacoes", ano=ano, mes=mes))

        ano, mes = _ano_mes_sel()

        cur.execute("""
            SELECT cod_filial, nome_filial FROM filiais
            WHERE cod_empresa = %s AND ativo = TRUE
            ORDER BY cod_filial
        """, (cod_empresa,))
        filiais = cur.fetchall() or []

        cur.execute("""
            SELECT id_avaliacao, codigo_avaliacao, descricao, versao
            FROM performances_avaliacoes
            WHERE cod_empresa = %s AND status = 'ATIVO'
            ORDER BY codigo_avaliacao, versao DESC
        """, (cod_empresa,))
        checklists = cur.fetchall() or []

        cur.execute("""
            SELECT id, codigo, descricao FROM cargos
            WHERE cod_empresa = %s AND ativo = TRUE
            ORDER BY descricao
        """, (cod_empresa,))
        cargos = cur.fetchall() or []

        cur.execute(f"""
            SELECT
                af.id_avaliacao_filial,
                af.cod_filial,
                f.nome_filial,
                a.codigo_avaliacao,
                a.descricao AS avaliacao_descricao,
                a.versao,
                c.descricao AS cargo_descricao,
                COUNT(e.id_execucao) AS vinculados,
                COUNT(e.id_execucao) FILTER (WHERE {SQL_RESPONDIDA}) AS respondidas,
                AVG(e.nota) FILTER (WHERE {SQL_RESPONDIDA}) AS media
            FROM performances_avaliacoes_filiais af
            LEFT JOIN filiais f
              ON f.cod_empresa = af.cod_empresa
             AND f.cod_filial = af.cod_filial
            LEFT JOIN performances_avaliacoes a ON a.id_avaliacao = af.id_avaliacao
            LEFT JOIN cargos c ON c.id = af.id_cargo
            LEFT JOIN performances_execucoes e ON e.id_avaliacao_filial = af.id_avaliacao_filial
            WHERE af.cod_empresa = %s
              AND af.ano = %s
              AND af.mes = %s
            GROUP BY af.id_avaliacao_filial, af.cod_filial, f.nome_filial,
                     a.codigo_avaliacao, a.descricao, a.versao, c.descricao
            ORDER BY af.cod_filial, a.codigo_avaliacao
        """, (cod_empresa, ano, mes))
        avaliacoes_mes = cur.fetchall() or []

        # Cargo sugerido na linha nova: Frentista, quando existir.
        id_cargo_sugerido = next(
            (c["id"] for c in cargos if "FRENTISTA" in (c["descricao"] or "").upper()),
            None,
        )

    finally:
        cur.close()
        conn.close()

    return render_template(
        "criar_avaliacoes.html",
        nome_empresa=session.get("nome_empresa"),
        ano_sel=ano,
        mes_sel=mes,
        filiais=filiais,
        checklists=checklists,
        cargos=cargos,
        id_cargo_sugerido=id_cargo_sugerido,
        avaliacoes_mes=avaliacoes_mes,
        url_voltar=url_for("performances.menu_avaliacoes_funcionarios"),
        texto_voltar="← Voltar",
    )


@performances_bp.route("/avaliacoes/criar/<int:id_avaliacao_filial>/excluir", methods=["POST"])
@permissao_obrigatoria(
    "PERFORMANCES",
    "CRIAR_AVALIACOES",
    redirecionar_para="performances.menu_avaliacoes_funcionarios",
)
def excluir_avaliacao_filial(id_avaliacao_filial):
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        af = _avaliacao_filial(cur, cod_empresa, id_avaliacao_filial, travar=True)
        if not af:
            flash("Avaliação não encontrada.", "error")
            return redirect(url_for("performances.criar_avaliacoes"))

        cur.execute(f"""
            SELECT e.id_execucao, {SQL_RESPONDIDA} AS respondida
            FROM performances_execucoes e
            WHERE e.id_avaliacao_filial = %s
        """, (id_avaliacao_filial,))
        execucoes = cur.fetchall() or []

        if any(e["respondida"] for e in execucoes):
            flash(
                "Esta avaliação já tem funcionários avaliados e não pode ser excluída. "
                "Use Vincular para retirar só quem não deve ser avaliado.",
                "error",
            )
            return redirect(url_for("performances.criar_avaliacoes", ano=af["ano"], mes=af["mes"]))

        _apagar_execucoes(cur, [e["id_execucao"] for e in execucoes])
        cur.execute(
            "DELETE FROM performances_avaliacoes_filiais WHERE id_avaliacao_filial = %s",
            (id_avaliacao_filial,),
        )
        conn.commit()
        flash(f"Avaliação da filial {af['cod_filial']} - {af['nome_filial']} excluída.", "success")
        return redirect(url_for("performances.criar_avaliacoes", ano=af["ano"], mes=af["mes"]))

    except Exception as e:
        conn.rollback()
        flash(f"Erro ao excluir avaliação: {e}", "error")
        return redirect(url_for("performances.criar_avaliacoes"))

    finally:
        cur.close()
        conn.close()


@performances_bp.route("/avaliacoes/criar/<int:id_avaliacao_filial>/vincular")
@permissao_obrigatoria(
    "PERFORMANCES",
    "CRIAR_AVALIACOES",
    redirecionar_para="performances.menu_avaliacoes_funcionarios",
)
def vincular_funcionarios_avaliacao(id_avaliacao_filial):
    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        af = _avaliacao_filial(cur, cod_empresa, id_avaliacao_filial)
        if not af:
            flash("Avaliação não encontrada.", "error")
            return redirect(url_for("performances.criar_avaliacoes"))

        # Os ativos da filial, mais quem já está vinculado e saiu dela (ou foi
        # desligado) — senão o vínculo ficaria invisível e sem como desfazer.
        cur.execute(f"""
            WITH vinc AS (
                SELECT
                    e.id_funcionario,
                    COUNT(*) AS qtd,
                    BOOL_OR({SQL_RESPONDIDA}) AS respondida,
                    MAX(e.nota) AS nota
                FROM performances_execucoes e
                WHERE e.id_avaliacao_filial = %s
                GROUP BY e.id_funcionario
            )
            SELECT
                f.id,
                f.nome,
                f.matricula,
                f.ativo,
                f.cod_filial,
                c.descricao AS cargo,
                (v.id_funcionario IS NOT NULL) AS vinculado,
                COALESCE(v.respondida, FALSE) AS respondida,
                v.nota
            FROM funcionarios f
            LEFT JOIN cargos c ON c.id = f.id_cargo
            LEFT JOIN vinc v ON v.id_funcionario = f.id
            WHERE f.cod_empresa = %s
              AND (
                    (f.cod_filial = %s AND f.ativo = TRUE)
                    OR v.id_funcionario IS NOT NULL
              )
            ORDER BY c.descricao NULLS LAST, f.nome
        """, (id_avaliacao_filial, cod_empresa, af["cod_filial"]))
        funcionarios = cur.fetchall() or []

    finally:
        cur.close()
        conn.close()

    return render_template(
        "vincular_funcionarios_avaliacao.html",
        nome_empresa=session.get("nome_empresa"),
        af=af,
        funcionarios=funcionarios,
        url_voltar=url_for("performances.criar_avaliacoes", ano=af["ano"], mes=af["mes"]),
        texto_voltar="← Voltar",
    )


@performances_bp.route("/avaliacoes/criar/<int:id_avaliacao_filial>/vinculo", methods=["POST"])
@permissao_obrigatoria(
    "PERFORMANCES",
    "CRIAR_AVALIACOES",
    redirecionar_para="performances.menu_avaliacoes_funcionarios",
)
def alternar_vinculo_avaliacao(id_avaliacao_filial):
    cod_empresa = str(session["cod_empresa"]).strip()
    dados = request.get_json(silent=True) or {}

    try:
        id_funcionario = int(dados.get("id_funcionario") or 0)
    except (TypeError, ValueError):
        id_funcionario = 0
    vincular = bool(dados.get("vincular"))
    confirmar = bool(dados.get("confirmar"))

    if not id_funcionario:
        return jsonify({"ok": False, "erro": "Funcionário não informado."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # A trava no cabeçalho serializa dois cliques seguidos no mesmo
        # checkbox — sem ela os dois veriam "não vinculado" e criariam dois.
        af = _avaliacao_filial(cur, cod_empresa, id_avaliacao_filial, travar=True)
        if not af:
            return jsonify({"ok": False, "erro": "Avaliação não encontrada."}), 404

        cur.execute(f"""
            SELECT e.id_execucao, {SQL_RESPONDIDA} AS respondida
            FROM performances_execucoes e
            WHERE e.id_avaliacao_filial = %s
              AND e.id_funcionario = %s
        """, (id_avaliacao_filial, id_funcionario))
        atuais = cur.fetchall() or []

        if vincular:
            if atuais:
                return jsonify({"ok": True, "vinculado": True})

            cur.execute("""
                SELECT id, nome FROM funcionarios
                WHERE id = %s AND cod_empresa = %s AND cod_filial = %s AND ativo = TRUE
            """, (id_funcionario, cod_empresa, af["cod_filial"]))
            func = cur.fetchone()
            if not func:
                conn.rollback()
                return jsonify({"ok": False, "erro": "Funcionário não é ativo desta filial."}), 400

            _vincular_funcionario(cur, af, func["id"], func["nome"])
            conn.commit()
            return jsonify({"ok": True, "vinculado": True})

        if not atuais:
            return jsonify({"ok": True, "vinculado": False})

        if any(e["respondida"] for e in atuais) and not confirmar:
            conn.rollback()
            return jsonify({
                "ok": False,
                "precisa_confirmar": True,
                "erro": "Este funcionário já foi avaliado. Desvincular apaga as notas lançadas.",
            }), 409

        _apagar_execucoes(cur, [e["id_execucao"] for e in atuais])
        conn.commit()
        return jsonify({"ok": True, "vinculado": False})

    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 500

    finally:
        cur.close()
        conn.close()


#--------------------------------------------------------------
# EXECUTAR / CONSULTAR AVALIAÇÕES (filiais do usuário)
#--------------------------------------------------------------
def _execucoes_do_mes(cur, cod_empresa, ano, mes, cod_filiais):
    if not cod_filiais:
        return []
    cur.execute(f"""
        SELECT
            e.id_execucao,
            e.cod_filial,
            f.nome_filial,
            e.nome_avaliado,
            e.nome_executor,
            e.status,
            e.nota,
            cg.descricao AS cargo,
            a.codigo_avaliacao,
            a.descricao AS avaliacao_descricao,
            {SQL_RESPONDIDA} AS respondida
        FROM performances_execucoes e
        LEFT JOIN filiais f
          ON f.cod_empresa = e.cod_empresa
         AND f.cod_filial = e.cod_filial
        LEFT JOIN performances_avaliacoes a ON a.id_avaliacao = e.id_avaliacao
        LEFT JOIN funcionarios fu ON fu.id = e.id_funcionario
        LEFT JOIN cargos cg ON cg.id = fu.id_cargo
        WHERE e.cod_empresa = %s
          AND e.data_avaliacao >= %s
          AND e.data_avaliacao < %s
          AND e.cod_filial = ANY(%s)
        ORDER BY e.cod_filial, e.nome_avaliado, e.id_execucao
    """, (
        cod_empresa,
        date(ano, mes, 1),
        date(ano + 1, 1, 1) if mes == 12 else date(ano, mes + 1, 1),
        sorted(cod_filiais),
    ))
    return cur.fetchall() or []


def _agrupar_por_filial(execucoes):
    """Um bloco por filial, com a média só de quem já foi avaliado."""
    blocos = {}
    for e in execucoes:
        b = blocos.setdefault(e["cod_filial"], {
            "cod_filial": e["cod_filial"],
            "nome_filial": e["nome_filial"],
            "itens": [],
        })
        b["itens"].append(e)
    for b in blocos.values():
        notas = [float(e["nota"] or 0) for e in b["itens"] if e["respondida"]]
        b["total"] = len(b["itens"])
        b["respondidas"] = len(notas)
        b["media"] = sum(notas) / len(notas) if notas else None
    return list(blocos.values())


def _tela_execucoes(template, ordenar_por_nota):
    cod_empresa = str(session["cod_empresa"]).strip()
    ano, mes = _ano_mes_sel()
    filial_sel = (request.args.get("filial") or "").strip()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        permitidas = cod_filiais_avaliacoes_usuario(cur, cod_empresa)

        cur.execute("""
            SELECT cod_filial, nome_filial FROM filiais
            WHERE cod_empresa = %s AND ativo = TRUE AND cod_filial = ANY(%s)
            ORDER BY cod_filial
        """, (cod_empresa, sorted(permitidas)))
        filiais = cur.fetchall() or []

        alvo = permitidas
        if filial_sel.isdigit() and int(filial_sel) in permitidas:
            alvo = {int(filial_sel)}
        else:
            filial_sel = ""

        blocos = _agrupar_por_filial(_execucoes_do_mes(cur, cod_empresa, ano, mes, alvo))

        if ordenar_por_nota:
            for b in blocos:
                b["itens"].sort(key=lambda e: (
                    not e["respondida"], -float(e["nota"] or 0), e["nome_avaliado"] or ""
                ))

    finally:
        cur.close()
        conn.close()

    return render_template(
        template,
        nome_empresa=session.get("nome_empresa"),
        ano_sel=ano,
        mes_sel=mes,
        filial_sel=filial_sel,
        filiais=filiais,
        sem_filial=not permitidas,
        blocos=blocos,
        url_voltar=url_for("performances.menu_avaliacoes_funcionarios"),
        texto_voltar="← Voltar",
    )


@performances_bp.route("/avaliacoes/executar")
@permissao_obrigatoria(
    "PERFORMANCES",
    "EXECUTAR_AVALIACOES",
    redirecionar_para="performances.menu_avaliacoes_funcionarios",
)
def executar_avaliacoes():
    return _tela_execucoes("executar_avaliacoes.html", ordenar_por_nota=False)

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
    redirecionar_para="performances.menu_avaliacoes_funcionarios",
)
def consultar_avaliacoes():
    return _tela_execucoes("consultar_avaliacoes.html", ordenar_por_nota=True)

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
        # A trava de filial vale no servidor, não só na lista de Executar.
        cur.execute("""
            SELECT cod_filial, data_avaliacao FROM performances_execucoes
            WHERE id_execucao = %s AND cod_empresa = %s
        """, (id_execucao, cod_empresa))
        alvo = cur.fetchone()
        if not alvo:
            flash("Avaliação não encontrada.", "error")
            return redirect(url_for("performances.executar_avaliacoes"))
        if int(alvo["cod_filial"]) not in cod_filiais_avaliacoes_usuario(cur, cod_empresa):
            flash("Você não tem acesso às avaliações desta filial.", "error")
            return redirect(url_for("performances.executar_avaliacoes"))

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
                    id_usuario_executor = COALESCE(id_usuario_executor, %s),
                    nome_executor = COALESCE(nome_executor, %s),
                    atualizado_em = NOW()
                WHERE id_execucao = %s
                  AND cod_empresa = %s
            """, (
                total_possivel,
                total_obtido,
                nota_final,
                session.get("id_usuario"),
                _nome_executor(),
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
        url_voltar=url_for(
            "performances.executar_avaliacoes",
            ano=execucao["data_avaliacao"].year,
            mes=execucao["data_avaliacao"].month,
            filial=execucao["cod_filial"],
        ),
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
            SELECT i.id_execucao, e.cod_filial
            FROM performances_execucao_itens i
            JOIN performances_execucoes e
              ON e.id_execucao = i.id_execucao
            WHERE i.id_execucao_item = %s
              AND e.cod_empresa = %s
        """, (id_execucao_item, cod_empresa))

        row = cur.fetchone()

        if not row:
            return jsonify({"ok": False, "erro": "Item não encontrado"}), 404

        if int(row["cod_filial"]) not in cod_filiais_avaliacoes_usuario(cur, cod_empresa):
            return jsonify({"ok": False, "erro": "Sem acesso às avaliações desta filial."}), 403

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
                id_usuario_executor = COALESCE(id_usuario_executor, %s),
                nome_executor = COALESCE(nome_executor, %s),
                atualizado_em = NOW()
            WHERE id_execucao = %s
              AND cod_empresa = %s
        """, (
            total_possivel,
            total_obtido,
            nota_final,
            session.get("id_usuario"),
            _nome_executor(),
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