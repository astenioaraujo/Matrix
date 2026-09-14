"""Módulo Logística — montagem de carregamentos.

Cadastros (Configurações de Logística): carretas, cavalos e motoristas.
A distribuidora é o cadastro de Operações (fornecedores_combustiveis) e o
preço sugerido na composição sai de precos_compra — a mesma rotina de
Operações → Informar Preço de Compra.
"""
from datetime import datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from flask import Blueprint, render_template, session, redirect, url_for, flash, request
from psycopg2.extras import RealDictCursor

from db import get_connection
from security_helpers import permissao_obrigatoria, usuario_tem_permissao

logistica_bp = Blueprint("logistica", __name__, url_prefix="/logistica")

QTD_TANQUES = 12
COLUNAS_TANQUES = [f"tanque_{i:02d}" for i in range(1, QTD_TANQUES + 1)]


def _hoje_br():
    return datetime.now(ZoneInfo("America/Recife")).date()


def _empresa():
    return str(session["cod_empresa"]).strip()


def _pode(opcao):
    if session.get("tipo_global") == "superusuario":
        return True
    return usuario_tem_permissao(session["id_usuario"], _empresa(), "LOGISTICA", opcao)


def _decimal_br(texto):
    """'1.234,56' → Decimal('1234.56'). Vazio → 0. Inválido → None."""
    texto = (texto or "").strip()
    if not texto:
        return Decimal("0")
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return Decimal(texto)
    except InvalidOperation:
        return None


def _inteiro(texto):
    texto = (texto or "").strip().replace(".", "")
    if not texto:
        return None
    try:
        return int(texto)
    except ValueError:
        return None


# ------------------------------------------------------------------
# MENU
# ------------------------------------------------------------------

@logistica_bp.route("/")
@permissao_obrigatoria("LOGISTICA", "MENU")
def menu_logistica():
    superusuario = session.get("tipo_global") == "superusuario"
    pode = {
        "CARREGAMENTOS": _pode("CARREGAMENTOS"),
        "CONFIGURACOES": _pode("CONFIGURACOES"),
        "PRECO_COMPRA": superusuario or usuario_tem_permissao(
            session["id_usuario"], _empresa(), "OPERACOES", "INFORMAR_PRECO_COMPRA"
        ),
    }
    return render_template(
        "logistica/menu.html",
        nome_empresa=session.get("nome_empresa", ""),
        pode=pode,
        url_voltar=url_for("sistema.selecionar_sistema"),
    )


@logistica_bp.route("/configuracoes")
@permissao_obrigatoria("LOGISTICA", "CONFIGURACOES", redirecionar_para="logistica.menu_logistica")
def configuracoes():
    return render_template(
        "logistica/configuracoes.html",
        nome_empresa=session.get("nome_empresa", ""),
        url_voltar=url_for("logistica.menu_logistica"),
    )


# ------------------------------------------------------------------
# CARRETAS
# ------------------------------------------------------------------

@logistica_bp.route("/configuracoes/carretas", methods=["GET", "POST"])
@permissao_obrigatoria("LOGISTICA", "CONFIGURACOES", redirecionar_para="logistica.menu_logistica")
def carretas():
    cod_empresa = _empresa()
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            id_carreta = _inteiro(request.form.get("id_carreta"))
            placa = (request.form.get("placa") or "").strip().upper()
            ativo = request.form.get("ativo") == "on"
            tanques = [_inteiro(request.form.get(c)) for c in COLUNAS_TANQUES]

            if not placa:
                flash("Informe a placa da carreta.", "error")
                return redirect(url_for("logistica.carretas"))
            if not any(tanques):
                flash("Informe a capacidade de pelo menos um tanque.", "error")
                return redirect(url_for("logistica.carretas"))

            sets = ", ".join(f"{c} = %s" for c in COLUNAS_TANQUES)
            if id_carreta:
                cur.execute(f"""
                    UPDATE logistica_carretas
                    SET placa = %s, {sets}, ativo = %s, atualizado_em = NOW()
                    WHERE id_carreta = %s AND cod_empresa = %s
                """, [placa, *tanques, ativo, id_carreta, cod_empresa])
            else:
                colunas = ", ".join(COLUNAS_TANQUES)
                marcas = ", ".join(["%s"] * QTD_TANQUES)
                cur.execute(f"""
                    INSERT INTO logistica_carretas (cod_empresa, placa, {colunas}, ativo)
                    VALUES (%s, %s, {marcas}, %s)
                """, [cod_empresa, placa, *tanques, ativo])

            conn.commit()
            flash(f"Carreta {placa} salva.", "success")
            return redirect(url_for("logistica.carretas"))

        cur.execute("""
            SELECT *
            FROM logistica_carretas
            WHERE cod_empresa = %s
            ORDER BY ativo DESC, placa
        """, (cod_empresa,))
        lista = cur.fetchall() or []

    except Exception as e:
        conn.rollback()
        if "uq_logistica_carretas_placa" in str(e):
            flash("Já existe uma carreta com essa placa.", "error")
        else:
            flash(f"Erro ao salvar carreta: {e}", "error")
        return redirect(url_for("logistica.carretas"))
    finally:
        cur.close()
        conn.close()

    for c in lista:
        c["capacidade_total"] = sum(c[t] or 0 for t in COLUNAS_TANQUES)
        c["qtd_tanques"] = sum(1 for t in COLUNAS_TANQUES if c[t])

    return render_template(
        "logistica/carretas.html",
        nome_empresa=session.get("nome_empresa", ""),
        carretas=lista,
        colunas_tanques=COLUNAS_TANQUES,
        url_voltar=url_for("logistica.configuracoes"),
    )


# ------------------------------------------------------------------
# CAVALOS E MOTORISTAS — mesmo template, dirigido pela especificação
# ------------------------------------------------------------------

CADASTROS_SIMPLES = {
    "cavalos": {
        "tabela": "logistica_cavalos",
        "pk": "id_cavalo",
        "titulo": "Cavalos",
        "singular": "cavalo",
        "campos": [
            {"nome": "placa", "rotulo": "Placa", "obrigatorio": True, "maiusculo": True},
        ],
        "ordem": "placa",
        "unico": "uq_logistica_cavalos_placa",
    },
    "motoristas": {
        "tabela": "logistica_motoristas",
        "pk": "id_motorista",
        "titulo": "Motoristas",
        "singular": "motorista",
        "campos": [
            {"nome": "nome", "rotulo": "Nome", "obrigatorio": True},
            {"nome": "cpf", "rotulo": "CPF", "cpf": True},
        ],
        "ordem": "nome",
    },
}


def _formatar_cpf(texto):
    digitos = "".join(ch for ch in (texto or "") if ch.isdigit())
    if len(digitos) != 11:
        return None
    return f"{digitos[:3]}.{digitos[3:6]}.{digitos[6:9]}-{digitos[9:]}"


@logistica_bp.route("/configuracoes/<cadastro>", methods=["GET", "POST"])
@permissao_obrigatoria("LOGISTICA", "CONFIGURACOES", redirecionar_para="logistica.menu_logistica")
def cadastro_simples(cadastro):
    spec = CADASTROS_SIMPLES.get(cadastro)
    if not spec:
        return redirect(url_for("logistica.configuracoes"))

    cod_empresa = _empresa()
    tabela, pk = spec["tabela"], spec["pk"]
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            id_registro = _inteiro(request.form.get("id"))
            ativo = request.form.get("ativo") == "on"
            valores = []
            for campo in spec["campos"]:
                valor = (request.form.get(campo["nome"]) or "").strip()
                if campo.get("maiusculo"):
                    valor = valor.upper()
                if campo.get("cpf") and valor:
                    formatado = _formatar_cpf(valor)
                    if not formatado:
                        flash("CPF inválido: precisa ter 11 dígitos.", "error")
                        return redirect(url_for("logistica.cadastro_simples", cadastro=cadastro))
                    valor = formatado
                if campo.get("obrigatorio") and not valor:
                    flash(f"Informe o campo {campo['rotulo']}.", "error")
                    return redirect(url_for("logistica.cadastro_simples", cadastro=cadastro))
                valores.append(valor or None)

            nomes = [c["nome"] for c in spec["campos"]]
            if id_registro:
                sets = ", ".join(f"{n} = %s" for n in nomes)
                cur.execute(f"""
                    UPDATE {tabela}
                    SET {sets}, ativo = %s, atualizado_em = NOW()
                    WHERE {pk} = %s AND cod_empresa = %s
                """, [*valores, ativo, id_registro, cod_empresa])
            else:
                cur.execute(f"""
                    INSERT INTO {tabela} (cod_empresa, {", ".join(nomes)}, ativo)
                    VALUES (%s, {", ".join(["%s"] * len(nomes))}, %s)
                """, [cod_empresa, *valores, ativo])

            conn.commit()
            flash(f"{spec['singular'].capitalize()} salvo.", "success")
            return redirect(url_for("logistica.cadastro_simples", cadastro=cadastro))

        cur.execute(f"""
            SELECT {pk} AS id, *
            FROM {tabela}
            WHERE cod_empresa = %s
            ORDER BY ativo DESC, {spec['ordem']}
        """, (cod_empresa,))
        lista = cur.fetchall() or []

    except Exception as e:
        conn.rollback()
        if spec.get("unico") and spec["unico"] in str(e):
            flash(f"Já existe um {spec['singular']} com essa placa.", "error")
        else:
            flash(f"Erro ao salvar: {e}", "error")
        return redirect(url_for("logistica.cadastro_simples", cadastro=cadastro))
    finally:
        cur.close()
        conn.close()

    return render_template(
        "logistica/cadastro_simples.html",
        nome_empresa=session.get("nome_empresa", ""),
        spec=spec,
        cadastro=cadastro,
        registros=lista,
        url_voltar=url_for("logistica.configuracoes"),
    )


@logistica_bp.route("/configuracoes/<cadastro>/excluir/<int:id_registro>", methods=["POST"])
@permissao_obrigatoria("LOGISTICA", "CONFIGURACOES", redirecionar_para="logistica.menu_logistica")
def excluir_cadastro(cadastro, id_registro):
    if cadastro == "carretas":
        tabela, pk, destino = "logistica_carretas", "id_carreta", url_for("logistica.carretas")
    elif cadastro in CADASTROS_SIMPLES:
        spec = CADASTROS_SIMPLES[cadastro]
        tabela, pk = spec["tabela"], spec["pk"]
        destino = url_for("logistica.cadastro_simples", cadastro=cadastro)
    else:
        return redirect(url_for("logistica.configuracoes"))

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"DELETE FROM {tabela} WHERE {pk} = %s AND cod_empresa = %s",
                    (id_registro, _empresa()))
        conn.commit()
        flash("Registro excluído." if cur.rowcount else "Registro não encontrado.",
              "success" if cur.rowcount else "error")
    except Exception as e:
        conn.rollback()
        if "foreign key" in str(e).lower():
            # já usado em carregamento: histórico não se apaga
            flash("Este registro já foi usado em carregamento. Desmarque Ativo em vez de excluir.", "error")
        else:
            flash(f"Erro ao excluir: {e}", "error")
    finally:
        cur.close()
        conn.close()
    return redirect(destino)


# ------------------------------------------------------------------
# CARREGAMENTOS
# ------------------------------------------------------------------

def _precos_na_data(cur, cod_empresa, data_ref):
    """Último preço de compra de cada produto até a data (inclusive).
    O preço é informado por dia; dia sem preço usa o mais recente antes dele.
    Preço zero conta como não informado (a tela de Operações grava zero
    quando o campo é apagado)."""
    cur.execute("""
        SELECT DISTINCT ON (cod_produto) cod_produto, preco_compra, data_preco
        FROM precos_compra
        WHERE cod_empresa = %s AND data_preco <= %s AND preco_compra > 0
        ORDER BY cod_produto, data_preco DESC
    """, (cod_empresa, data_ref))
    return {
        str(r["cod_produto"]).strip(): {
            "preco": float(r["preco_compra"] or 0),
            "data": r["data_preco"].strftime("%d/%m/%Y"),
        }
        for r in cur.fetchall()
    }


@logistica_bp.route("/carregamentos")
@permissao_obrigatoria("LOGISTICA", "CARREGAMENTOS", redirecionar_para="logistica.menu_logistica")
def carregamentos():
    cod_empresa = _empresa()
    hoje = _hoje_br()
    try:
        ano = int(request.args.get("ano") or hoje.year)
        mes = int(request.args.get("mes") or hoje.month)
    except ValueError:
        ano, mes = hoje.year, hoje.month

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT
                c.id_carregamento, c.numero, c.data_pedido, c.data_carregamento,
                m.nome AS motorista, f.nome_fornecedor AS distribuidora,
                ca.placa AS placa_carreta, cv.placa AS placa_cavalo,
                COALESCE(SUM(i.quantidade), 0) AS volume,
                COALESCE(SUM(i.quantidade * i.preco_unitario), 0) AS valor,
                COUNT(DISTINCT i.cod_filial) AS qtd_postos
            FROM logistica_carregamentos c
            LEFT JOIN logistica_motoristas m ON m.id_motorista = c.id_motorista
            LEFT JOIN fornecedores_combustiveis f ON f.id_fornecedor = c.id_fornecedor
            LEFT JOIN logistica_carretas ca ON ca.id_carreta = c.id_carreta
            LEFT JOIN logistica_cavalos cv ON cv.id_cavalo = c.id_cavalo
            LEFT JOIN logistica_carregamentos_itens i ON i.id_carregamento = c.id_carregamento
            WHERE c.cod_empresa = %s
              AND EXTRACT(YEAR FROM c.data_pedido) = %s
              AND EXTRACT(MONTH FROM c.data_pedido) = %s
            GROUP BY c.id_carregamento, m.nome, f.nome_fornecedor, ca.placa, cv.placa
            ORDER BY c.data_pedido DESC, c.numero DESC
        """, (cod_empresa, ano, mes))
        lista = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    return render_template(
        "logistica/carregamentos.html",
        nome_empresa=session.get("nome_empresa", ""),
        carregamentos=lista,
        ano=ano,
        mes=mes,
        anos=list(range(hoje.year - 2, hoje.year + 2)),
        total_volume=sum(float(c["volume"]) for c in lista),
        total_valor=sum(float(c["valor"]) for c in lista),
        url_voltar=url_for("logistica.menu_logistica"),
    )


@logistica_bp.route("/carregamentos/novo", methods=["GET", "POST"])
@logistica_bp.route("/carregamentos/<int:id_carregamento>", methods=["GET", "POST"])
@permissao_obrigatoria("LOGISTICA", "CARREGAMENTOS", redirecionar_para="logistica.menu_logistica")
def carregamento_form(id_carregamento=None):
    cod_empresa = _empresa()
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST":
            try:
                return _salvar_carregamento(conn, cur, cod_empresa, id_carregamento)
            except Exception as e:
                conn.rollback()
                flash(f"Erro ao salvar carregamento: {e}", "error")
                return redirect(request.url)

        cabecalho, itens = None, []
        if id_carregamento:
            cur.execute("""
                SELECT * FROM logistica_carregamentos
                WHERE id_carregamento = %s AND cod_empresa = %s
            """, (id_carregamento, cod_empresa))
            cabecalho = cur.fetchone()
            if not cabecalho:
                flash("Carregamento não encontrado.", "error")
                return redirect(url_for("logistica.carregamentos"))
            cur.execute("""
                SELECT cod_filial, cod_produto, quantidade, preco_unitario, codigo
                FROM logistica_carregamentos_itens
                WHERE id_carregamento = %s
                ORDER BY ordem, id_carregamento_item
            """, (id_carregamento,))
            itens = [
                {
                    "cod_filial": r["cod_filial"],
                    "cod_produto": str(r["cod_produto"]).strip(),
                    "quantidade": float(r["quantidade"]),
                    "preco_unitario": float(r["preco_unitario"]),
                    "codigo": r["codigo"] or "",
                }
                for r in cur.fetchall()
            ]

        data_pedido = cabecalho["data_pedido"] if cabecalho else _hoje_br()

        # Cadastro inativo some das opções, mas continua aparecendo no
        # carregamento antigo que o usou.
        usados = cabecalho or {}
        cur.execute("""
            SELECT id_carreta, placa, ativo, """ + ", ".join(COLUNAS_TANQUES) + """
            FROM logistica_carretas
            WHERE cod_empresa = %s AND (ativo OR id_carreta = %s)
            ORDER BY placa
        """, (cod_empresa, usados.get("id_carreta")))
        lista_carretas = []
        for c in cur.fetchall():
            tanques = [c[t] for t in COLUNAS_TANQUES if c[t]]
            lista_carretas.append({
                "id": c["id_carreta"], "placa": c["placa"],
                "tanques": tanques, "capacidade": sum(tanques),
            })

        cur.execute("""
            SELECT id_cavalo AS id, placa FROM logistica_cavalos
            WHERE cod_empresa = %s AND (ativo OR id_cavalo = %s) ORDER BY placa
        """, (cod_empresa, usados.get("id_cavalo")))
        lista_cavalos = cur.fetchall()

        cur.execute("""
            SELECT id_motorista AS id, nome, cpf FROM logistica_motoristas
            WHERE cod_empresa = %s AND (ativo OR id_motorista = %s) ORDER BY nome
        """, (cod_empresa, usados.get("id_motorista")))
        lista_motoristas = cur.fetchall()

        cur.execute("""
            SELECT id_fornecedor AS id, nome_fornecedor AS nome
            FROM fornecedores_combustiveis
            WHERE cod_empresa = %s AND (COALESCE(ativo, true) OR id_fornecedor = %s)
            ORDER BY nome_fornecedor
        """, (cod_empresa, usados.get("id_fornecedor")))
        lista_distribuidoras = cur.fetchall()

        cur.execute("""
            SELECT cod_filial, nome_filial, cnpj
            FROM filiais
            WHERE cod_empresa = %s AND ativo
            ORDER BY cod_filial
        """, (cod_empresa,))
        postos = [
            {"cod": r["cod_filial"], "nome": r["nome_filial"], "cnpj": r["cnpj"] or ""}
            for r in cur.fetchall()
        ]

        cur.execute("""
            SELECT cod_produto, descricao FROM combustiveis
            WHERE cod_empresa = %s ORDER BY cod_produto
        """, (cod_empresa,))
        produtos = [
            {"cod": str(r["cod_produto"]).strip(), "nome": r["descricao"]}
            for r in cur.fetchall()
        ]

        precos = _precos_na_data(cur, cod_empresa, data_pedido)

    finally:
        cur.close()
        conn.close()

    return render_template(
        "logistica/carregamento_form.html",
        nome_empresa=session.get("nome_empresa", ""),
        cab=cabecalho,
        data_pedido=data_pedido.isoformat(),
        itens=itens,
        carretas=lista_carretas,
        cavalos=lista_cavalos,
        motoristas=lista_motoristas,
        distribuidoras=lista_distribuidoras,
        postos=postos,
        produtos=produtos,
        precos=precos,
        url_voltar=url_for("logistica.carregamentos"),
    )


def _salvar_carregamento(conn, cur, cod_empresa, id_carregamento):
    """Grava cabeçalho e composição numa transação só. A composição é
    reescrita inteira: a tela sempre manda todas as linhas."""
    f = request.form
    voltar = (url_for("logistica.carregamento_form", id_carregamento=id_carregamento)
              if id_carregamento else url_for("logistica.carregamento_form"))

    data_pedido = (f.get("data_pedido") or "").strip()
    data_carregamento = (f.get("data_carregamento") or "").strip() or None
    if not data_pedido:
        flash("Informe a data do pedido.", "error")
        return redirect(voltar)

    cabecalho = {
        "id_carreta": _inteiro(f.get("id_carreta")),
        "id_cavalo": _inteiro(f.get("id_cavalo")),
        "id_motorista": _inteiro(f.get("id_motorista")),
        "id_fornecedor": _inteiro(f.get("id_fornecedor")),
    }
    faltando = [rot for campo, rot in (
        ("id_carreta", "carreta"), ("id_cavalo", "cavalo"),
        ("id_motorista", "motorista"), ("id_fornecedor", "distribuidora"),
    ) if not cabecalho[campo]]
    if faltando:
        flash("Escolha: " + ", ".join(faltando) + ".", "error")
        return redirect(voltar)

    # linhas da composição
    cur.execute("SELECT cod_filial FROM filiais WHERE cod_empresa = %s", (cod_empresa,))
    postos_validos = {r["cod_filial"] for r in cur.fetchall()}
    cur.execute("SELECT cod_produto FROM combustiveis WHERE cod_empresa = %s", (cod_empresa,))
    produtos_validos = {str(r["cod_produto"]).strip() for r in cur.fetchall()}

    itens = []
    for n, (filial, produto, qtd, preco, codigo) in enumerate(zip(
        f.getlist("item_filial"), f.getlist("item_produto"), f.getlist("item_quantidade"),
        f.getlist("item_preco"), f.getlist("item_codigo"),
    ), start=1):
        if not (filial or produto or qtd.strip()):
            continue  # linha em branco
        cod_filial = _inteiro(filial)
        quantidade = _decimal_br(qtd)
        preco_unitario = _decimal_br(preco)
        if cod_filial not in postos_validos or produto not in produtos_validos:
            flash(f"Linha {n}: escolha o posto e o produto.", "error")
            return redirect(voltar)
        if quantidade is None or quantidade <= 0:
            flash(f"Linha {n}: quantidade inválida.", "error")
            return redirect(voltar)
        if preco_unitario is None or preco_unitario < 0:
            flash(f"Linha {n}: valor unitário inválido.", "error")
            return redirect(voltar)
        itens.append((cod_filial, produto, quantidade, preco_unitario,
                      (codigo or "").strip().upper() or None, n * 10))

    observacao = (f.get("observacao") or "").strip() or None

    if id_carregamento:
        cur.execute("""
            UPDATE logistica_carregamentos
            SET data_pedido = %s, data_carregamento = %s,
                id_carreta = %s, id_cavalo = %s, id_motorista = %s, id_fornecedor = %s,
                observacao = %s, atualizado_em = NOW()
            WHERE id_carregamento = %s AND cod_empresa = %s
        """, (data_pedido, data_carregamento, cabecalho["id_carreta"], cabecalho["id_cavalo"],
              cabecalho["id_motorista"], cabecalho["id_fornecedor"], observacao,
              id_carregamento, cod_empresa))
        if cur.rowcount == 0:
            conn.rollback()
            flash("Carregamento não encontrado.", "error")
            return redirect(url_for("logistica.carregamentos"))
    else:
        cur.execute("""
            INSERT INTO logistica_carregamentos (
                cod_empresa, numero, data_pedido, data_carregamento,
                id_carreta, id_cavalo, id_motorista, id_fornecedor,
                observacao, id_usuario_criacao
            )
            SELECT %s, COALESCE(MAX(numero), 0) + 1, %s, %s, %s, %s, %s, %s, %s, %s
            FROM logistica_carregamentos WHERE cod_empresa = %s
            RETURNING id_carregamento, numero
        """, (cod_empresa, data_pedido, data_carregamento, cabecalho["id_carreta"],
              cabecalho["id_cavalo"], cabecalho["id_motorista"], cabecalho["id_fornecedor"],
              observacao, session.get("id_usuario"), cod_empresa))
        id_carregamento = cur.fetchone()["id_carregamento"]

    cur.execute("DELETE FROM logistica_carregamentos_itens WHERE id_carregamento = %s",
                (id_carregamento,))
    for cod_filial, produto, quantidade, preco_unitario, codigo, ordem in itens:
        cur.execute("""
            INSERT INTO logistica_carregamentos_itens (
                cod_empresa, id_carregamento, cod_filial, cod_produto,
                quantidade, preco_unitario, codigo, ordem
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (cod_empresa, id_carregamento, cod_filial, produto,
              quantidade, preco_unitario, codigo, ordem))

    conn.commit()
    flash("Carregamento salvo.", "success")
    return redirect(url_for("logistica.carregamento_form", id_carregamento=id_carregamento))


@logistica_bp.route("/carregamentos/<int:id_carregamento>/excluir", methods=["POST"])
@permissao_obrigatoria("LOGISTICA", "CARREGAMENTOS", redirecionar_para="logistica.menu_logistica")
def excluir_carregamento(id_carregamento):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            DELETE FROM logistica_carregamentos
            WHERE id_carregamento = %s AND cod_empresa = %s
        """, (id_carregamento, _empresa()))
        conn.commit()
        flash("Carregamento excluído." if cur.rowcount else "Carregamento não encontrado.",
              "success" if cur.rowcount else "error")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("logistica.carregamentos"))


@logistica_bp.route("/api/precos-compra")
@permissao_obrigatoria("LOGISTICA", "CARREGAMENTOS", redirecionar_para="logistica.menu_logistica")
def api_precos_compra():
    """Preços sugeridos quando a data do pedido muda na tela."""
    data_ref = (request.args.get("data") or "").strip()
    if not data_ref:
        return {"ok": False, "erro": "Informe a data."}, 400
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        return {"ok": True, "precos": _precos_na_data(cur, _empresa(), data_ref)}
    finally:
        cur.close()
        conn.close()
