"""
PDV Matrix — frente de loja.

Arquitetura (documento "Estrutura do Financeiro para Sistemas de Gestão
Informatizados", Inovai, 16/08/2026):

    Venda → Itens da Venda
    Venda → Formas de Recebimento

E, a partir da conclusão, cada forma de recebimento desagua no seu módulo:

    DINHEIRO / PIX   → pdv_lancamentos_financeiros (entrou dinheiro)
    DEBITO / CREDITO → pdv_cartoes_recebimentos + pdv_cartoes_parcelas
    NOTA_PRAZO       → pdv_notas_prazo (pendência do cliente)
    CONSIGNACAO      → pdv_consignacoes (maletas, conciliação própria)

A venda, uma vez concluída, não volta a ser alterada. Tudo o que acontece
depois pertence ao Financeiro.
"""

from datetime import date, datetime

from flask import (Blueprint, render_template, redirect, url_for, session,
                   flash, request, jsonify)
from psycopg2.extras import RealDictCursor

from db import get_connection
from security_helpers import usuario_tem_permissao
from services.pdv_service import empresa_opera_pdv
from services.pdv_estoque_service import (baixar_itens_da_venda, extrato_produto,
                                          movimentar, TIPOS_MOVIMENTO)
from services.pdv_entrada_service import (ratear_frete, custo_unitario,
                                          gerar_titulos_pagar, parcelar)
from services.pdv_campanhas_service import (campanhas_vigentes, promocoes_do_dia,
                                            preco_promocional, carregar_itens,
                                            SITUACOES_CAMPANHA)
from services.pdv_importacao_estoque_service import (ler_csv, agrupar_por_sku,
                                                     importar, data_do_nome)
from services.pdv_produtos_filiais_service import (incluir_por_sku, ocultar,
                                                   candidatos_a_ocultar,
                                                   MESES_SEM_MOVIMENTO)
from services.pdv_canais_service import (canais_da_filial, canal_padrao,
                                         estoque_por_canal, saldos_por_canal,
                                         transferir_estoque)
from services.pdv_devolucao_service import (itens_disponiveis, registrar_devolucao,
                                            DESTINOS_VALOR)
from services.pdv_titulos_manuais_service import (tipos_despesa, listar_titulos,
                                                  incluir_titulo, excluir_titulo,
                                                  orcamento_do_ano, salvar_previsao,
                                                  replicar_previsao,
                                                  gerar_titulos_do_mes,
                                                  fluxo_caixa_pagar,
                                                  totais_por_grupo_conta, MESES)
from services.pdv_financeiro_service import (lancar, saldo_ate, transferir, extrato_conta,
                                             caixa_geral, baixar_nota_prazo,
                                             converter_nota_em_titulos,
                                             baixar_titulo_receber, baixar_titulo_pagar,
                                             ORIGENS_LANCAMENTO)

pdv_bp = Blueprint("pdv", __name__, url_prefix="/pdv")


# ─── ACESSO ──────────────────────────────────────────────────────────────────
# São duas travas independentes:
#   1. a empresa opera com PDV? (Configurações → Configuração de PDV)
#   2. o usuário tem a permissão da tela?
# A primeira vale inclusive para o superusuário — é a proteção que impede o
# módulo de aparecer em empresa que não deveria tê-lo. E ela está aqui, não só
# no menu: sem isto, a URL digitada à mão entraria.

def _superusuario():
    return str(session.get("tipo_global") or "").strip().lower() == "superusuario"


def pode(opcao):
    """Permissão do usuário logado dentro do sistema PDV."""
    if "id_usuario" not in session or "cod_empresa" not in session:
        return False
    if not empresa_opera_pdv(session["cod_empresa"]):
        return False
    if _superusuario():
        return True
    return usuario_tem_permissao(
        session["id_usuario"], str(session["cod_empresa"]).strip(), "PDV", opcao
    )


def _checar_acesso(opcao="MENU"):
    """Devolve um redirect quando o acesso é negado, ou None quando liberado."""
    if "id_usuario" not in session or "cod_empresa" not in session:
        return redirect(url_for("auth.index"))
    if not empresa_opera_pdv(session["cod_empresa"]):
        flash("Esta empresa não opera com PDV.", "error")
        return redirect(url_for("sistema.selecionar_sistema"))
    if not pode(opcao):
        flash("Você não tem permissão para acessar esta tela do PDV.", "error")
        return redirect(url_for("sistema.selecionar_sistema"))
    return None


def _erro_permissao(opcao):
    """Versão JSON do _checar_acesso, para os endpoints de API."""
    if "id_usuario" not in session or "cod_empresa" not in session:
        return jsonify({"ok": False, "erro": "Sessão expirada."}), 401
    if not empresa_opera_pdv(session["cod_empresa"]):
        return jsonify({"ok": False, "erro": "Esta empresa não opera com PDV."}), 403
    if not pode(opcao):
        return jsonify({"ok": False, "erro": "Sem permissão."}), 403
    return None


def _empresa():
    return str(session.get("cod_empresa") or "").strip()


def _cod_filial():
    """
    O PDV é de loja única por enquanto (O Closet tem só a filial 1). Quando
    houver mais de uma, isto passa a sair da tela; centralizado aqui para não
    espalhar o "1" pelo código.
    """
    return 1


def _cursor():
    return get_connection().cursor(cursor_factory=RealDictCursor)


# ─── CADASTROS (dirigidos por especificação) ─────────────────────────────────
# Os cinco cadastros do PDV têm a mesma tela: uma grade com uma linha por
# registro, salva/exclui por linha. Em vez de cinco templates quase iguais, a
# tela é montada a partir desta especificação. Cadastro novo = uma entrada
# aqui, sem template nem endpoint novo.
#
#   tipo do campo: texto | numero | dinheiro | checkbox | data | opcoes
#
#   "por_filial": True  →  o cadastro pertence à loja, não à empresa. A lista
#   só traz os da filial corrente e o registro novo nasce nela. É o caso da
#   vendedora: quem vende numa loja não vende na outra.

CADASTROS = {
    "clientes": {
        "titulo": "Clientes",
        "descricao": "Clientes da loja. O limite de crédito orienta a venda a prazo.",
        "permissao": "CLIENTES",
        "tabela": "pdv_clientes",
        "pk": "id_pdv_cliente",
        "ordenacao": "ordem, nome",
        "campos": [
            {"nome": "ordem", "rotulo": "Ordem", "tipo": "numero", "largura": "80px", "padrao": 10},
            {"nome": "nome", "rotulo": "Nome", "tipo": "texto", "obrigatorio": True},
            {"nome": "cpf_cnpj", "rotulo": "CPF/CNPJ", "tipo": "texto", "largura": "150px"},
            {"nome": "telefone", "rotulo": "Telefone", "tipo": "texto", "largura": "140px"},
            {"nome": "email", "rotulo": "E-mail", "tipo": "texto", "largura": "200px"},
            {"nome": "limite_credito", "rotulo": "Limite de Crédito", "tipo": "dinheiro", "largura": "140px", "padrao": 0},
            {"nome": "ativo", "rotulo": "Ativo", "tipo": "checkbox", "largura": "70px", "padrao": True},
        ],
    },
    "vendedores": {
        "titulo": "Vendedores",
        "descricao": ("Quem realiza as vendas nesta loja. Não precisa ser usuário do "
                      "Matrix. A vendedora de uma filial não aparece na outra."),
        "permissao": "VENDEDORES",
        "tabela": "pdv_vendedores",
        "pk": "id_pdv_vendedor",
        "ordenacao": "ordem, nome",
        "por_filial": True,
        "campos": [
            {"nome": "ordem", "rotulo": "Ordem", "tipo": "numero", "largura": "80px", "padrao": 10},
            {"nome": "nome", "rotulo": "Nome", "tipo": "texto", "obrigatorio": True},
            {"nome": "apelido", "rotulo": "Apelido", "tipo": "texto", "largura": "180px"},
            {"nome": "ativo", "rotulo": "Ativo", "tipo": "checkbox", "largura": "70px", "padrao": True},
        ],
    },
    "produtos": {
        "titulo": "Produtos",
        "descricao": "Cadastro e saldo dos itens de estoque. A quantidade é movimentada pelas vendas e pelas entradas de mercadoria.",
        "permissao": "PRODUTOS",
        "tabela": "pdv_produtos",
        "pk": "id_pdv_produto",
        "ordenacao": "ordem, descricao",
        "campos": [
            {"nome": "ordem", "rotulo": "Ordem", "tipo": "numero", "largura": "80px", "padrao": 10},
            {"nome": "sku", "rotulo": "SKU", "tipo": "texto", "largura": "150px"},
            {"nome": "descricao", "rotulo": "Descrição", "tipo": "texto", "obrigatorio": True},
            {"nome": "unidade", "rotulo": "Un.", "tipo": "texto", "largura": "70px", "padrao": "UN"},
            {"nome": "preco_venda", "rotulo": "Preço de Venda", "tipo": "dinheiro", "largura": "130px", "padrao": 0},
            {"nome": "custo_atual", "rotulo": "Custo", "tipo": "dinheiro", "largura": "120px", "padrao": 0},
            # quantidade_atual NÃO entra aqui de propósito: saldo só muda por
            # movimento de estoque (venda, entrada, ajuste), nunca digitado
            # direto no cadastro — senão fica saldo sem lastro. Para acertar,
            # use Estoque → Ajustar.
            {"nome": "ativo", "rotulo": "Ativo", "tipo": "checkbox", "largura": "70px", "padrao": True},
        ],
    },
    "contas-financeiras": {
        "titulo": "Contas Financeiras",
        "descricao": "Qualquer lugar onde o dinheiro possa ficar: o caixa da loja é uma conta como o banco.",
        "permissao": "CONTAS_FINANCEIRAS",
        "tabela": "pdv_contas_financeiras",
        "pk": "id_pdv_conta_financeira",
        "ordenacao": "ordem, nome",
        "campos": [
            {"nome": "ordem", "rotulo": "Ordem", "tipo": "numero", "largura": "80px", "padrao": 10},
            {"nome": "nome", "rotulo": "Nome", "tipo": "texto", "obrigatorio": True},
            {"nome": "tipo", "rotulo": "Tipo", "tipo": "opcoes", "largura": "110px", "padrao": "CAIXA",
             "opcoes": [("CAIXA", "Caixa"), ("BANCO", "Banco")]},
            {"nome": "banco", "rotulo": "Banco", "tipo": "texto", "largura": "150px"},
            {"nome": "agencia", "rotulo": "Agência", "tipo": "texto", "largura": "100px"},
            {"nome": "conta", "rotulo": "Conta", "tipo": "texto", "largura": "120px"},
            {"nome": "saldo_inicial", "rotulo": "Saldo Inicial", "tipo": "dinheiro", "largura": "130px", "padrao": 0},
            {"nome": "data_saldo_inicial", "rotulo": "Data do Saldo", "tipo": "data", "largura": "140px"},
            # a gaveta: é nela que o dinheiro da venda cai, sem ninguém escolher
            {"nome": "caixa_padrao", "rotulo": "Caixa da Loja", "tipo": "checkbox",
             "largura": "100px", "padrao": False},
            {"nome": "ativo", "rotulo": "Ativo", "tipo": "checkbox", "largura": "70px", "padrao": True},
        ],
    },
    "fornecedores": {
        "titulo": "Fornecedores",
        "descricao": "De quem a loja compra. As notas de entrada e os títulos a pagar saem daqui.",
        "permissao": "FORNECEDORES",
        "tabela": "pdv_fornecedores",
        "pk": "id_pdv_fornecedor",
        "ordenacao": "ordem, nome",
        "campos": [
            {"nome": "ordem", "rotulo": "Ordem", "tipo": "numero", "largura": "80px", "padrao": 10},
            {"nome": "nome", "rotulo": "Nome", "tipo": "texto", "obrigatorio": True},
            {"nome": "cnpj", "rotulo": "CNPJ", "tipo": "texto", "largura": "160px"},
            {"nome": "telefone", "rotulo": "Telefone", "tipo": "texto", "largura": "140px"},
            {"nome": "email", "rotulo": "E-mail", "tipo": "texto", "largura": "200px"},
            {"nome": "ativo", "rotulo": "Ativo", "tipo": "checkbox", "largura": "70px", "padrao": True},
        ],
    },
    "ctes": {
        "titulo": "CT-e / Fretes",
        "descricao": ("Lance o CT-e antes da nota fiscal: na entrada da mercadoria ele é "
                      "vinculado à compra e o frete entra no custo dos produtos."),
        "permissao": "CTE",
        "tabela": "pdv_ctes",
        "pk": "id_pdv_cte",
        "ordenacao": "ordem, data_emissao DESC, id_pdv_cte DESC",
        "campos": [
            {"nome": "ordem", "rotulo": "Ordem", "tipo": "numero", "largura": "80px", "padrao": 10},
            {"nome": "transportadora", "rotulo": "Transportadora", "tipo": "texto", "obrigatorio": True},
            {"nome": "numero", "rotulo": "Número", "tipo": "texto", "largura": "110px"},
            {"nome": "serie", "rotulo": "Série", "tipo": "texto", "largura": "80px"},
            {"nome": "chave", "rotulo": "Chave", "tipo": "texto", "largura": "200px"},
            {"nome": "data_emissao", "rotulo": "Emissão", "tipo": "data", "largura": "140px"},
            {"nome": "valor_total", "rotulo": "Valor do Frete", "tipo": "dinheiro", "largura": "130px", "padrao": 0},
            {"nome": "ativo", "rotulo": "Ativo", "tipo": "checkbox", "largura": "70px", "padrao": True},
        ],
    },
    "operadoras-cartao": {
        "titulo": "Operadoras de Cartão",
        "descricao": "Os dias de crédito definem a previsão de recebimento de cada parcela.",
        "permissao": "OPERADORAS_CARTAO",
        "tabela": "pdv_operadoras_cartao",
        "pk": "id_pdv_operadora",
        "ordenacao": "ordem, nome",
        "campos": [
            {"nome": "ordem", "rotulo": "Ordem", "tipo": "numero", "largura": "80px", "padrao": 10},
            {"nome": "nome", "rotulo": "Nome", "tipo": "texto", "obrigatorio": True},
            {"nome": "aceita_debito", "rotulo": "Débito", "tipo": "checkbox", "largura": "70px", "padrao": True},
            {"nome": "aceita_credito", "rotulo": "Crédito", "tipo": "checkbox", "largura": "70px", "padrao": True},
            {"nome": "dias_credito_debito", "rotulo": "Dias (Débito)", "tipo": "numero", "largura": "110px", "padrao": 1},
            {"nome": "dias_credito_credito", "rotulo": "Dias (1ª parc. Crédito)", "tipo": "numero", "largura": "140px", "padrao": 30},
            {"nome": "ativo", "rotulo": "Ativo", "tipo": "checkbox", "largura": "70px", "padrao": True},
        ],
    },
    "despesas-tipos": {
        "titulo": "Tipos de Despesa",
        "descricao": ("O que a loja paga todo mês: luz, água, telefone, aluguel. É por "
                      "este tipo que a despesa se agrupa no orçamento e nos títulos manuais."),
        "permissao": "DESPESAS_TIPOS",
        "tabela": "pdv_despesas_tipos",
        "pk": "id_pdv_despesa_tipo",
        "ordenacao": "ordem, nome",
        "campos": [
            {"nome": "ordem", "rotulo": "Ordem", "tipo": "numero", "largura": "80px", "padrao": 10},
            {"nome": "nome", "rotulo": "Nome", "tipo": "texto", "obrigatorio": True},
            {"nome": "grupo", "rotulo": "Grupo", "tipo": "texto", "largura": "160px"},
            {"nome": "dia_vencimento", "rotulo": "Dia do Vencimento", "tipo": "numero", "largura": "140px", "padrao": 10},
            # A classificação do Fluxo de Caixa do Matrix (grupos_gerenciais /
            # contas_gerenciais). Fica no TIPO, não no título: reclassificar
            # aqui conserta de uma vez todos os títulos daquele tipo.
            {"nome": "cod_grupo", "rotulo": "Grupo", "tipo": "numero", "largura": "80px"},
            {"nome": "cod_conta", "rotulo": "Conta", "tipo": "numero", "largura": "80px"},
            {"nome": "ativo", "rotulo": "Ativo", "tipo": "checkbox", "largura": "70px", "padrao": True},
        ],
    },
}


def _valor_do_campo(campo, bruto):
    """Converte o que veio do JSON para o tipo da coluna."""
    tipo = campo["tipo"]
    if tipo == "checkbox":
        return bool(bruto)
    if tipo in ("numero", "dinheiro"):
        if bruto in (None, ""):
            return campo.get("padrao", 0)
        try:
            return float(str(bruto).replace(",", "."))
        except (TypeError, ValueError):
            return campo.get("padrao", 0)
    if tipo == "data":
        return bruto or None
    texto = (bruto or "")
    texto = texto.strip() if isinstance(texto, str) else texto
    # texto vazio cai no padrão da especificação quando ele existe: limpar a
    # unidade do produto, por exemplo, mandaria NULL para uma coluna NOT NULL
    return texto or campo.get("padrao") or None


# ─── MENU ────────────────────────────────────────────────────────────────────

@pdv_bp.route("/")
def menu_pdv():
    redir = _checar_acesso("MENU")
    if redir:
        return redir
    return render_template(
        "pdv/menu_pdv.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema"),
        pode_vender=pode("VENDER"),
        pode_consultar=pode("CONSULTAR_VENDAS"),
        pode_cadastros=pode("CADASTROS"),
        pode_estoque=pode("ESTOQUE"),
        pode_entradas=pode("ENTRADA_MERCADORIAS"),
        pode_financeiro=pode("FINANCEIRO_MENU"),
        pode_caixa_central=pode("CAIXA_CENTRAL"),
        pode_devolucoes=pode("DEVOLUCOES"),
        pode_canais=pode("CANAIS_VENDA"),
        pode_campanhas=pode("CAMPANHAS_MENU"),
    )


@pdv_bp.route("/cadastros")
def menu_cadastros():
    redir = _checar_acesso("CADASTROS")
    if redir:
        return redir
    itens = [
        {"chave": chave, "titulo": spec["titulo"], "descricao": spec["descricao"]}
        for chave, spec in CADASTROS.items()
        if pode(spec["permissao"])
    ]
    return render_template(
        "pdv/menu_cadastros.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_pdv"),
        itens=itens,
    )


@pdv_bp.route("/cadastros/<chave>")
def cadastro(chave):
    spec = CADASTROS.get(chave)
    if not spec:
        flash("Cadastro não encontrado.", "error")
        return redirect(url_for("pdv.menu_cadastros"))
    redir = _checar_acesso(spec["permissao"])
    if redir:
        return redir
    return render_template(
        "pdv/cadastro.html",
        nome_empresa=session.get("nome_empresa"),
        empresa_ativa=_empresa(),
        url_voltar=url_for("pdv.menu_cadastros"),
        chave=chave,
        spec=spec,
    )


# ─── API DOS CADASTROS ───────────────────────────────────────────────────────

@pdv_bp.route("/api/cadastros/<chave>", methods=["GET"])
def api_cadastro_listar(chave):
    spec = CADASTROS.get(chave)
    if not spec:
        return jsonify({"ok": False, "erro": "Cadastro não encontrado."}), 404
    erro = _erro_permissao(spec["permissao"])
    if erro:
        return erro

    colunas = [spec["pk"]] + [c["nome"] for c in spec["campos"]]
    filtro = " AND cod_filial = %s" if spec.get("por_filial") else ""
    parametros = [_empresa()] + ([_cod_filial()] if spec.get("por_filial") else [])

    cur = _cursor()
    try:
        cur.execute(
            f"SELECT {', '.join(colunas)} FROM {spec['tabela']} "
            f"WHERE cod_empresa = %s{filtro} ORDER BY {spec['ordenacao']}",
            parametros,
        )
        registros = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    for r in registros:
        for k, v in list(r.items()):
            if isinstance(v, (date, datetime)):
                r[k] = v.isoformat()[:10]
            elif hasattr(v, "quantize"):          # numeric → Decimal
                r[k] = float(v)
    return jsonify({"ok": True, "registros": registros})


@pdv_bp.route("/api/cadastros/<chave>", methods=["POST"])
@pdv_bp.route("/api/cadastros/<chave>/<int:id_registro>", methods=["PUT"])
def api_cadastro_gravar(chave, id_registro=None):
    spec = CADASTROS.get(chave)
    if not spec:
        return jsonify({"ok": False, "erro": "Cadastro não encontrado."}), 404
    erro = _erro_permissao(spec["permissao"])
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    valores = {}
    for campo in spec["campos"]:
        valor = _valor_do_campo(campo, dados.get(campo["nome"]))
        if campo.get("obrigatorio") and not valor:
            return jsonify({"ok": False, "erro": f"Informe {campo['rotulo']}."}), 400
        valores[campo["nome"]] = valor

    conn = get_connection()
    cur = conn.cursor()
    try:
        try:
            if id_registro:
                sets = ", ".join(f"{c} = %s" for c in valores) + ", atualizado_em = now()"
                filtro = " AND cod_filial = %s" if spec.get("por_filial") else ""
                extra = [_cod_filial()] if spec.get("por_filial") else []
                cur.execute(
                    f"UPDATE {spec['tabela']} SET {sets} "
                    f"WHERE {spec['pk']} = %s AND cod_empresa = %s{filtro}",
                    list(valores.values()) + [id_registro, _empresa()] + extra,
                )
                if cur.rowcount == 0:
                    return jsonify({"ok": False, "erro": "Registro não encontrado."}), 404
            else:
                colunas = ["cod_empresa"] + list(valores)
                iniciais = [_empresa()]
                if spec.get("por_filial"):
                    colunas.insert(1, "cod_filial")
                    iniciais.append(_cod_filial())
                cur.execute(
                    f"INSERT INTO {spec['tabela']} ({', '.join(colunas)}) "
                    f"VALUES ({', '.join(['%s'] * len(colunas))}) RETURNING {spec['pk']}",
                    iniciais + list(valores.values()),
                )
                id_registro = cur.fetchone()[0]
            conn.commit()
        except Exception as e:
            conn.rollback()
            # só existe uma gaveta por empresa (índice único parcial)
            if "uq_pdv_caixa_padrao" in str(e):
                return jsonify({
                    "ok": False,
                    "erro": ("Já existe outra conta marcada como Caixa da Loja. "
                             "Desmarque a atual antes de marcar esta."),
                }), 400
            raise
    finally:
        cur.close()
    return jsonify({"ok": True, "id": id_registro})


@pdv_bp.route("/api/cadastros/<chave>/<int:id_registro>", methods=["DELETE"])
def api_cadastro_excluir(chave, id_registro):
    spec = CADASTROS.get(chave)
    if not spec:
        return jsonify({"ok": False, "erro": "Cadastro não encontrado."}), 404
    erro = _erro_permissao(spec["permissao"])
    if erro:
        return erro

    conn = get_connection()
    cur = conn.cursor()
    try:
        try:
            filtro = " AND cod_filial = %s" if spec.get("por_filial") else ""
            extra = [_cod_filial()] if spec.get("por_filial") else []
            cur.execute(
                f"DELETE FROM {spec['tabela']} "
                f"WHERE {spec['pk']} = %s AND cod_empresa = %s{filtro}",
                [id_registro, _empresa()] + extra,
            )
        except Exception:
            # registro já usado por uma venda: o histórico não pode ser
            # apagado, então a saída é desativar
            conn.rollback()
            return jsonify({
                "ok": False,
                "erro": "Este registro já foi usado em alguma operação. Desmarque 'Ativo' em vez de excluir.",
            }), 400
        conn.commit()
    finally:
        cur.close()
    return jsonify({"ok": True})


# ─── VENDA ───────────────────────────────────────────────────────────────────

FORMAS_RECEBIMENTO = [
    ("DINHEIRO",    "Dinheiro"),
    ("PIX",         "PIX"),
    ("DEBITO",      "Cartão de Débito"),
    ("CREDITO",     "Cartão de Crédito"),
    ("NOTA_PRAZO",  "Nota a Prazo"),
    ("CONSIGNACAO", "Consignação (maleta)"),
]

# Formas que entram direto no caixa: o dinheiro entrou agora.
FORMAS_EM_ESPECIE = ("DINHEIRO", "PIX")
FORMAS_CARTAO = ("DEBITO", "CREDITO")


def _num(valor, padrao=0.0):
    """Número tolerante: aceita 1.234,56 e texto meio digitado sem virar NaN."""
    if valor in (None, ""):
        return padrao
    if isinstance(valor, (int, float)):
        return float(valor)
    texto = str(valor).strip().replace(".", "").replace(",", ".")
    try:
        return float(texto)
    except ValueError:
        return padrao


def _centavos(valor):
    """Compara valores em centavos — float não fecha soma exata."""
    return int(round(_num(valor) * 100))


@pdv_bp.route("/vender")
def vender():
    redir = _checar_acesso("VENDER")
    if redir:
        return redir

    cur = _cursor()
    try:
        cur.execute(
            "SELECT id_pdv_cliente, nome FROM pdv_clientes "
            "WHERE cod_empresa = %s AND ativo ORDER BY ordem, nome", (_empresa(),))
        clientes = [dict(r) for r in cur.fetchall()]

        cur.execute(
            "SELECT id_pdv_vendedor, nome FROM pdv_vendedores "
            "WHERE cod_empresa = %s AND cod_filial = %s AND ativo "
            "ORDER BY ordem, nome", (_empresa(), _cod_filial()))
        vendedores = [dict(r) for r in cur.fetchall()]

        # A loja tem ~2.200 peças: embutir todas na página levava meio mega de
        # HTML a cada abertura. O produto entra pelo SKU (é o que o leitor de
        # código de barras lê) ou pela busca por descrição, ambos sob demanda.
        cur.execute("""
            SELECT COUNT(*) AS total FROM pdv_produtos_filiais pf
            JOIN pdv_produtos p ON p.id_pdv_produto = pf.id_pdv_produto
            WHERE pf.cod_empresa = %s AND pf.cod_filial = %s
              AND pf.situacao = 'ATIVO' AND p.ativo
        """, (_empresa(), _cod_filial()))
        total_produtos = cur.fetchone()["total"]

        # só as marcas que esta loja trabalha — a lista da empresa inteira
        # traria marcas que ela não tem
        cur.execute("""
            SELECT DISTINCT p.marca
            FROM pdv_produtos_filiais pf
            JOIN pdv_produtos p ON p.id_pdv_produto = pf.id_pdv_produto
            WHERE pf.cod_empresa = %s AND pf.cod_filial = %s
              AND pf.situacao = 'ATIVO' AND p.ativo AND p.marca IS NOT NULL
            ORDER BY p.marca
        """, (_empresa(), _cod_filial()))
        marcas = [r["marca"] for r in cur.fetchall()]

        cur.execute(
            "SELECT id_pdv_conta_financeira, nome, caixa_padrao FROM pdv_contas_financeiras "
            "WHERE cod_empresa = %s AND ativo ORDER BY ordem, nome", (_empresa(),))
        contas = [dict(r) for r in cur.fetchall()]

        cur.execute(
            "SELECT id_pdv_operadora, nome, aceita_debito, aceita_credito "
            "FROM pdv_operadoras_cartao WHERE cod_empresa = %s AND ativo ORDER BY ordem, nome",
            (_empresa(),))
        operadoras = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()


    caixa_padrao = next((c for c in contas if c.get("caixa_padrao")), None)

    cur = _cursor()
    try:
        canais = canais_da_filial(cur, _empresa(), _cod_filial())
        padrao = canal_padrao(cur, _empresa(), _cod_filial())
    finally:
        cur.close()

    return render_template(
        "pdv/vender.html",
        canais=canais,
        id_canal_padrao=padrao["id_pdv_canal"] if padrao else None,
        nome_caixa_padrao=caixa_padrao["nome"] if caixa_padrao else None,
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_pdv"),
        clientes=clientes,
        vendedores=vendedores,
        total_produtos=total_produtos,
        marcas=marcas,
        contas=contas,
        operadoras=operadoras,
        formas=FORMAS_RECEBIMENTO,
    )


@pdv_bp.route("/api/vendas", methods=["POST"])
def api_concluir_venda():
    """
    Conclui a venda: grava cabeçalho, itens e formas de recebimento, e produz
    os efeitos de cada forma nos módulos de destino. Tudo numa transação só —
    uma venda pela metade (com nota a prazo sem venda, por exemplo) seria pior
    do que venda nenhuma.
    """
    erro = _erro_permissao("VENDER")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    itens = dados.get("itens") or []
    recebimentos = dados.get("recebimentos") or []

    if not itens:
        return jsonify({"ok": False, "erro": "A venda não tem itens."}), 400
    if not recebimentos:
        return jsonify({"ok": False, "erro": "Informe as formas de recebimento."}), 400

    id_vendedor = dados.get("id_pdv_vendedor")
    if not id_vendedor:
        return jsonify({"ok": False, "erro": "Informe o vendedor."}), 400

    desconto_venda = _num(dados.get("valor_desconto"))

    # A soma dos itens tem que dar o total, e a soma dos recebimentos também.
    # Só depois dessas duas validações a venda pode ser concluída.
    # As promoções são resolvidas no servidor: o preço que vale é o da
    # campanha vigente hoje, não o que a tela mandou. Assim uma tela aberta
    # desde ontem, com uma campanha que já terminou, não vende no preço velho.
    conn_promo = get_connection()
    cur_promo = conn_promo.cursor(cursor_factory=RealDictCursor)
    try:
        promocoes = promocoes_do_dia(cur_promo, _empresa())
    finally:
        cur_promo.close()

    total_itens = 0
    for item in itens:
        quantidade = _num(item.get("quantidade"))
        preco = _num(item.get("preco_unitario"))
        desconto = _num(item.get("valor_desconto"))
        if quantidade <= 0:
            return jsonify({"ok": False, "erro": "Item com quantidade zerada."}), 400
        promo = promocoes.get(item.get("id_pdv_produto"))
        item["_campanha"] = promo["id_pdv_campanha"] if promo else None

        item["_quantidade"] = quantidade
        item["_preco"] = preco
        item["_desconto"] = desconto
        item["_total"] = round(quantidade * preco - desconto, 2)
        total_itens += item["_total"]

    valor_bruto = round(total_itens, 2)
    valor_total = round(valor_bruto - desconto_venda, 2)
    if valor_total <= 0:
        return jsonify({"ok": False, "erro": "O valor total da venda tem que ser maior que zero."}), 400

    total_recebido = 0
    for receb in recebimentos:
        forma = (receb.get("forma") or "").strip().upper()
        if forma not in dict(FORMAS_RECEBIMENTO):
            return jsonify({"ok": False, "erro": f"Forma de recebimento inválida: {forma}."}), 400
        valor = _num(receb.get("valor"))
        if valor <= 0:
            return jsonify({"ok": False, "erro": "Forma de recebimento com valor zerado."}), 400
        receb["_forma"] = forma
        receb["_valor"] = round(valor, 2)
        total_recebido += receb["_valor"]

        # Dinheiro é dinheiro: cai na gaveta da loja, a vendedora não escolhe
        # nada. A conta vem do cadastro (a marcada como caixa padrão). PIX,
        # sim, precisa dizer em qual conta caiu.
        if forma == "DINHEIRO":
            receb["id_pdv_conta_financeira"] = None      # resolvido adiante
        elif forma == "PIX" and not receb.get("id_pdv_conta_financeira"):
            return jsonify({
                "ok": False,
                "erro": "Informe em qual conta caiu o PIX.",
            }), 400
        if forma in FORMAS_CARTAO and not receb.get("id_pdv_operadora"):
            return jsonify({"ok": False, "erro": "Informe a operadora do cartão."}), 400

    if _centavos(total_recebido) != _centavos(valor_total):
        return jsonify({
            "ok": False,
            "erro": (f"A soma das formas de recebimento (R$ {total_recebido:.2f}) não fecha "
                     f"com o total da venda (R$ {valor_total:.2f})."),
        }), 400

    id_cliente = dados.get("id_pdv_cliente") or None
    precisa_cliente = any(
        r["_forma"] in ("NOTA_PRAZO", "CONSIGNACAO") for r in recebimentos
    )
    if precisa_cliente and not id_cliente:
        return jsonify({
            "ok": False,
            "erro": "Nota a prazo e consignação exigem um cliente identificado.",
        }), 400

    cod_empresa = _empresa()
    cod_filial = _cod_filial()
    id_usuario = session.get("id_usuario")
    data_venda = date.today()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # a gaveta da loja, para as formas em dinheiro
        id_caixa_padrao = None
        if any(r["_forma"] == "DINHEIRO" for r in recebimentos):
            cur.execute("""
                SELECT id_pdv_conta_financeira FROM pdv_contas_financeiras
                WHERE cod_empresa = %s AND caixa_padrao AND ativo
            """, (cod_empresa,))
            linha = cur.fetchone()
            if not linha:
                return jsonify({
                    "ok": False,
                    "erro": ("Nenhuma conta está marcada como caixa da loja. "
                             "Marque uma em Cadastros → Contas Financeiras."),
                }), 400
            id_caixa_padrao = linha["id_pdv_conta_financeira"]

        nome_cliente = None
        if id_cliente:
            cur.execute(
                "SELECT nome FROM pdv_clientes WHERE id_pdv_cliente = %s AND cod_empresa = %s",
                (id_cliente, cod_empresa))
            linha = cur.fetchone()
            if not linha:
                return jsonify({"ok": False, "erro": "Cliente não encontrado."}), 400
            nome_cliente = linha["nome"]

        # a vendedora tem que ser desta loja: venda de uma filial não pode sair
        # no nome de quem trabalha na outra
        cur.execute("""
            SELECT nome FROM pdv_vendedores
            WHERE id_pdv_vendedor = %s AND cod_empresa = %s AND cod_filial = %s
        """, (id_vendedor, cod_empresa, cod_filial))
        linha = cur.fetchone()
        if not linha:
            return jsonify({
                "ok": False,
                "erro": "Vendedor não encontrado nesta loja.",
            }), 400
        nome_vendedor = linha["nome"]

        cur.execute(
            "SELECT COALESCE(MAX(numero_venda), 0) + 1 AS proximo FROM pdv_vendas "
            "WHERE cod_empresa = %s AND cod_filial = %s", (cod_empresa, cod_filial))
        numero_venda = cur.fetchone()["proximo"]

        # por onde a venda saiu: balcão, e-commerce, outlet
        id_canal = dados.get("id_pdv_canal") or None
        if id_canal:
            cur.execute("""
                SELECT 1 FROM pdv_canais_venda
                WHERE id_pdv_canal = %s AND cod_empresa = %s AND ativo
            """, (id_canal, cod_empresa))
            if not cur.fetchone():
                return jsonify({"ok": False, "erro": "Canal de venda inválido."}), 400

        cur.execute("""
            INSERT INTO pdv_vendas
                (cod_empresa, cod_filial, numero_venda, data_venda, hora_venda,
                 id_pdv_cliente, nome_cliente, id_pdv_vendedor, nome_vendedor,
                 valor_bruto, valor_desconto, valor_total, situacao, observacao,
                 id_usuario, id_pdv_canal)
            VALUES (%s, %s, %s, %s, now()::time, %s, %s, %s, %s, %s, %s, %s, 'CONCLUIDA',
                    %s, %s, %s)
            RETURNING id_pdv_venda
        """, (cod_empresa, cod_filial, numero_venda, data_venda,
              id_cliente, nome_cliente, id_vendedor, nome_vendedor,
              valor_bruto, desconto_venda, valor_total,
              (dados.get("observacao") or "").strip() or None, id_usuario, id_canal))
        id_venda = cur.fetchone()["id_pdv_venda"]

        movimentos_estoque = []
        for sequencia, item in enumerate(itens, start=1):
            movimentos_estoque.append({
                "id_pdv_produto": item.get("id_pdv_produto") or None,
                "quantidade": item["_quantidade"],
                "custo_unitario": _num(item.get("custo_unitario")),
                "descricao_produto": (item.get("descricao_produto") or "").strip(),
            })
            cur.execute("""
                INSERT INTO pdv_vendas_itens
                    (cod_empresa, id_pdv_venda, sequencia, id_pdv_produto, descricao_produto,
                     unidade, quantidade, preco_unitario, valor_desconto, valor_total,
                     custo_unitario, id_pdv_campanha)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (cod_empresa, id_venda, sequencia, item.get("id_pdv_produto") or None,
                  (item.get("descricao_produto") or "").strip() or "Item",
                  (item.get("unidade") or "UN").strip(),
                  item["_quantidade"], item["_preco"], item["_desconto"], item["_total"],
                  _num(item.get("custo_unitario")), item["_campanha"]))

        # Itens da Venda → Movimentações de Estoque
        baixar_itens_da_venda(cur, cod_empresa, cod_filial, id_venda, data_venda,
                              movimentos_estoque, id_usuario, id_canal)

        for sequencia, receb in enumerate(recebimentos, start=1):
            forma = receb["_forma"]
            valor = receb["_valor"]
            id_conta = receb.get("id_pdv_conta_financeira") or None
            if forma == "DINHEIRO":
                id_conta = id_caixa_padrao
            id_operadora = receb.get("id_pdv_operadora") or None
            qtd_parcelas = int(_num(receb.get("qtd_parcelas"), 1)) or 1
            if forma != "CREDITO":
                qtd_parcelas = 1

            cur.execute("""
                INSERT INTO pdv_vendas_recebimentos
                    (cod_empresa, id_pdv_venda, sequencia, forma, valor,
                     id_pdv_conta_financeira, id_pdv_operadora, qtd_parcelas, nsu)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id_pdv_venda_recebimento
            """, (cod_empresa, id_venda, sequencia, forma, valor,
                  id_conta, id_operadora, qtd_parcelas,
                  (receb.get("nsu") or "").strip() or None))
            id_receb = cur.fetchone()["id_pdv_venda_recebimento"]

            _desaguar_recebimento(
                cur, cod_empresa, cod_filial, id_venda, id_receb, numero_venda,
                data_venda, forma, valor, id_conta, id_operadora, qtd_parcelas,
                id_cliente, nome_cliente, id_vendedor, receb.get("nsu"), id_usuario,
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao concluir a venda: {e}"}), 500
    finally:
        cur.close()

    return jsonify({"ok": True, "id_pdv_venda": id_venda, "numero_venda": numero_venda,
                    "valor_total": valor_total})


def _desaguar_recebimento(cur, cod_empresa, cod_filial, id_venda, id_receb, numero_venda,
                          data_venda, forma, valor, id_conta, id_operadora, qtd_parcelas,
                          id_cliente, nome_cliente, id_vendedor, nsu, id_usuario):
    """
    Produz, para uma forma de recebimento, o efeito dela no módulo de destino.
    Roda dentro da transação da venda.
    """
    if forma in FORMAS_EM_ESPECIE:
        # entrou dinheiro agora: vai direto para o extrato da conta
        cur.execute("""
            INSERT INTO pdv_lancamentos_financeiros
                (cod_empresa, id_pdv_conta_financeira, data_lancamento, valor,
                 historico, tipo_origem, id_origem, id_usuario)
            VALUES (%s, %s, %s, %s, %s, 'VENDA', %s, %s)
        """, (cod_empresa, id_conta, data_venda, valor,
              f"Venda nº {numero_venda} — {forma.title()}", id_venda, id_usuario))
        return

    if forma in FORMAS_CARTAO:
        cur.execute("""
            INSERT INTO pdv_cartoes_recebimentos
                (cod_empresa, cod_filial, id_pdv_venda, id_pdv_venda_recebimento,
                 id_pdv_operadora, modalidade, data_venda, valor_bruto, qtd_parcelas, nsu)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id_pdv_cartao_recebimento
        """, (cod_empresa, cod_filial, id_venda, id_receb, id_operadora, forma,
              data_venda, valor, qtd_parcelas, (nsu or "").strip() or None))
        id_cartao = cur.fetchone()["id_pdv_cartao_recebimento"]

        cur.execute("""
            SELECT dias_credito_debito, dias_credito_credito
            FROM pdv_operadoras_cartao WHERE id_pdv_operadora = %s
        """, (id_operadora,))
        operadora = cur.fetchone() or {}
        if forma == "DEBITO":
            dias_primeira, intervalo = int(operadora.get("dias_credito_debito") or 1), 30
        else:
            dias_primeira, intervalo = int(operadora.get("dias_credito_credito") or 30), 30

        # o detalhe existe mesmo com antecipação de recebíveis: é ele que
        # responde depois "essa venda foi em quantas vezes"
        centavos = int(round(valor * 100))
        base = centavos // qtd_parcelas
        for parcela in range(1, qtd_parcelas + 1):
            # a diferença de arredondamento vai toda na última parcela, para a
            # soma das parcelas fechar com o valor do cartão
            valor_parcela = base if parcela < qtd_parcelas else centavos - base * (qtd_parcelas - 1)
            cur.execute("""
                INSERT INTO pdv_cartoes_parcelas
                    (cod_empresa, id_pdv_cartao_recebimento, numero_parcela, valor,
                     previsao_credito, situacao)
                VALUES (%s, %s, %s, %s, %s + %s::int, 'A_RECEBER')
            """, (cod_empresa, id_cartao, parcela, valor_parcela / 100.0,
                  data_venda, dias_primeira + intervalo * (parcela - 1)))
        return

    if forma == "NOTA_PRAZO":
        # a venda fecha; a pendência passa a ser do Financeiro
        cur.execute("""
            INSERT INTO pdv_notas_prazo
                (cod_empresa, cod_filial, id_pdv_venda, id_pdv_venda_recebimento,
                 id_pdv_cliente, nome_cliente, id_pdv_vendedor, data_emissao, valor, situacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'ABERTA')
        """, (cod_empresa, cod_filial, id_venda, id_receb, id_cliente, nome_cliente,
              id_vendedor, data_venda, valor))
        return

    if forma == "CONSIGNACAO":
        cur.execute("""
            INSERT INTO pdv_consignacoes
                (cod_empresa, cod_filial, id_pdv_venda, id_pdv_venda_recebimento,
                 id_pdv_cliente, nome_cliente, data_envio, valor, situacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'ABERTA')
        """, (cod_empresa, cod_filial, id_venda, id_receb, id_cliente, nome_cliente,
              data_venda, valor))
        return


# ─── CONSULTA DE VENDAS ──────────────────────────────────────────────────────

@pdv_bp.route("/vendas")
def consultar_vendas():
    redir = _checar_acesso("CONSULTAR_VENDAS")
    if redir:
        return redir

    hoje = date.today().isoformat()
    data_de = (request.args.get("data_de") or hoje).strip()
    data_ate = (request.args.get("data_ate") or hoje).strip()

    cur = _cursor()
    try:
        cur.execute("""
            SELECT v.id_pdv_venda, v.numero_venda, v.data_venda, v.hora_venda,
                   v.nome_cliente, v.nome_vendedor, v.valor_total, v.situacao,
                   string_agg(DISTINCT r.forma, ', ' ORDER BY r.forma) AS formas
            FROM pdv_vendas v
            LEFT JOIN pdv_vendas_recebimentos r ON r.id_pdv_venda = v.id_pdv_venda
            WHERE v.cod_empresa = %s AND v.data_venda BETWEEN %s AND %s
            GROUP BY v.id_pdv_venda
            ORDER BY v.data_venda DESC, v.numero_venda DESC
        """, (_empresa(), data_de, data_ate))
        vendas = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    total = sum(float(v["valor_total"] or 0) for v in vendas if v["situacao"] == "CONCLUIDA")

    return render_template(
        "pdv/consultar_vendas.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_pdv"),
        vendas=vendas,
        data_de=data_de,
        data_ate=data_ate,
        total=total,
        rotulos_forma=dict(FORMAS_RECEBIMENTO),
    )


@pdv_bp.route("/vendas/<int:id_venda>")
def detalhe_venda(id_venda):
    redir = _checar_acesso("CONSULTAR_VENDAS")
    if redir:
        return redir

    cur = _cursor()
    try:
        cur.execute("""
            SELECT * FROM pdv_vendas
            WHERE id_pdv_venda = %s AND cod_empresa = %s
        """, (id_venda, _empresa()))
        venda = cur.fetchone()
        if not venda:
            flash("Venda não encontrada.", "error")
            return redirect(url_for("pdv.consultar_vendas"))
        venda = dict(venda)

        cur.execute("""
            SELECT * FROM pdv_vendas_itens
            WHERE id_pdv_venda = %s ORDER BY sequencia
        """, (id_venda,))
        itens = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT r.*, c.nome AS nome_conta, o.nome AS nome_operadora
            FROM pdv_vendas_recebimentos r
            LEFT JOIN pdv_contas_financeiras c
                   ON c.id_pdv_conta_financeira = r.id_pdv_conta_financeira
            LEFT JOIN pdv_operadoras_cartao o
                   ON o.id_pdv_operadora = r.id_pdv_operadora
            WHERE r.id_pdv_venda = %s ORDER BY r.sequencia
        """, (id_venda,))
        recebimentos = [dict(r) for r in cur.fetchall()]

        # rastreabilidade: o que essa venda produziu nos outros módulos
        cur.execute("""
            SELECT p.numero_parcela, p.valor, p.previsao_credito, p.situacao,
                   cr.modalidade, o.nome AS nome_operadora
            FROM pdv_cartoes_parcelas p
            JOIN pdv_cartoes_recebimentos cr
              ON cr.id_pdv_cartao_recebimento = p.id_pdv_cartao_recebimento
            LEFT JOIN pdv_operadoras_cartao o ON o.id_pdv_operadora = cr.id_pdv_operadora
            WHERE cr.id_pdv_venda = %s
            ORDER BY cr.id_pdv_cartao_recebimento, p.numero_parcela
        """, (id_venda,))
        parcelas_cartao = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT l.data_lancamento, l.valor, l.historico, c.nome AS nome_conta
            FROM pdv_lancamentos_financeiros l
            LEFT JOIN pdv_contas_financeiras c
                   ON c.id_pdv_conta_financeira = l.id_pdv_conta_financeira
            WHERE l.cod_empresa = %s AND l.tipo_origem = 'VENDA' AND l.id_origem = %s
            ORDER BY l.id_pdv_lancamento
        """, (_empresa(), id_venda))
        lancamentos = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT valor, valor_baixado, situacao, data_emissao
            FROM pdv_notas_prazo WHERE id_pdv_venda = %s
        """, (id_venda,))
        notas = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT valor, valor_conciliado, situacao, data_envio
            FROM pdv_consignacoes WHERE id_pdv_venda = %s
        """, (id_venda,))
        consignacoes = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    return render_template(
        "pdv/detalhe_venda.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.consultar_vendas"),
        venda=venda,
        itens=itens,
        recebimentos=recebimentos,
        parcelas_cartao=parcelas_cartao,
        lancamentos=lancamentos,
        notas=notas,
        consignacoes=consignacoes,
        rotulos_forma=dict(FORMAS_RECEBIMENTO),
        pode_devolver=pode("DEVOLUCOES"),
    )


# ─── ESTOQUE ─────────────────────────────────────────────────────────────────

@pdv_bp.route("/estoque")
def consultar_estoque():
    """Posição de todos os produtos: quanto tem e quanto vale."""
    redir = _checar_acesso("ESTOQUE")
    if redir:
        return redir

    cur = _cursor()
    try:
        # A posição é da LOJA: só os itens que esta filial trabalha, com o
        # saldo dela. Disponível e "abaixo do mínimo" são calculados aqui,
        # nunca gravados.
        cur.execute("""
            SELECT p.id_pdv_produto, p.sku, p.descricao, p.unidade, p.ativo,
                   p.custo_atual, p.preco_venda,
                   pf.quantidade_atual, pf.quantidade_reservada,
                   pf.estoque_minimo, pf.estoque_maximo, pf.situacao,
                   pf.ultimo_movimento_em,
                   pf.quantidade_atual - pf.quantidade_reservada AS disponivel,
                   pf.quantidade_atual * p.custo_atual AS valor_custo
            FROM pdv_produtos_filiais pf
            JOIN pdv_produtos p ON p.id_pdv_produto = pf.id_pdv_produto
            WHERE pf.cod_empresa = %s AND pf.cod_filial = %s
              AND (pf.situacao = 'ATIVO' OR %s)
            ORDER BY p.ordem, p.descricao
        """, (_empresa(), _cod_filial(),
              (request.args.get("mostrar_ocultos") or "") == "1"))
        produtos = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    total_custo = sum(float(p["valor_custo"] or 0) for p in produtos)

    cur = _cursor()
    try:
        canais = canais_da_filial(cur, _empresa(), _cod_filial())
        por_canal = estoque_por_canal(cur, _empresa())
    finally:
        cur.close()

    return render_template(
        "pdv/estoque.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_pdv"),
        produtos=produtos,
        total_custo=total_custo,
        pode_ajustar=pode("AJUSTE_ESTOQUE"),
        pode_importar=pode("IMPORTAR_ESTOQUE"),
        pode_produtos_filial=pode("PRODUTOS_FILIAL"),
        mostrar_ocultos=(request.args.get("mostrar_ocultos") or "") == "1",
        canais=canais,
        estoque_por_canal=por_canal,
    )


@pdv_bp.route("/estoque/<int:id_produto>")
def extrato_estoque(id_produto):
    """O extrato do produto: saldo inicial + entradas − saídas = saldo final."""
    redir = _checar_acesso("ESTOQUE")
    if redir:
        return redir

    hoje = date.today()
    data_de = (request.args.get("data_de") or hoje.replace(day=1).isoformat()).strip()
    data_ate = (request.args.get("data_ate") or hoje.isoformat()).strip()

    cur = _cursor()
    try:
        cur.execute("""
            SELECT p.id_pdv_produto, p.sku, p.descricao, p.unidade, p.custo_atual,
                   COALESCE(pf.quantidade_atual, 0) AS quantidade_atual
            FROM pdv_produtos p
            LEFT JOIN pdv_produtos_filiais pf
                   ON pf.id_pdv_produto = p.id_pdv_produto
                  AND pf.cod_empresa = p.cod_empresa AND pf.cod_filial = %s
            WHERE p.id_pdv_produto = %s AND p.cod_empresa = %s
        """, (_cod_filial(), id_produto, _empresa()))
        produto = cur.fetchone()
        if not produto:
            flash("Produto não encontrado.", "error")
            return redirect(url_for("pdv.consultar_estoque"))

        extrato = extrato_produto(cur, _empresa(), id_produto, data_de, data_ate)
    finally:
        cur.close()

    return render_template(
        "pdv/estoque_extrato.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.consultar_estoque"),
        produto=dict(produto),
        extrato=extrato,
        data_de=data_de,
        data_ate=data_ate,
        tipos=TIPOS_MOVIMENTO,
        pode_ajustar=pode("AJUSTE_ESTOQUE"),
    )


# Ajuste manual: a única porta pela qual o estoque entra enquanto a Entrada de
# Mercadorias não existe. Continua valendo depois, para perda, quebra e
# acerto de inventário.
TIPOS_AJUSTE = ("ENTRADA", "AJUSTE", "PERDA", "DEVOLUCAO")


@pdv_bp.route("/estoque/ajustar", methods=["POST"])
def ajustar_estoque():
    erro = _erro_permissao("AJUSTE_ESTOQUE")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    id_produto = dados.get("id_pdv_produto")
    tipo = (dados.get("tipo") or "").strip().upper()
    quantidade = _num(dados.get("quantidade"))

    if not id_produto:
        return jsonify({"ok": False, "erro": "Informe o produto."}), 400
    if tipo not in TIPOS_AJUSTE:
        return jsonify({"ok": False, "erro": "Tipo de ajuste inválido."}), 400
    if not quantidade:
        return jsonify({"ok": False, "erro": "Informe uma quantidade diferente de zero."}), 400

    # Perda sempre tira; entrada sempre põe. No AJUSTE o sinal é de quem
    # digita — é ele que serve para acertar inventário nos dois sentidos.
    if tipo == "PERDA":
        quantidade = -abs(quantidade)
    elif tipo in ("ENTRADA", "DEVOLUCAO"):
        quantidade = abs(quantidade)

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT custo_atual FROM pdv_produtos
            WHERE id_pdv_produto = %s AND cod_empresa = %s
        """, (id_produto, _empresa()))
        produto = cur.fetchone()
        if not produto:
            return jsonify({"ok": False, "erro": "Produto não encontrado."}), 404

        custo = _num(dados.get("custo_unitario")) or float(produto["custo_atual"] or 0)

        # com estoque por canal ligado, o movimento precisa dizer a que canal
        # pertence — senão o saldo fica num limbo "sem canal"
        id_canal = dados.get("id_pdv_canal") or None
        if estoque_por_canal(cur, _empresa()) and not id_canal:
            return jsonify({
                "ok": False,
                "erro": "Informe o canal: esta empresa controla estoque separado por canal.",
            }), 400

        movimentar(
            cur, _empresa(), _cod_filial(), id_produto, date.today(), tipo,
            quantidade, custo_unitario=custo, tipo_origem="AJUSTE",
            historico=(dados.get("historico") or "").strip() or TIPOS_MOVIMENTO.get(tipo),
            id_usuario=session.get("id_usuario"), id_canal=id_canal,
        )

        # entrada manual atualiza o custo do produto (parâmetro do sistema é
        # Último Preço de Compra)
        if quantidade > 0 and _num(dados.get("custo_unitario")) > 0:
            cur.execute("""
                UPDATE pdv_produtos
                   SET custo_atual = %s, ultimo_preco_compra = %s, atualizado_em = now()
                 WHERE id_pdv_produto = %s AND cod_empresa = %s
            """, (custo, custo, id_produto, _empresa()))

        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao ajustar: {e}"}), 500
    finally:
        cur.close()

    return jsonify({"ok": True})


# ─── ENTRADA DE MERCADORIAS ──────────────────────────────────────────────────

@pdv_bp.route("/entradas/nova")
def nova_entrada():
    redir = _checar_acesso("ENTRADA_MERCADORIAS")
    if redir:
        return redir

    cur = _cursor()
    try:
        cur.execute(
            "SELECT id_pdv_fornecedor, nome FROM pdv_fornecedores "
            "WHERE cod_empresa = %s AND ativo ORDER BY ordem, nome", (_empresa(),))
        fornecedores = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT id_pdv_cte, numero, serie, transportadora, valor_total, data_emissao
            FROM pdv_ctes
            WHERE cod_empresa = %s AND ativo
            ORDER BY data_emissao DESC NULLS LAST, id_pdv_cte DESC
        """, (_empresa(),))
        ctes = [dict(r) for r in cur.fetchall()]

        # a entrada enxerga o cadastro central inteiro: é justamente por ela
        # que uma peça nova passa a existir na loja
        cur.execute(
            "SELECT id_pdv_produto, sku, descricao, unidade, custo_atual "
            "FROM pdv_produtos WHERE cod_empresa = %s AND ativo ORDER BY ordem, descricao",
            (_empresa(),))
        produtos = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    for cte in ctes:
        cte["valor_total"] = float(cte["valor_total"] or 0)
        cte["data_emissao"] = cte["data_emissao"].isoformat() if cte["data_emissao"] else None
    for p in produtos:
        p["custo_atual"] = float(p["custo_atual"] or 0)

    return render_template(
        "pdv/entrada_mercadoria.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.consultar_entradas"),
        fornecedores=fornecedores,
        ctes=ctes,
        produtos=produtos,
        hoje=date.today().isoformat(),
    )


@pdv_bp.route("/api/entradas", methods=["POST"])
def api_concluir_entrada():
    """
    Conclui a nota de entrada. Numa transação só:
      1. grava cabeçalho e itens
      2. rateia o frete do CT-e vinculado entre os itens
      3. dá entrada no estoque com o custo já com frete
      4. atualiza o último preço de compra do produto
      5. gera os títulos a pagar
    Nada aqui toca o fluxo de caixa: comprar não é pagar.
    """
    erro = _erro_permissao("ENTRADA_MERCADORIAS")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    itens = dados.get("itens") or []
    parcelas_informadas = dados.get("parcelas") or []

    if not itens:
        return jsonify({"ok": False, "erro": "A nota não tem itens."}), 400

    numero = (dados.get("numero") or "").strip()
    if not numero:
        return jsonify({"ok": False, "erro": "Informe o número da nota fiscal."}), 400

    id_fornecedor = dados.get("id_pdv_fornecedor")
    if not id_fornecedor:
        return jsonify({"ok": False, "erro": "Informe o fornecedor."}), 400

    data_entrada = (dados.get("data_entrada") or date.today().isoformat()).strip()

    total_produtos = 0.0
    for item in itens:
        quantidade = _num(item.get("quantidade"))
        preco = _num(item.get("preco_unitario"))
        desconto = _num(item.get("valor_desconto"))
        if quantidade <= 0:
            return jsonify({"ok": False, "erro": "Item com quantidade zerada."}), 400
        if not item.get("id_pdv_produto"):
            return jsonify({
                "ok": False,
                "erro": "Todo item da nota precisa apontar para um produto cadastrado.",
            }), 400
        item["_quantidade"] = quantidade
        item["_preco"] = preco
        item["_desconto"] = desconto
        item["_total"] = round(quantidade * preco - desconto, 2)
        total_produtos += item["_total"]

    total_produtos = round(total_produtos, 2)
    desconto_nota = _num(dados.get("valor_desconto"))

    # o frete vem do CT-e vinculado — é ele que faz o frete virar custo
    id_cte = dados.get("id_pdv_cte") or None
    valor_frete = 0.0

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if id_cte:
            cur.execute("""
                SELECT valor_total FROM pdv_ctes
                WHERE id_pdv_cte = %s AND cod_empresa = %s
            """, (id_cte, _empresa()))
            linha = cur.fetchone()
            if not linha:
                return jsonify({"ok": False, "erro": "CT-e não encontrado."}), 400
            valor_frete = float(linha["valor_total"] or 0)

        valor_total = round(total_produtos - desconto_nota + valor_frete, 2)

        # as parcelas têm que fechar com o total da nota
        parcelas = []
        for parcela in parcelas_informadas:
            valor = _num(parcela.get("valor"))
            vencimento = (parcela.get("data_vencimento") or "").strip()
            if valor <= 0 or not vencimento:
                continue
            parcelas.append({"valor": round(valor, 2), "data_vencimento": vencimento})

        if parcelas:
            soma = round(sum(p["valor"] for p in parcelas), 2)
            if _centavos(soma) != _centavos(valor_total):
                return jsonify({
                    "ok": False,
                    "erro": (f"A soma das parcelas (R$ {soma:.2f}) não fecha com o total "
                             f"da nota (R$ {valor_total:.2f})."),
                }), 400

        cur.execute(
            "SELECT nome FROM pdv_fornecedores WHERE id_pdv_fornecedor = %s AND cod_empresa = %s",
            (id_fornecedor, _empresa()))
        linha = cur.fetchone()
        if not linha:
            return jsonify({"ok": False, "erro": "Fornecedor não encontrado."}), 400
        nome_fornecedor = linha["nome"]

        cur.execute("""
            INSERT INTO pdv_notas_entrada
                (cod_empresa, cod_filial, id_pdv_fornecedor, nome_fornecedor, numero, serie,
                 chave_nfe, data_emissao, data_entrada, id_pdv_cte, valor_produtos,
                 valor_desconto, valor_frete, valor_total, condicao_pagamento,
                 situacao, observacao, id_usuario)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    'CONCLUIDA', %s, %s)
            RETURNING id_pdv_nota_entrada
        """, (_empresa(), _cod_filial(), id_fornecedor, nome_fornecedor, numero,
              (dados.get("serie") or "").strip() or None,
              (dados.get("chave_nfe") or "").strip() or None,
              (dados.get("data_emissao") or "").strip() or None,
              data_entrada, id_cte, total_produtos, desconto_nota, valor_frete, valor_total,
              (dados.get("condicao_pagamento") or "").strip() or None,
              (dados.get("observacao") or "").strip() or None,
              session.get("id_usuario")))
        id_nota = cur.fetchone()["id_pdv_nota_entrada"]

        # frete rateado proporcionalmente ao valor de cada item
        ratear_frete(itens, valor_frete)

        for sequencia, item in enumerate(itens, start=1):
            custo = custo_unitario(item)
            cur.execute("""
                INSERT INTO pdv_notas_entrada_itens
                    (cod_empresa, id_pdv_nota_entrada, sequencia, id_pdv_produto,
                     descricao_produto, unidade, quantidade, preco_unitario,
                     valor_desconto, valor_total, valor_frete_rateado, custo_unitario)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (_empresa(), id_nota, sequencia, item["id_pdv_produto"],
                  (item.get("descricao_produto") or "").strip() or "Item",
                  (item.get("unidade") or "UN").strip(),
                  item["_quantidade"], item["_preco"], item["_desconto"],
                  item["_total"], item["_frete"], custo))

            # entrada física no estoque, com o custo já incorporando o frete
            movimentar(
                cur, _empresa(), _cod_filial(), item["id_pdv_produto"], data_entrada,
                tipo="ENTRADA", quantidade=item["_quantidade"], custo_unitario=custo,
                tipo_origem="NOTA_ENTRADA", id_origem=id_nota,
                historico=f"NF {numero} — {nome_fornecedor}",
                id_usuario=session.get("id_usuario"),
            )

            # parâmetro do sistema: Último Preço de Compra
            cur.execute("""
                UPDATE pdv_produtos
                   SET custo_atual = %s, ultimo_preco_compra = %s, atualizado_em = now()
                 WHERE id_pdv_produto = %s AND cod_empresa = %s
            """, (custo, custo, item["id_pdv_produto"], _empresa()))

        if parcelas:
            gerar_titulos_pagar(cur, _empresa(), id_nota, id_fornecedor, nome_fornecedor,
                                parcelas, documento=f"NF {numero}")

        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao concluir a entrada: {e}"}), 500
    finally:
        cur.close()

    return jsonify({"ok": True, "id_pdv_nota_entrada": id_nota, "valor_total": valor_total,
                    "parcelas": len(parcelas)})


@pdv_bp.route("/api/entradas/parcelar", methods=["POST"])
def api_parcelar():
    """Sugestão de parcelamento para a tela — nada é gravado aqui."""
    erro = _erro_permissao("ENTRADA_MERCADORIAS")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    try:
        primeira = datetime.strptime(
            (dados.get("primeiro_vencimento") or date.today().isoformat()).strip(), "%Y-%m-%d"
        ).date()
    except ValueError:
        primeira = date.today()

    return jsonify({"ok": True, "parcelas": parcelar(
        _num(dados.get("valor_total")),
        int(_num(dados.get("quantidade"), 1)) or 1,
        primeira,
        int(_num(dados.get("intervalo_dias"), 30)) or 30,
    )})


@pdv_bp.route("/entradas")
def consultar_entradas():
    redir = _checar_acesso("ENTRADA_MERCADORIAS")
    if redir:
        return redir

    hoje = date.today()
    data_de = (request.args.get("data_de") or hoje.replace(day=1).isoformat()).strip()
    data_ate = (request.args.get("data_ate") or hoje.isoformat()).strip()

    cur = _cursor()
    try:
        cur.execute("""
            SELECT n.id_pdv_nota_entrada, n.numero, n.serie, n.data_entrada,
                   n.nome_fornecedor, n.valor_produtos, n.valor_frete, n.valor_total,
                   n.situacao, COUNT(t.id_pdv_titulo_pagar) AS parcelas
            FROM pdv_notas_entrada n
            LEFT JOIN pdv_titulos_pagar t ON t.id_pdv_nota_entrada = n.id_pdv_nota_entrada
            WHERE n.cod_empresa = %s AND n.data_entrada BETWEEN %s AND %s
            GROUP BY n.id_pdv_nota_entrada
            ORDER BY n.data_entrada DESC, n.id_pdv_nota_entrada DESC
        """, (_empresa(), data_de, data_ate))
        notas = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    total = sum(float(n["valor_total"] or 0) for n in notas if n["situacao"] == "CONCLUIDA")

    return render_template(
        "pdv/entradas.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_pdv"),
        notas=notas,
        data_de=data_de,
        data_ate=data_ate,
        total=total,
    )


@pdv_bp.route("/entradas/<int:id_nota>")
def detalhe_entrada(id_nota):
    redir = _checar_acesso("ENTRADA_MERCADORIAS")
    if redir:
        return redir

    cur = _cursor()
    try:
        cur.execute("""
            SELECT n.*, c.numero AS numero_cte, c.transportadora
            FROM pdv_notas_entrada n
            LEFT JOIN pdv_ctes c ON c.id_pdv_cte = n.id_pdv_cte
            WHERE n.id_pdv_nota_entrada = %s AND n.cod_empresa = %s
        """, (id_nota, _empresa()))
        nota = cur.fetchone()
        if not nota:
            flash("Nota de entrada não encontrada.", "error")
            return redirect(url_for("pdv.consultar_entradas"))
        nota = dict(nota)

        cur.execute("""
            SELECT * FROM pdv_notas_entrada_itens
            WHERE id_pdv_nota_entrada = %s ORDER BY sequencia
        """, (id_nota,))
        itens = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT * FROM pdv_titulos_pagar
            WHERE id_pdv_nota_entrada = %s ORDER BY numero_parcela
        """, (id_nota,))
        titulos = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT m.data_movimento, m.quantidade, m.custo_unitario, p.descricao
            FROM pdv_estoque_movimentos m
            JOIN pdv_produtos p ON p.id_pdv_produto = m.id_pdv_produto
            WHERE m.cod_empresa = %s AND m.tipo_origem = 'NOTA_ENTRADA' AND m.id_origem = %s
            ORDER BY m.id_pdv_estoque_movimento
        """, (_empresa(), id_nota))
        movimentos = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    return render_template(
        "pdv/entrada_detalhe.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.consultar_entradas"),
        nota=nota,
        itens=itens,
        titulos=titulos,
        movimentos=movimentos,
    )


# ─── CONTAS A PAGAR ──────────────────────────────────────────────────────────

@pdv_bp.route("/contas-pagar")
def contas_pagar():
    """
    As obrigações da loja. A baixa (que gera a saída no fluxo de caixa) entra
    na fase do Financeiro, junto com a baixa das notas a prazo — as duas
    precisam do mesmo caminho até o lançamento.
    """
    redir = _checar_acesso("CONTAS_PAGAR")
    if redir:
        return redir

    hoje = date.today()
    situacao = (request.args.get("situacao") or "TODOS").strip().upper()
    # "Todos" nos meses é o padrão: quem abre a tela quer ver o que deve, não
    # um mês específico. O mês entra quando se pergunta "e novembro?".
    mes = str(request.args.get("mes") or "TODOS").strip().upper()
    ano = int(_num(request.args.get("ano"), hoje.year))

    condicoes = ["t.cod_empresa = %s", "EXTRACT(YEAR FROM t.data_vencimento) = %s"]
    parametros = [_empresa(), ano]
    if mes != "TODOS":
        condicoes.append("EXTRACT(MONTH FROM t.data_vencimento) = %s")
        parametros.append(int(_num(mes, hoje.month)))
    if situacao != "TODOS":
        condicoes.append("t.situacao = %s")
        parametros.append(situacao)

    cur = _cursor()
    try:
        cur.execute(f"""
            SELECT t.*, n.numero AS numero_nota, d.nome AS nome_tipo,
                   d.cod_grupo, d.cod_conta, cg.descricao AS nome_conta
            FROM pdv_titulos_pagar t
            LEFT JOIN pdv_notas_entrada n ON n.id_pdv_nota_entrada = t.id_pdv_nota_entrada
            LEFT JOIN pdv_despesas_tipos d ON d.id_pdv_despesa_tipo = t.id_pdv_despesa_tipo
            LEFT JOIN contas_gerenciais cg ON cg.cod_empresa = t.cod_empresa
                                          AND cg.cod_grupo = d.cod_grupo
                                          AND cg.cod_conta = d.cod_conta
            WHERE {' AND '.join(condicoes)}
            ORDER BY t.data_vencimento, t.id_pdv_titulo_pagar
        """, parametros)
        titulos = [dict(r) for r in cur.fetchall()]
        contas = _contas_ativas(cur)
    finally:
        cur.close()

    # Os três valores da tela: o que se deve, o que já se pagou e a soma do
    # que passou pelo filtro. São calculados aqui, nunca gravados.
    total_valor = sum(float(t["valor"] or 0) for t in titulos)
    total_baixado = sum(float(t["valor_baixado"] or 0) for t in titulos)
    total_aberto = total_valor - total_baixado
    vencidos = sum(1 for t in titulos
                   if t["situacao"] == "ABERTO" and t["data_vencimento"] < hoje)

    return render_template(
        "pdv/contas_pagar.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_titulos_pagar"),
        titulos=titulos,
        contas=contas,
        pode_pagar=pode("BAIXAR_PAGAR"),
        pode_caixa=pode("CAIXA"),
        hoje_iso=hoje.isoformat(),
        situacao=situacao,
        mes=mes,
        ano=ano,
        meses=list(enumerate(MESES, start=1)),
        anos=list(range(hoje.year - 2, hoje.year + 3)),
        total_valor=total_valor,
        total_baixado=total_baixado,
        total_aberto=total_aberto,
        vencidos=vencidos,
        hoje=hoje,
    )


@pdv_bp.route("/financeiro/titulos-pagar")
def menu_titulos_pagar():
    """Menu das obrigações: consultar os títulos ou vê-los distribuídos no tempo."""
    redir = _checar_acesso("CONTAS_PAGAR")
    if redir:
        return redir
    return render_template(
        "pdv/menu_titulos_pagar.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_financeiro"),
        pode_fluxo=pode("FLUXO_CAIXA_PAGAR"),
        pode_titulos_manuais=pode("TITULOS_MANUAIS"),
    )


@pdv_bp.route("/financeiro/titulos-pagar/por-grupo")
def titulos_pagar_por_grupo():
    """
    O Contas a Pagar visto de cima: só os totais de cada conta gerencial, com
    subtotal por grupo. Sem lista de títulos — para isso existe a Consulta.
    """
    redir = _checar_acesso("CONTAS_PAGAR")
    if redir:
        return redir

    hoje = date.today()
    mes = str(request.args.get("mes") or "TODOS").strip().upper()
    ano = int(_num(request.args.get("ano"), hoje.year))
    situacao = (request.args.get("situacao") or "TODOS").strip().upper()

    cur = _cursor()
    try:
        dados = totais_por_grupo_conta(cur, _empresa(), ano, mes, situacao)
    finally:
        cur.close()

    return render_template(
        "pdv/titulos_pagar_por_grupo.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_titulos_pagar"),
        dados=dados,
        mes=mes,
        ano=ano,
        situacao=situacao,
        meses=list(enumerate(MESES, start=1)),
        anos=list(range(hoje.year - 2, hoje.year + 3)),
    )


@pdv_bp.route("/financeiro/titulos-pagar/fluxo-caixa")
def fluxo_caixa_titulos_pagar():
    """
    Os títulos distribuídos no tempo: uma linha por compromisso, uma coluna
    por mês. As doze parcelas de "FOLHA" viram uma linha só — é assim que se
    lê um fluxo de caixa.
    """
    redir = _checar_acesso("FLUXO_CAIXA_PAGAR")
    if redir:
        return redir

    hoje = date.today()
    ano = int(_num(request.args.get("ano"), hoje.year))
    mes_inicial = int(_num(request.args.get("mes_inicial"), 1))

    cur = _cursor()
    try:
        dados = fluxo_caixa_pagar(cur, _empresa(), ano, mes_inicial=mes_inicial)
    finally:
        cur.close()

    return render_template(
        "pdv/fluxo_caixa_pagar.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_titulos_pagar"),
        dados=dados,
        ano=ano,
        mes_inicial=mes_inicial,
        anos=list(range(hoje.year - 2, hoje.year + 3)),
        nomes_meses=list(enumerate(MESES, start=1)),
    )


# ─── FINANCEIRO / FLUXO DE CAIXA ─────────────────────────────────────────────

def _contas_ativas(cur):
    cur.execute("""
        SELECT id_pdv_conta_financeira, nome, tipo, caixa_padrao
        FROM pdv_contas_financeiras
        WHERE cod_empresa = %s AND ativo
        ORDER BY ordem, nome
    """, (_empresa(),))
    return [dict(r) for r in cur.fetchall()]


@pdv_bp.route("/financeiro")
def menu_financeiro():
    redir = _checar_acesso("FINANCEIRO_MENU")
    if redir:
        return redir
    return render_template(
        "pdv/menu_financeiro.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_pdv"),
        pode_extrato=pode("EXTRATO"),
        pode_caixa_geral=pode("CAIXA_GERAL"),
        pode_lancamentos=pode("LANCAMENTOS"),
        pode_transferencias=pode("TRANSFERENCIAS"),
        pode_conciliacao=pode("CONCILIACAO"),
        pode_notas_prazo=pode("NOTAS_PRAZO"),
        pode_titulos=pode("TITULOS_RECEBER"),
        pode_pagar=pode("BAIXAR_PAGAR"),
        pode_titulos_manuais=pode("TITULOS_MANUAIS"),
        pode_orcamento=pode("ORCAMENTO_DESPESAS"),
        pode_fluxo_pagar=pode("FLUXO_CAIXA_PAGAR"),
    )


def _periodo_do_mes():
    hoje = date.today()
    return (
        (request.args.get("data_de") or hoje.replace(day=1).isoformat()).strip(),
        (request.args.get("data_ate") or hoje.isoformat()).strip(),
    )


@pdv_bp.route("/financeiro/caixa-geral")
def caixa_geral_pdv():
    """Posição consolidada de todas as contas financeiras."""
    redir = _checar_acesso("CAIXA_GERAL")
    if redir:
        return redir

    data_de, data_ate = _periodo_do_mes()
    cur = _cursor()
    try:
        dados = caixa_geral(cur, _empresa(), data_de, data_ate)
    finally:
        cur.close()

    return render_template(
        "pdv/caixa_geral.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_financeiro"),
        dados=dados,
        data_de=data_de,
        data_ate=data_ate,
    )


@pdv_bp.route("/financeiro/extrato")
@pdv_bp.route("/financeiro/extrato/<int:id_conta>")
def extrato_conta_pdv(id_conta=None):
    """Extrato de uma conta: saldo inicial + entradas − saídas = saldo final."""
    redir = _checar_acesso("EXTRATO")
    if redir:
        return redir

    data_de, data_ate = _periodo_do_mes()
    cur = _cursor()
    try:
        contas = _contas_ativas(cur)
        if not contas:
            flash("Cadastre ao menos uma conta financeira.", "error")
            return redirect(url_for("pdv.menu_financeiro"))

        if id_conta is None:
            id_conta = contas[0]["id_pdv_conta_financeira"]
        conta = next((c for c in contas
                      if c["id_pdv_conta_financeira"] == id_conta), None)
        if not conta:
            flash("Conta não encontrada.", "error")
            return redirect(url_for("pdv.extrato_conta_pdv"))

        extrato = extrato_conta(cur, _empresa(), id_conta, data_de, data_ate)
    finally:
        cur.close()

    return render_template(
        "pdv/extrato_conta.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_financeiro"),
        contas=contas,
        conta=conta,
        extrato=extrato,
        data_de=data_de,
        data_ate=data_ate,
        origens=ORIGENS_LANCAMENTO,
        pode_conciliar=pode("CONCILIACAO"),
    )


@pdv_bp.route("/financeiro/lancamentos", methods=["GET"])
def tela_lancamentos():
    """Lançamento manual: o que entra ou sai sem passar por venda ou título."""
    redir = _checar_acesso("LANCAMENTOS")
    if redir:
        return redir

    cur = _cursor()
    try:
        contas = _contas_ativas(cur)
    finally:
        cur.close()

    return render_template(
        "pdv/lancamentos.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_financeiro"),
        contas=contas,
        hoje=date.today().isoformat(),
        pode_transferir=pode("TRANSFERENCIAS"),
    )


@pdv_bp.route("/api/financeiro/lancamentos", methods=["POST"])
def api_lancar():
    erro = _erro_permissao("LANCAMENTOS")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    id_conta = dados.get("id_pdv_conta_financeira")
    valor = _num(dados.get("valor"))
    historico = (dados.get("historico") or "").strip()
    sentido = (dados.get("sentido") or "ENTRADA").strip().upper()

    if not id_conta:
        return jsonify({"ok": False, "erro": "Informe a conta financeira."}), 400
    if valor <= 0:
        return jsonify({"ok": False, "erro": "O valor tem que ser maior que zero."}), 400
    if not historico:
        return jsonify({"ok": False, "erro": "Informe o histórico."}), 400

    # a tela pergunta entrada/saída; no banco isso é o sinal do valor
    valor = abs(valor) if sentido == "ENTRADA" else -abs(valor)

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        lancar(cur, _empresa(), id_conta,
               (dados.get("data_lancamento") or date.today().isoformat()).strip(),
               valor, historico, tipo_origem="MANUAL",
               id_usuario=session.get("id_usuario"))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao lançar: {e}"}), 500
    finally:
        cur.close()
    return jsonify({"ok": True})


@pdv_bp.route("/api/financeiro/transferencias", methods=["POST"])
def api_transferir():
    erro = _erro_permissao("TRANSFERENCIAS")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        transferir(
            cur, _empresa(),
            dados.get("id_conta_origem"), dados.get("id_conta_destino"),
            (dados.get("data_movimento") or date.today().isoformat()).strip(),
            _num(dados.get("valor")),
            (dados.get("historico") or "").strip() or None,
            session.get("id_usuario"),
        )
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao transferir: {e}"}), 500
    finally:
        cur.close()
    return jsonify({"ok": True})


@pdv_bp.route("/api/financeiro/conciliar", methods=["POST"])
def api_conciliar():
    """
    Conciliação bancária: marca o lançamento como conferido contra o extrato
    do banco. Não altera valor nenhum — é conferência, não correção.
    """
    erro = _erro_permissao("CONCILIACAO")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    ids = dados.get("ids") or []
    if not ids:
        return jsonify({"ok": False, "erro": "Nenhum lançamento selecionado."}), 400

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE pdv_lancamentos_financeiros
               SET conciliado = %s, atualizado_em = now()
             WHERE cod_empresa = %s AND id_pdv_lancamento = ANY(%s)
        """, (bool(dados.get("conciliado")), _empresa(), list(ids)))
        conn.commit()
        alterados = cur.rowcount
    finally:
        cur.close()
    return jsonify({"ok": True, "alterados": alterados})


# ─── CONTAS A RECEBER ────────────────────────────────────────────────────────

@pdv_bp.route("/financeiro/notas-prazo")
def tela_notas_prazo():
    """Recebimento e baixa de notas a prazo, e conversão em títulos."""
    redir = _checar_acesso("NOTAS_PRAZO")
    if redir:
        return redir

    situacao = (request.args.get("situacao") or "ABERTA").strip().upper()
    filtro = "" if situacao == "TODAS" else " AND n.situacao = %s"
    parametros = [_empresa()] + ([] if situacao == "TODAS" else [situacao])

    cur = _cursor()
    try:
        cur.execute(f"""
            SELECT n.*, v.numero_venda, vd.nome AS nome_vendedor
            FROM pdv_notas_prazo n
            LEFT JOIN pdv_vendas v ON v.id_pdv_venda = n.id_pdv_venda
            LEFT JOIN pdv_vendedores vd ON vd.id_pdv_vendedor = n.id_pdv_vendedor
            WHERE n.cod_empresa = %s{filtro}
            ORDER BY n.data_emissao, n.id_pdv_nota_prazo
        """, parametros)
        notas = [dict(r) for r in cur.fetchall()]
        contas = _contas_ativas(cur)
    finally:
        cur.close()

    total_aberto = sum(float(n["valor"] or 0) - float(n["valor_baixado"] or 0)
                       for n in notas if n["situacao"] == "ABERTA")

    return render_template(
        "pdv/notas_prazo.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_financeiro"),
        notas=notas,
        contas=contas,
        situacao=situacao,
        total_aberto=total_aberto,
        hoje=date.today().isoformat(),
    )


@pdv_bp.route("/api/financeiro/notas-prazo/baixar", methods=["POST"])
def api_baixar_nota():
    erro = _erro_permissao("NOTAS_PRAZO")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        baixar_nota_prazo(
            cur, _empresa(), dados.get("id_pdv_nota_prazo"),
            dados.get("id_pdv_conta_financeira"),
            (dados.get("data_baixa") or date.today().isoformat()).strip(),
            round(_num(dados.get("valor")), 2),
            session.get("id_usuario"),
        )
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao baixar: {e}"}), 500
    finally:
        cur.close()
    return jsonify({"ok": True})


@pdv_bp.route("/api/financeiro/notas-prazo/converter", methods=["POST"])
def api_converter_nota():
    erro = _erro_permissao("NOTAS_PRAZO")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    parcelas = []
    for parcela in (dados.get("parcelas") or []):
        valor = _num(parcela.get("valor"))
        vencimento = (parcela.get("data_vencimento") or "").strip()
        if valor > 0 and vencimento:
            parcelas.append({"valor": round(valor, 2), "data_vencimento": vencimento})

    if not parcelas:
        return jsonify({"ok": False, "erro": "Informe ao menos uma parcela."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        converter_nota_em_titulos(cur, _empresa(), dados.get("id_pdv_nota_prazo"), parcelas)
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao converter: {e}"}), 500
    finally:
        cur.close()
    return jsonify({"ok": True})


@pdv_bp.route("/financeiro/titulos-receber")
def tela_titulos_receber():
    redir = _checar_acesso("TITULOS_RECEBER")
    if redir:
        return redir

    situacao = (request.args.get("situacao") or "ABERTO").strip().upper()
    filtro = "" if situacao == "TODOS" else " AND t.situacao = %s"
    parametros = [_empresa()] + ([] if situacao == "TODOS" else [situacao])

    cur = _cursor()
    try:
        cur.execute(f"""
            SELECT t.*, c.nome AS nome_cliente, n.id_pdv_venda
            FROM pdv_titulos_receber t
            LEFT JOIN pdv_clientes c ON c.id_pdv_cliente = t.id_pdv_cliente
            LEFT JOIN pdv_notas_prazo n ON n.id_pdv_nota_prazo = t.id_pdv_nota_prazo
            WHERE t.cod_empresa = %s{filtro}
            ORDER BY t.data_vencimento, t.id_pdv_titulo
        """, parametros)
        titulos = [dict(r) for r in cur.fetchall()]
        contas = _contas_ativas(cur)
    finally:
        cur.close()

    hoje = date.today()
    total_aberto = sum(float(t["valor"] or 0) - float(t["valor_baixado"] or 0)
                       for t in titulos if t["situacao"] == "ABERTO")
    vencidos = sum(1 for t in titulos
                   if t["situacao"] == "ABERTO" and t["data_vencimento"] < hoje)

    return render_template(
        "pdv/titulos_receber.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_financeiro"),
        titulos=titulos,
        contas=contas,
        situacao=situacao,
        total_aberto=total_aberto,
        vencidos=vencidos,
        hoje=hoje,
        hoje_iso=hoje.isoformat(),
    )


@pdv_bp.route("/api/financeiro/titulos-receber/baixar", methods=["POST"])
def api_baixar_titulo_receber():
    erro = _erro_permissao("TITULOS_RECEBER")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        baixar_titulo_receber(
            cur, _empresa(), dados.get("id_pdv_titulo"),
            dados.get("id_pdv_conta_financeira"),
            (dados.get("data_baixa") or date.today().isoformat()).strip(),
            round(_num(dados.get("valor")), 2),
            session.get("id_usuario"),
        )
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao baixar: {e}"}), 500
    finally:
        cur.close()
    return jsonify({"ok": True})


@pdv_bp.route("/api/financeiro/titulos-pagar/baixar", methods=["POST"])
def api_baixar_titulo_pagar():
    erro = _erro_permissao("BAIXAR_PAGAR")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        baixar_titulo_pagar(
            cur, _empresa(), dados.get("id_pdv_titulo_pagar"),
            dados.get("id_pdv_conta_financeira"),
            (dados.get("data_baixa") or date.today().isoformat()).strip(),
            round(_num(dados.get("valor")), 2),
            session.get("id_usuario"),
        )
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao pagar: {e}"}), 500
    finally:
        cur.close()
    return jsonify({"ok": True})


# ─── TÍTULOS MANUAIS E ORÇAMENTO DE DESPESAS ─────────────────────────────────
# A despesa que não passa por nota de entrada (luz, água, telefone, aluguel).
# Ela gera a mesma obrigação que a compra gera, então grava na mesma
# `pdv_titulos_pagar`, marcada pela coluna `origem` — ver o cabeçalho de
# `migrations/criar_pdv_titulos_manuais.sql`. O orçamento, esse sim, tem tabela
# própria: previsão não é obrigação e não pode aparecer em Contas a Pagar.


def _ano_mes():
    hoje = date.today()
    return (int(_num(request.args.get("ano"), hoje.year)),
            int(_num(request.args.get("mes"), hoje.month)))


@pdv_bp.route("/financeiro/titulos-manuais")
def tela_titulos_manuais():
    redir = _checar_acesso("TITULOS_MANUAIS")
    if redir:
        return redir

    ano, mes = _ano_mes()
    situacao = (request.args.get("situacao") or "TODOS").strip().upper()

    cur = _cursor()
    try:
        titulos = listar_titulos(cur, _empresa(), ano, mes, situacao)
        tipos = tipos_despesa(cur, _empresa())
        cur.execute("""
            SELECT id_pdv_fornecedor, nome FROM pdv_fornecedores
            WHERE cod_empresa = %s AND ativo ORDER BY nome
        """, (_empresa(),))
        fornecedores = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    hoje = date.today()
    return render_template(
        "pdv/titulos_manuais.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_financeiro"),
        titulos=titulos,
        tipos=tipos,
        fornecedores=fornecedores,
        ano=ano,
        mes=mes,
        meses=list(enumerate(MESES, start=1)),
        anos=list(range(hoje.year - 2, hoje.year + 3)),
        situacao=situacao,
        hoje=hoje,
        total=sum(float(t["valor"] or 0) for t in titulos),
        total_aberto=sum(float(t["valor"] or 0) - float(t["valor_baixado"] or 0)
                         for t in titulos if t["situacao"] == "ABERTO"),
    )


@pdv_bp.route("/api/financeiro/titulos-manuais", methods=["POST"])
def api_incluir_titulo_manual():
    erro = _erro_permissao("TITULOS_MANUAIS")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        quantidade = incluir_titulo(cur, _empresa(), {
            "id_pdv_despesa_tipo": dados.get("id_pdv_despesa_tipo") or None,
            "id_pdv_fornecedor": dados.get("id_pdv_fornecedor") or None,
            "descricao": dados.get("descricao"),
            "documento": dados.get("documento"),
            "valor": _num(dados.get("valor")),
            "data_vencimento": dados.get("data_vencimento"),
            "qtd_parcelas": int(_num(dados.get("qtd_parcelas"), 1)),
            "ano": dados.get("ano"),
            "mes": dados.get("mes"),
            "observacao": dados.get("observacao"),
        })
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao incluir: {e}"}), 500
    finally:
        cur.close()
    return jsonify({"ok": True, "titulos": quantidade})


@pdv_bp.route("/api/financeiro/titulos-manuais/<int:id_titulo>", methods=["DELETE"])
def api_excluir_titulo_manual(id_titulo):
    erro = _erro_permissao("TITULOS_MANUAIS")
    if erro:
        return erro

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        excluir_titulo(cur, _empresa(), id_titulo)
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao excluir: {e}"}), 500
    finally:
        cur.close()
    return jsonify({"ok": True})


@pdv_bp.route("/financeiro/orcamento")
def tela_orcamento_despesas():
    """Previsão de despesas do ano, por tipo. Nada aqui deve nada a ninguém."""
    redir = _checar_acesso("ORCAMENTO_DESPESAS")
    if redir:
        return redir

    ano = int(_num(request.args.get("ano"), date.today().year))
    cur = _cursor()
    try:
        dados = orcamento_do_ano(cur, _empresa(), ano)
    finally:
        cur.close()

    hoje = date.today()
    return render_template(
        "pdv/orcamento_despesas.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_financeiro"),
        dados=dados,
        ano=ano,
        anos=list(range(hoje.year - 2, hoje.year + 3)),
        meses=list(enumerate(MESES, start=1)),
        mes_atual=hoje.month,
    )


@pdv_bp.route("/api/financeiro/orcamento", methods=["POST"])
def api_salvar_orcamento():
    erro = _erro_permissao("ORCAMENTO_DESPESAS")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        id_tipo = dados.get("id_pdv_despesa_tipo")
        ano = int(_num(dados.get("ano"), date.today().year))
        mes = int(_num(dados.get("mes"), 1))
        valor = round(_num(dados.get("valor")), 2)
        if dados.get("replicar"):
            meses = replicar_previsao(cur, _empresa(), id_tipo, ano, mes, valor)
        else:
            salvar_previsao(cur, _empresa(), id_tipo, ano, mes, valor)
            meses = 1
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao gravar: {e}"}), 500
    finally:
        cur.close()
    return jsonify({"ok": True, "meses": meses})


@pdv_bp.route("/api/financeiro/orcamento/gerar-titulos", methods=["POST"])
def api_gerar_titulos_orcamento():
    """Transforma a previsão do mês em títulos a pagar. Rodar de novo não duplica."""
    erro = _erro_permissao("ORCAMENTO_DESPESAS")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        quantidade = gerar_titulos_do_mes(
            cur, _empresa(),
            int(_num(dados.get("ano"), date.today().year)),
            int(_num(dados.get("mes"), date.today().month)),
        )
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao gerar: {e}"}), 500
    finally:
        cur.close()
    return jsonify({"ok": True, "titulos": quantidade})


# ─── CAIXA CENTRAL DE VENDAS ─────────────────────────────────────────────────
# Conferência do dia pelo Financeiro. A venda continua fechada: aqui só se
# corrigem atributos COMPLEMENTARES do recebimento, e sempre com auditoria.

# Os únicos atributos corrigíveis. Valor, produto, cliente e vendedor não
# entram nesta lista de propósito — mexer neles seria reabrir a venda.
CAMPOS_CORRIGIVEIS = ("id_pdv_operadora", "qtd_parcelas")


@pdv_bp.route("/caixa-central")
def caixa_central():
    redir = _checar_acesso("CAIXA_CENTRAL")
    if redir:
        return redir

    data = (request.args.get("data") or date.today().isoformat()).strip()

    cur = _cursor()
    try:
        cur.execute("""
            SELECT id_pdv_venda, numero_venda, hora_venda, nome_cliente,
                   nome_vendedor, valor_total, situacao
            FROM pdv_vendas
            WHERE cod_empresa = %s AND data_venda = %s
            ORDER BY numero_venda
        """, (_empresa(), data))
        vendas = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT r.*, c.nome AS nome_conta, o.nome AS nome_operadora
            FROM pdv_vendas_recebimentos r
            JOIN pdv_vendas v ON v.id_pdv_venda = r.id_pdv_venda
            LEFT JOIN pdv_contas_financeiras c
                   ON c.id_pdv_conta_financeira = r.id_pdv_conta_financeira
            LEFT JOIN pdv_operadoras_cartao o ON o.id_pdv_operadora = r.id_pdv_operadora
            WHERE v.cod_empresa = %s AND v.data_venda = %s
            ORDER BY r.id_pdv_venda, r.sequencia
        """, (_empresa(), data))
        recebimentos = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT id_pdv_operadora, nome, aceita_debito, aceita_credito
            FROM pdv_operadoras_cartao
            WHERE cod_empresa = %s AND ativo ORDER BY ordem, nome
        """, (_empresa(),))
        operadoras = [dict(r) for r in cur.fetchall()]

        # as correções já feitas no dia, para a conferência ficar auditável
        cur.execute("""
            SELECT a.*, v.numero_venda
            FROM pdv_caixa_central_auditoria a
            JOIN pdv_vendas v ON v.id_pdv_venda = a.id_pdv_venda
            WHERE a.cod_empresa = %s AND v.data_venda = %s
            ORDER BY a.alterado_em DESC
        """, (_empresa(), data))
        auditoria = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    # amarra os recebimentos em cada venda
    por_venda = {}
    for r in recebimentos:
        por_venda.setdefault(r["id_pdv_venda"], []).append(r)
    for venda in vendas:
        venda["recebimentos"] = por_venda.get(venda["id_pdv_venda"], [])

    # totais por forma: é o que o Financeiro confere contra o caixa e as maquinetas
    totais_forma = {}
    for r in recebimentos:
        totais_forma[r["forma"]] = totais_forma.get(r["forma"], 0) + float(r["valor"] or 0)

    total_dia = sum(float(v["valor_total"] or 0) for v in vendas
                    if v["situacao"] == "CONCLUIDA")

    return render_template(
        "pdv/caixa_central.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_pdv"),
        data=data,
        vendas=vendas,
        operadoras=operadoras,
        auditoria=auditoria,
        totais_forma=totais_forma,
        total_dia=total_dia,
        rotulos_forma=dict(FORMAS_RECEBIMENTO),
        formas_cartao=FORMAS_CARTAO,
    )


@pdv_bp.route("/api/caixa-central/corrigir", methods=["POST"])
def api_corrigir_recebimento():
    """
    Corrige operadora ou quantidade de parcelas de um recebimento em cartão.

    O valor do recebimento **não muda** — só como ele está classificado. As
    parcelas de cartão são refeitas a partir do novo dado, porque elas são
    derivadas dele, e cada alteração grava uma linha de auditoria.
    """
    erro = _erro_permissao("CAIXA_CENTRAL")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    id_receb = dados.get("id_pdv_venda_recebimento")
    if not id_receb:
        return jsonify({"ok": False, "erro": "Recebimento não informado."}), 400

    alteracoes = {c: dados[c] for c in CAMPOS_CORRIGIVEIS if c in dados}
    if not alteracoes:
        return jsonify({"ok": False, "erro": "Nada a corrigir."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT r.*, v.data_venda, v.cod_filial, o.nome AS nome_operadora
            FROM pdv_vendas_recebimentos r
            JOIN pdv_vendas v ON v.id_pdv_venda = r.id_pdv_venda
            LEFT JOIN pdv_operadoras_cartao o ON o.id_pdv_operadora = r.id_pdv_operadora
            WHERE r.id_pdv_venda_recebimento = %s AND r.cod_empresa = %s
        """, (id_receb, _empresa()))
        receb = cur.fetchone()
        if not receb:
            return jsonify({"ok": False, "erro": "Recebimento não encontrado."}), 404
        if receb["forma"] not in FORMAS_CARTAO:
            return jsonify({
                "ok": False,
                "erro": "Só recebimentos em cartão têm operadora e parcelas a corrigir.",
            }), 400

        nova_operadora = receb["id_pdv_operadora"]
        novas_parcelas = receb["qtd_parcelas"]
        registros_auditoria = []

        if "id_pdv_operadora" in alteracoes:
            valor = alteracoes["id_pdv_operadora"]
            valor = int(valor) if valor else None
            if valor != receb["id_pdv_operadora"]:
                cur.execute("""
                    SELECT nome FROM pdv_operadoras_cartao
                    WHERE id_pdv_operadora = %s AND cod_empresa = %s
                """, (valor, _empresa()))
                linha = cur.fetchone()
                if not linha:
                    return jsonify({"ok": False, "erro": "Operadora não encontrada."}), 400
                registros_auditoria.append((
                    "id_pdv_operadora",
                    str(receb["id_pdv_operadora"] or ""), str(valor),
                    receb["nome_operadora"], linha["nome"],
                ))
                nova_operadora = valor

        if "qtd_parcelas" in alteracoes:
            # sem fallback silencioso: um 0 digitado por engano não pode virar
            # 1 e transformar uma venda em 6x numa venda à vista
            valor = int(_num(alteracoes["qtd_parcelas"], 0))
            if valor < 1:
                return jsonify({"ok": False, "erro": "Quantidade de parcelas inválida."}), 400
            if receb["forma"] == "DEBITO" and valor != 1:
                return jsonify({
                    "ok": False,
                    "erro": "Cartão de débito é sempre em uma parcela.",
                }), 400
            if valor != receb["qtd_parcelas"]:
                registros_auditoria.append((
                    "qtd_parcelas",
                    str(receb["qtd_parcelas"]), str(valor),
                    f"{receb['qtd_parcelas']}x", f"{valor}x",
                ))
                novas_parcelas = valor

        if not registros_auditoria:
            return jsonify({"ok": True, "alterado": False})

        cur.execute("""
            UPDATE pdv_vendas_recebimentos
               SET id_pdv_operadora = %s, qtd_parcelas = %s, atualizado_em = now()
             WHERE id_pdv_venda_recebimento = %s AND cod_empresa = %s
        """, (nova_operadora, novas_parcelas, id_receb, _empresa()))

        # As parcelas de cartão são derivadas da operadora e da quantidade:
        # mudou o dado, elas são refeitas. O valor total do recebimento é o
        # mesmo — o que muda é como ele se distribui no tempo.
        cur.execute("""
            SELECT id_pdv_cartao_recebimento FROM pdv_cartoes_recebimentos
            WHERE id_pdv_venda_recebimento = %s AND cod_empresa = %s
        """, (id_receb, _empresa()))
        cartao = cur.fetchone()

        if cartao:
            id_cartao = cartao["id_pdv_cartao_recebimento"]
            cur.execute("""
                UPDATE pdv_cartoes_recebimentos
                   SET id_pdv_operadora = %s, qtd_parcelas = %s, atualizado_em = now()
                 WHERE id_pdv_cartao_recebimento = %s
            """, (nova_operadora, novas_parcelas, id_cartao))

            cur.execute("DELETE FROM pdv_cartoes_parcelas WHERE id_pdv_cartao_recebimento = %s",
                        (id_cartao,))

            cur.execute("""
                SELECT dias_credito_debito, dias_credito_credito
                FROM pdv_operadoras_cartao WHERE id_pdv_operadora = %s
            """, (nova_operadora,))
            operadora = cur.fetchone() or {}
            if receb["forma"] == "DEBITO":
                dias_primeira = int(operadora.get("dias_credito_debito") or 1)
            else:
                dias_primeira = int(operadora.get("dias_credito_credito") or 30)

            centavos = int(round(float(receb["valor"] or 0) * 100))
            base = centavos // novas_parcelas
            for parcela in range(1, novas_parcelas + 1):
                valor_parcela = (base if parcela < novas_parcelas
                                 else centavos - base * (novas_parcelas - 1))
                cur.execute("""
                    INSERT INTO pdv_cartoes_parcelas
                        (cod_empresa, id_pdv_cartao_recebimento, numero_parcela, valor,
                         previsao_credito, situacao)
                    VALUES (%s, %s, %s, %s, %s + %s::int, 'A_RECEBER')
                """, (_empresa(), id_cartao, parcela, valor_parcela / 100.0,
                      receb["data_venda"], dias_primeira + 30 * (parcela - 1)))

        for campo, anterior, novo, desc_anterior, desc_novo in registros_auditoria:
            cur.execute("""
                INSERT INTO pdv_caixa_central_auditoria
                    (cod_empresa, id_pdv_venda, id_pdv_venda_recebimento, campo,
                     valor_anterior, valor_novo, descricao_anterior, descricao_nova,
                     id_usuario, nome_usuario)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (_empresa(), receb["id_pdv_venda"], id_receb, campo,
                  anterior, novo, desc_anterior, desc_novo,
                  session.get("id_usuario"), session.get("nome_usuario")))

        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao corrigir: {e}"}), 500
    finally:
        cur.close()

    return jsonify({"ok": True, "alterado": True})


@pdv_bp.route("/caixa-central/cartoes")
def gerencial_cartoes():
    """
    Informações gerenciais dos cartões.

    A quantidade de parcelas tem pouco efeito no caixa quando a empresa
    antecipa os recebíveis — mas tem muito efeito gerencial. É por isso que o
    detalhe das parcelas é preservado mesmo com antecipação: sem ele, nada
    disto aqui existiria.
    """
    redir = _checar_acesso("CAIXA_CENTRAL")
    if redir:
        return redir

    hoje = date.today()
    data_de = (request.args.get("data_de") or hoje.replace(day=1).isoformat()).strip()
    data_ate = (request.args.get("data_ate") or hoje.isoformat()).strip()

    cur = _cursor()
    try:
        # distribuição das vendas por quantidade de parcelas
        cur.execute("""
            SELECT cr.modalidade, cr.qtd_parcelas,
                   COUNT(*) AS operacoes,
                   SUM(cr.valor_bruto) AS valor
            FROM pdv_cartoes_recebimentos cr
            WHERE cr.cod_empresa = %s AND cr.data_venda BETWEEN %s AND %s
            GROUP BY cr.modalidade, cr.qtd_parcelas
            ORDER BY cr.modalidade, cr.qtd_parcelas
        """, (_empresa(), data_de, data_ate))
        distribuicao = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT o.nome, COUNT(*) AS operacoes, SUM(cr.valor_bruto) AS valor
            FROM pdv_cartoes_recebimentos cr
            LEFT JOIN pdv_operadoras_cartao o ON o.id_pdv_operadora = cr.id_pdv_operadora
            WHERE cr.cod_empresa = %s AND cr.data_venda BETWEEN %s AND %s
            GROUP BY o.nome
            ORDER BY SUM(cr.valor_bruto) DESC
        """, (_empresa(), data_de, data_ate))
        por_operadora = [dict(r) for r in cur.fetchall()]

        # prazo médio ponderado pelo valor: o prazo que a empresa teria se não
        # antecipasse os recebíveis
        cur.execute("""
            SELECT
                COALESCE(SUM(p.valor * (p.previsao_credito - cr.data_venda)), 0) AS valor_dias,
                COALESCE(SUM(p.valor), 0) AS valor
            FROM pdv_cartoes_parcelas p
            JOIN pdv_cartoes_recebimentos cr
              ON cr.id_pdv_cartao_recebimento = p.id_pdv_cartao_recebimento
            WHERE cr.cod_empresa = %s AND cr.data_venda BETWEEN %s AND %s
              AND p.previsao_credito IS NOT NULL
        """, (_empresa(), data_de, data_ate))
        prazo = cur.fetchone()
    finally:
        cur.close()

    total_valor = sum(float(d["valor"] or 0) for d in distribuicao)
    total_operacoes = sum(int(d["operacoes"] or 0) for d in distribuicao)

    for d in distribuicao:
        d["valor"] = float(d["valor"] or 0)
        d["percentual"] = (d["valor"] / total_valor * 100) if total_valor else 0
        d["ticket_medio"] = d["valor"] / d["operacoes"] if d["operacoes"] else 0

    valor_prazo = float(prazo["valor"] or 0)
    prazo_medio = (float(prazo["valor_dias"] or 0) / valor_prazo) if valor_prazo else 0

    return render_template(
        "pdv/gerencial_cartoes.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.caixa_central"),
        data_de=data_de,
        data_ate=data_ate,
        distribuicao=distribuicao,
        por_operadora=por_operadora,
        total_valor=total_valor,
        total_operacoes=total_operacoes,
        ticket_medio_geral=(total_valor / total_operacoes) if total_operacoes else 0,
        prazo_medio=prazo_medio,
    )


# ─── DEVOLUÇÃO E CANCELAMENTO ────────────────────────────────────────────────
# Devolver é operação nova, com documento próprio. A venda de origem não é
# alterada — só ganha o marcador CANCELADA quando a devolução é total.

@pdv_bp.route("/devolucoes")
def consultar_devolucoes():
    redir = _checar_acesso("DEVOLUCOES")
    if redir:
        return redir

    hoje = date.today()
    data_de = (request.args.get("data_de") or hoje.replace(day=1).isoformat()).strip()
    data_ate = (request.args.get("data_ate") or hoje.isoformat()).strip()

    cur = _cursor()
    try:
        cur.execute("""
            SELECT d.*, v.numero_venda, v.nome_cliente, v.data_venda,
                   c.nome AS nome_conta,
                   (SELECT COUNT(*) FROM pdv_devolucoes_itens i
                     WHERE i.id_pdv_devolucao = d.id_pdv_devolucao) AS itens
            FROM pdv_devolucoes d
            JOIN pdv_vendas v ON v.id_pdv_venda = d.id_pdv_venda
            LEFT JOIN pdv_contas_financeiras c
                   ON c.id_pdv_conta_financeira = d.id_pdv_conta_financeira
            WHERE d.cod_empresa = %s AND d.data_devolucao BETWEEN %s AND %s
            ORDER BY d.data_devolucao DESC, d.numero_devolucao DESC
        """, (_empresa(), data_de, data_ate))
        devolucoes = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    total = sum(float(d["valor_total"] or 0) for d in devolucoes)

    return render_template(
        "pdv/devolucoes.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_pdv"),
        devolucoes=devolucoes,
        data_de=data_de,
        data_ate=data_ate,
        total=total,
        destinos=DESTINOS_VALOR,
    )


@pdv_bp.route("/devolucoes/nova/<int:id_venda>")
def nova_devolucao(id_venda):
    redir = _checar_acesso("DEVOLUCOES")
    if redir:
        return redir

    cur = _cursor()
    try:
        cur.execute("""
            SELECT id_pdv_venda, numero_venda, data_venda, nome_cliente,
                   nome_vendedor, valor_total, situacao
            FROM pdv_vendas WHERE id_pdv_venda = %s AND cod_empresa = %s
        """, (id_venda, _empresa()))
        venda = cur.fetchone()
        if not venda:
            flash("Venda não encontrada.", "error")
            return redirect(url_for("pdv.consultar_vendas"))
        venda = dict(venda)

        itens = itens_disponiveis(cur, _empresa(), id_venda)

        cur.execute("""
            SELECT forma, valor FROM pdv_vendas_recebimentos
            WHERE id_pdv_venda = %s ORDER BY sequencia
        """, (id_venda,))
        recebimentos = [dict(r) for r in cur.fetchall()]

        contas = _contas_ativas(cur)

        # só faz sentido oferecer abatimento se existe nota a prazo em aberto
        cur.execute("""
            SELECT COUNT(*) AS total FROM pdv_notas_prazo
            WHERE id_pdv_venda = %s AND cod_empresa = %s AND situacao = 'ABERTA'
        """, (id_venda, _empresa()))
        tem_nota_aberta = cur.fetchone()["total"] > 0
    finally:
        cur.close()

    for item in itens:
        item["preco_efetivo"] = float(item["preco_efetivo"] or 0)

    return render_template(
        "pdv/devolucao_nova.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.detalhe_venda", id_venda=id_venda),
        venda=venda,
        itens=itens,
        recebimentos=recebimentos,
        contas=contas,
        tem_nota_aberta=tem_nota_aberta,
        tem_cartao=any(r["forma"] in FORMAS_CARTAO for r in recebimentos),
        destinos=DESTINOS_VALOR,
        hoje=date.today().isoformat(),
        rotulos_forma=dict(FORMAS_RECEBIMENTO),
    )


@pdv_bp.route("/api/devolucoes", methods=["POST"])
def api_registrar_devolucao():
    erro = _erro_permissao("DEVOLUCOES")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    itens = [
        {"id_pdv_venda_item": i.get("id_pdv_venda_item"),
         "quantidade": _num(i.get("quantidade"))}
        for i in (dados.get("itens") or [])
        if _num(i.get("quantidade")) > 0
    ]
    if not itens:
        return jsonify({"ok": False, "erro": "Informe o que está sendo devolvido."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        resultado = registrar_devolucao(
            cur, _empresa(), _cod_filial(), dados.get("id_pdv_venda"),
            (dados.get("data_devolucao") or date.today().isoformat()).strip(),
            itens,
            (dados.get("destino_valor") or "DINHEIRO").strip().upper(),
            dados.get("id_pdv_conta_financeira") or None,
            (dados.get("motivo") or "").strip() or None,
            session.get("id_usuario"), session.get("nome_usuario"),
        )
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao registrar a devolução: {e}"}), 500
    finally:
        cur.close()

    return jsonify({"ok": True, **resultado})


@pdv_bp.route("/devolucoes/<int:id_devolucao>")
def detalhe_devolucao(id_devolucao):
    redir = _checar_acesso("DEVOLUCOES")
    if redir:
        return redir

    cur = _cursor()
    try:
        cur.execute("""
            SELECT d.*, v.numero_venda, v.data_venda, v.nome_cliente, v.nome_vendedor,
                   v.valor_total AS valor_venda, c.nome AS nome_conta
            FROM pdv_devolucoes d
            JOIN pdv_vendas v ON v.id_pdv_venda = d.id_pdv_venda
            LEFT JOIN pdv_contas_financeiras c
                   ON c.id_pdv_conta_financeira = d.id_pdv_conta_financeira
            WHERE d.id_pdv_devolucao = %s AND d.cod_empresa = %s
        """, (id_devolucao, _empresa()))
        devolucao = cur.fetchone()
        if not devolucao:
            flash("Devolução não encontrada.", "error")
            return redirect(url_for("pdv.consultar_devolucoes"))
        devolucao = dict(devolucao)

        cur.execute("""
            SELECT * FROM pdv_devolucoes_itens
            WHERE id_pdv_devolucao = %s ORDER BY id_pdv_devolucao_item
        """, (id_devolucao,))
        itens = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT m.data_movimento, m.quantidade, m.custo_unitario, p.descricao
            FROM pdv_estoque_movimentos m
            JOIN pdv_produtos p ON p.id_pdv_produto = m.id_pdv_produto
            WHERE m.cod_empresa = %s AND m.tipo_origem = 'DEVOLUCAO' AND m.id_origem = %s
            ORDER BY m.id_pdv_estoque_movimento
        """, (_empresa(), id_devolucao))
        movimentos = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT l.data_lancamento, l.valor, l.historico, c.nome AS nome_conta
            FROM pdv_lancamentos_financeiros l
            LEFT JOIN pdv_contas_financeiras c
                   ON c.id_pdv_conta_financeira = l.id_pdv_conta_financeira
            WHERE l.cod_empresa = %s AND l.tipo_origem = 'DEVOLUCAO' AND l.id_origem = %s
        """, (_empresa(), id_devolucao))
        lancamentos = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    return render_template(
        "pdv/devolucao_detalhe.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.consultar_devolucoes"),
        devolucao=devolucao,
        itens=itens,
        movimentos=movimentos,
        lancamentos=lancamentos,
        destinos=DESTINOS_VALOR,
    )


# ─── CANAIS DE VENDA ─────────────────────────────────────────────────────────
# Canal é a porta por onde a venda saiu (balcão, e-commerce, outlet) — não é
# filial. Por padrão todos usam o estoque da filial; o parâmetro
# `estoque_por_canal` liga o saldo individualizado.

@pdv_bp.route("/canais")
def config_canais():
    redir = _checar_acesso("CANAIS_VENDA")
    if redir:
        return redir

    cur = _cursor()
    try:
        cur.execute("""
            SELECT cod_filial, nome_filial FROM filiais
            WHERE cod_empresa = %s AND ativo ORDER BY cod_filial
        """, (_empresa(),))
        filiais = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT id_pdv_canal, cod_filial, nome, padrao, ativo, ordem
            FROM pdv_canais_venda WHERE cod_empresa = %s
            ORDER BY cod_filial, ordem, nome
        """, (_empresa(),))
        canais = [dict(r) for r in cur.fetchall()]

        por_canal = estoque_por_canal(cur, _empresa())
    finally:
        cur.close()

    for filial in filiais:
        filial["canais"] = [c for c in canais if c["cod_filial"] == filial["cod_filial"]]

    return render_template(
        "pdv/canais_venda.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_pdv"),
        filiais=filiais,
        estoque_por_canal=por_canal,
        pode_transferir=pode("TRANSFERIR_ESTOQUE"),
    )


@pdv_bp.route("/api/canais", methods=["POST"])
@pdv_bp.route("/api/canais/<int:id_canal>", methods=["PUT", "DELETE"])
def api_canais(id_canal=None):
    erro = _erro_permissao("CANAIS_VENDA")
    if erro:
        return erro

    conn = get_connection()
    cur = conn.cursor()
    try:
        if request.method == "DELETE":
            try:
                cur.execute("""
                    DELETE FROM pdv_canais_venda
                    WHERE id_pdv_canal = %s AND cod_empresa = %s
                """, (id_canal, _empresa()))
            except Exception:
                conn.rollback()
                return jsonify({
                    "ok": False,
                    "erro": ("Este canal já foi usado em vendas ou movimentos. "
                             "Desmarque 'Ativo' em vez de excluir."),
                }), 400
            conn.commit()
            return jsonify({"ok": True})

        dados = request.get_json(silent=True) or {}
        nome = (dados.get("nome") or "").strip()
        if not nome:
            return jsonify({"ok": False, "erro": "Informe o nome do canal."}), 400

        padrao = bool(dados.get("padrao"))
        ativo = bool(dados.get("ativo", True))
        ordem = int(_num(dados.get("ordem"), 10)) or 10
        cod_filial = int(_num(dados.get("cod_filial"), _cod_filial()))

        try:
            # só um canal padrão por filial: marcar um desmarca o outro
            if padrao:
                cur.execute("""
                    UPDATE pdv_canais_venda SET padrao = FALSE, atualizado_em = now()
                    WHERE cod_empresa = %s AND cod_filial = %s
                """, (_empresa(), cod_filial))

            if id_canal:
                cur.execute("""
                    UPDATE pdv_canais_venda
                       SET nome = %s, padrao = %s, ativo = %s, ordem = %s,
                           atualizado_em = now()
                     WHERE id_pdv_canal = %s AND cod_empresa = %s
                """, (nome, padrao, ativo, ordem, id_canal, _empresa()))
                if cur.rowcount == 0:
                    return jsonify({"ok": False, "erro": "Canal não encontrado."}), 404
            else:
                cur.execute("""
                    INSERT INTO pdv_canais_venda
                        (cod_empresa, cod_filial, nome, padrao, ativo, ordem)
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING id_pdv_canal
                """, (_empresa(), cod_filial, nome, padrao, ativo, ordem))
                id_canal = cur.fetchone()[0]
            conn.commit()
        except Exception as e:
            conn.rollback()
            return jsonify({"ok": False, "erro": f"Erro ao salvar: {e}"}), 500
    finally:
        cur.close()
    return jsonify({"ok": True, "id": id_canal})


@pdv_bp.route("/api/canais/estoque-por-canal", methods=["PUT"])
def api_estoque_por_canal():
    """
    Liga/desliga o saldo individualizado por canal.

    Desligado (padrão), os canais usam o estoque da filial e a transferência
    entre canais deixa de fazer sentido — some da tela.
    """
    erro = _erro_permissao("CANAIS_VENDA")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO pdv_parametros (cod_empresa, opera_pdv, estoque_por_canal)
            VALUES (%s, TRUE, %s)
            ON CONFLICT (cod_empresa)
            DO UPDATE SET estoque_por_canal = EXCLUDED.estoque_por_canal,
                          atualizado_em = now()
        """, (_empresa(), bool(dados.get("estoque_por_canal"))))
        conn.commit()
    finally:
        cur.close()
    return jsonify({"ok": True})


@pdv_bp.route("/canais/transferir")
def tela_transferir_canal():
    redir = _checar_acesso("TRANSFERIR_ESTOQUE")
    if redir:
        return redir

    cur = _cursor()
    try:
        if not estoque_por_canal(cur, _empresa()):
            flash("Os canais usam o estoque da filial — não há o que transferir.", "error")
            return redirect(url_for("pdv.config_canais"))

        canais = canais_da_filial(cur, _empresa(), _cod_filial())
        cur.execute("""
            SELECT p.id_pdv_produto, p.sku, p.descricao, p.unidade,
                   pf.quantidade_atual
            FROM pdv_produtos_filiais pf
            JOIN pdv_produtos p ON p.id_pdv_produto = pf.id_pdv_produto
            WHERE pf.cod_empresa = %s AND pf.cod_filial = %s
              AND pf.situacao = 'ATIVO' AND p.ativo
            ORDER BY p.ordem, p.descricao
        """, (_empresa(), _cod_filial()))
        produtos = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    for p in produtos:
        p["quantidade_atual"] = float(p["quantidade_atual"] or 0)

    return render_template(
        "pdv/transferir_canal.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.config_canais"),
        canais=canais,
        produtos=produtos,
        hoje=date.today().isoformat(),
    )


@pdv_bp.route("/api/canais/saldos/<int:id_produto>")
def api_saldos_canal(id_produto):
    erro = _erro_permissao("TRANSFERIR_ESTOQUE")
    if erro:
        return erro
    cur = _cursor()
    try:
        return jsonify({"ok": True, **saldos_por_canal(cur, _empresa(), id_produto)})
    finally:
        cur.close()


@pdv_bp.route("/api/canais/transferir", methods=["POST"])
def api_transferir_canal():
    erro = _erro_permissao("TRANSFERIR_ESTOQUE")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if not estoque_por_canal(cur, _empresa()):
            return jsonify({
                "ok": False,
                "erro": "Os canais usam o estoque da filial — não há o que transferir.",
            }), 400

        transferir_estoque(
            cur, _empresa(), _cod_filial(), dados.get("id_pdv_produto"),
            dados.get("id_canal_origem"), dados.get("id_canal_destino"),
            _num(dados.get("quantidade")),
            (dados.get("data_movimento") or date.today().isoformat()).strip(),
            (dados.get("observacao") or "").strip() or None,
            session.get("id_usuario"),
        )
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao transferir: {e}"}), 500
    finally:
        cur.close()
    return jsonify({"ok": True})


# ─── IMPORTAÇÃO DO ESTOQUE ───────────────────────────────────────────────────

@pdv_bp.route("/estoque/importar", methods=["GET", "POST"])
def importar_estoque():
    """
    Carga do estoque a partir do CSV da loja.

    Reimportar o mesmo arquivo é seguro: produto que já tem movimento recebe
    só o ajuste da diferença, nunca uma nova entrada cheia.
    """
    redir = _checar_acesso("IMPORTAR_ESTOQUE")
    if redir:
        return redir

    cur = _cursor()
    try:
        canais = canais_da_filial(cur, _empresa(), _cod_filial())
        cur.execute("""
            SELECT * FROM pdv_estoque_importacoes
            WHERE cod_empresa = %s ORDER BY id_pdv_estoque_importacao DESC LIMIT 10
        """, (_empresa(),))
        importacoes = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    if request.method == "GET":
        return render_template(
            "pdv/importar_estoque.html",
            nome_empresa=session.get("nome_empresa"),
            url_voltar=url_for("pdv.consultar_estoque"),
            canais=canais,
            importacoes=importacoes,
            resultado=None,
            avisos=[],
        )

    arquivo = request.files.get("arquivo")
    if not arquivo or not arquivo.filename:
        flash("Escolha o arquivo CSV.", "error")
        return redirect(url_for("pdv.importar_estoque"))

    try:
        linhas, avisos = ler_csv(arquivo.read())
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for("pdv.importar_estoque"))

    produtos = agrupar_por_sku(linhas)
    referencia = (data_do_nome(arquivo.filename)
                  or datetime.strptime(
                      (request.form.get("data_referencia") or date.today().isoformat()),
                      "%Y-%m-%d").date())

    canais_por_nome = {c["nome"]: c["id_pdv_canal"] for c in canais}

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        resumo = importar(cur, _empresa(), _cod_filial(), produtos, referencia,
                          canais_por_nome, session.get("id_usuario"))

        cur.execute("""
            INSERT INTO pdv_estoque_importacoes
                (cod_empresa, cod_filial, nome_arquivo, data_referencia, linhas,
                 produtos_novos, produtos_atualizados, movimentos, pecas,
                 id_usuario, nome_usuario)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (_empresa(), _cod_filial(), arquivo.filename, referencia, len(linhas),
              resumo["produtos_novos"], resumo["produtos_atualizados"],
              resumo["movimentos"], resumo["pecas"],
              session.get("id_usuario"), session.get("nome_usuario")))
        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f"Erro ao importar: {e}", "error")
        return redirect(url_for("pdv.importar_estoque"))
    finally:
        cur.close()

    cur = _cursor()
    try:
        cur.execute("""
            SELECT * FROM pdv_estoque_importacoes
            WHERE cod_empresa = %s ORDER BY id_pdv_estoque_importacao DESC LIMIT 10
        """, (_empresa(),))
        importacoes = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    return render_template(
        "pdv/importar_estoque.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.consultar_estoque"),
        canais=canais,
        importacoes=importacoes,
        resultado={**resumo, "linhas": len(linhas), "produtos": len(produtos),
                   "arquivo": arquivo.filename, "referencia": referencia},
        avisos=avisos[:30],
    )


# ─── CAMPANHAS (PREÇO PROMOCIONAL) ───────────────────────────────────────────

@pdv_bp.route("/campanhas")
def menu_campanhas():
    redir = _checar_acesso("CAMPANHAS_MENU")
    if redir:
        return redir

    cur = _cursor()
    try:
        vigentes = campanhas_vigentes(cur, _empresa())
        cur.execute("""
            SELECT COUNT(*) AS total FROM pdv_campanhas_itens ci
            JOIN pdv_campanhas c ON c.id_pdv_campanha = ci.id_pdv_campanha
            WHERE ci.cod_empresa = %s AND c.situacao = 'ATIVA'
              AND CURRENT_DATE BETWEEN c.data_inicio AND c.data_fim
        """, (_empresa(),))
        itens_vigentes = cur.fetchone()["total"]
    finally:
        cur.close()

    return render_template(
        "pdv/menu_campanhas.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_pdv"),
        vigentes=vigentes,
        itens_vigentes=itens_vigentes,
        pode_cadastrar=pode("CAMPANHAS"),
        pode_itens=pode("CAMPANHAS_ITENS"),
    )


@pdv_bp.route("/campanhas/itens")
def itens_campanha():
    """
    Carrega os itens de uma campanha por filtro (marca, categoria ou todos) e
    aplica o percentual de desconto.
    """
    redir = _checar_acesso("CAMPANHAS_ITENS")
    if redir:
        return redir

    ano = (request.args.get("ano") or "").strip()
    id_campanha = request.args.get("id_campanha")

    cur = _cursor()
    try:
        cur.execute("""
            SELECT DISTINCT EXTRACT(YEAR FROM data_inicio)::int AS ano
            FROM pdv_campanhas WHERE cod_empresa = %s ORDER BY 1 DESC
        """, (_empresa(),))
        anos = [r["ano"] for r in cur.fetchall()]

        if not ano and anos:
            ano = str(anos[0])

        filtro_ano = " AND EXTRACT(YEAR FROM data_inicio) = %s" if ano else ""
        parametros = [_empresa()] + ([int(ano)] if ano else [])
        cur.execute(f"""
            SELECT c.*, (SELECT COUNT(*) FROM pdv_campanhas_itens i
                          WHERE i.id_pdv_campanha = c.id_pdv_campanha) AS itens
            FROM pdv_campanhas c
            WHERE c.cod_empresa = %s{filtro_ano}
            ORDER BY c.data_inicio DESC, c.id_pdv_campanha DESC
        """, parametros)
        campanhas = [dict(r) for r in cur.fetchall()]

        campanha = None
        itens = []
        if id_campanha:
            campanha = next((c for c in campanhas
                             if str(c["id_pdv_campanha"]) == str(id_campanha)), None)
            if campanha:
                cur.execute("""
                    SELECT ci.id_pdv_campanha_item, ci.percentual_desconto,
                           p.id_pdv_produto, p.sku, p.descricao, p.marca,
                           p.categoria, p.cor, p.tamanho, p.preco_venda,
                           COALESCE(pf.quantidade_atual, 0) AS quantidade_atual
                    FROM pdv_campanhas_itens ci
                    JOIN pdv_produtos p ON p.id_pdv_produto = ci.id_pdv_produto
                    LEFT JOIN pdv_produtos_filiais pf
                           ON pf.id_pdv_produto = p.id_pdv_produto
                          AND pf.cod_empresa = p.cod_empresa AND pf.cod_filial = %s
                    WHERE ci.id_pdv_campanha = %s
                    ORDER BY p.descricao
                """, (_cod_filial(), campanha["id_pdv_campanha"],))
                itens = [dict(r) for r in cur.fetchall()]
                for i in itens:
                    i["preco_promocional"] = preco_promocional(
                        i["preco_venda"], i["percentual_desconto"])

        # os filtros disponíveis saem do próprio cadastro de produtos
        cur.execute("""
            SELECT DISTINCT marca FROM pdv_produtos
            WHERE cod_empresa = %s AND ativo AND marca IS NOT NULL ORDER BY 1
        """, (_empresa(),))
        marcas = [r["marca"] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT categoria FROM pdv_produtos
            WHERE cod_empresa = %s AND ativo AND categoria IS NOT NULL ORDER BY 1
        """, (_empresa(),))
        categorias = [r["categoria"] for r in cur.fetchall()]
    finally:
        cur.close()

    return render_template(
        "pdv/campanha_itens.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_campanhas"),
        anos=anos,
        ano=ano,
        campanhas=campanhas,
        campanha=campanha,
        itens=itens,
        marcas=marcas,
        categorias=categorias,
        total_itens=len(itens),
    )


@pdv_bp.route("/api/campanhas/<int:id_campanha>/carregar", methods=["POST"])
def api_carregar_itens_campanha(id_campanha):
    erro = _erro_permissao("CAMPANHAS_ITENS")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 1 FROM pdv_campanhas
            WHERE id_pdv_campanha = %s AND cod_empresa = %s
        """, (id_campanha, _empresa()))
        if not cur.fetchone():
            return jsonify({"ok": False, "erro": "Campanha não encontrada."}), 404

        incluidos, atualizados = carregar_itens(
            cur, _empresa(), id_campanha,
            _num(dados.get("percentual_desconto")),
            (dados.get("marca") or "").strip() or None,
            (dados.get("categoria") or "").strip() or None,
        )
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao carregar itens: {e}"}), 500
    finally:
        cur.close()

    return jsonify({"ok": True, "incluidos": incluidos, "atualizados": atualizados})


@pdv_bp.route("/api/campanhas/itens/<int:id_item>", methods=["DELETE"])
def api_remover_item_campanha(id_item):
    erro = _erro_permissao("CAMPANHAS_ITENS")
    if erro:
        return erro

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            DELETE FROM pdv_campanhas_itens
            WHERE id_pdv_campanha_item = %s AND cod_empresa = %s
        """, (id_item, _empresa()))
        conn.commit()
    finally:
        cur.close()
    return jsonify({"ok": True})


@pdv_bp.route("/api/campanhas/<int:id_campanha>/itens", methods=["DELETE"])
def api_limpar_itens_campanha(id_campanha):
    """Esvazia a campanha — útil para recarregar com outro filtro."""
    erro = _erro_permissao("CAMPANHAS_ITENS")
    if erro:
        return erro

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            DELETE FROM pdv_campanhas_itens
            WHERE id_pdv_campanha = %s AND cod_empresa = %s
        """, (id_campanha, _empresa()))
        removidos = cur.rowcount
        conn.commit()
    finally:
        cur.close()
    return jsonify({"ok": True, "removidos": removidos})


@pdv_bp.route("/campanhas/cadastrar")
def cadastrar_campanhas():
    """
    Cadastro das campanhas. Não usa o cadastro genérico porque a campanha tem
    ações próprias de ciclo de vida: pausar, retomar e encerrar antes do fim.
    """
    redir = _checar_acesso("CAMPANHAS")
    if redir:
        return redir

    cur = _cursor()
    try:
        cur.execute("""
            SELECT c.*, (SELECT COUNT(*) FROM pdv_campanhas_itens i
                          WHERE i.id_pdv_campanha = c.id_pdv_campanha) AS itens
            FROM pdv_campanhas c
            WHERE c.cod_empresa = %s
            ORDER BY c.data_inicio DESC, c.id_pdv_campanha DESC
        """, (_empresa(),))
        campanhas = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    hoje = date.today()
    for c in campanhas:
        c["vigente"] = (c["situacao"] == "ATIVA"
                        and c["data_inicio"] <= hoje <= c["data_fim"])

    return render_template(
        "pdv/campanhas.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_campanhas"),
        campanhas=campanhas,
        situacoes=SITUACOES_CAMPANHA,
        hoje=hoje.isoformat(),
        pode_itens=pode("CAMPANHAS_ITENS"),
    )


@pdv_bp.route("/api/campanhas", methods=["POST"])
@pdv_bp.route("/api/campanhas/<int:id_campanha>", methods=["PUT", "DELETE"])
def api_campanhas(id_campanha=None):
    erro = _erro_permissao("CAMPANHAS")
    if erro:
        return erro

    conn = get_connection()
    cur = conn.cursor()
    try:
        if request.method == "DELETE":
            # os itens da campanha saem junto (ON DELETE CASCADE), mas venda
            # que saiu por ela não deixa: o histórico aponta para a campanha e
            # não pode virar referência solta
            try:
                cur.execute("""
                    DELETE FROM pdv_campanhas
                    WHERE id_pdv_campanha = %s AND cod_empresa = %s
                """, (id_campanha, _empresa()))
            except Exception:
                conn.rollback()
                return jsonify({
                    "ok": False,
                    "erro": ("Esta campanha já tem vendas. Encerre-a em vez de excluir — "
                             "o histórico precisa continuar apontando para ela."),
                }), 400
            conn.commit()
            return jsonify({"ok": True})

        dados = request.get_json(silent=True) or {}
        nome = (dados.get("nome") or "").strip()
        inicio = (dados.get("data_inicio") or "").strip()
        fim = (dados.get("data_fim") or "").strip()
        percentual = _num(dados.get("percentual_desconto"))

        if not nome:
            return jsonify({"ok": False, "erro": "Informe o nome da campanha."}), 400
        if not inicio or not fim:
            return jsonify({"ok": False, "erro": "Informe o período da campanha."}), 400
        if fim < inicio:
            return jsonify({"ok": False, "erro": "O término não pode ser antes do início."}), 400
        if percentual < 0 or percentual >= 100:
            return jsonify({"ok": False, "erro": "O desconto tem que estar entre 0 e 100."}), 400

        if id_campanha:
            cur.execute("""
                UPDATE pdv_campanhas
                   SET nome = %s, data_inicio = %s, data_fim = %s,
                       percentual_desconto = %s, observacao = %s, atualizado_em = now()
                 WHERE id_pdv_campanha = %s AND cod_empresa = %s
            """, (nome, inicio, fim, percentual,
                  (dados.get("observacao") or "").strip() or None,
                  id_campanha, _empresa()))
            if cur.rowcount == 0:
                return jsonify({"ok": False, "erro": "Campanha não encontrada."}), 404
        else:
            cur.execute("""
                INSERT INTO pdv_campanhas
                    (cod_empresa, nome, data_inicio, data_fim, percentual_desconto,
                     observacao, situacao)
                VALUES (%s, %s, %s, %s, %s, %s, 'ATIVA')
                RETURNING id_pdv_campanha
            """, (_empresa(), nome, inicio, fim, percentual,
                  (dados.get("observacao") or "").strip() or None))
            id_campanha = cur.fetchone()[0]
        conn.commit()
    finally:
        cur.close()
    return jsonify({"ok": True, "id": id_campanha})


@pdv_bp.route("/api/campanhas/<int:id_campanha>/situacao", methods=["PUT"])
def api_situacao_campanha(id_campanha):
    """
    Pausa, retoma ou encerra a campanha.

    Encerrar traz `data_fim` para hoje: o período gravado passa a refletir o
    que de fato valeu, e não o que estava previsto. Por isso encerrar não tem
    volta — para valer de novo, cria-se outra campanha.
    """
    erro = _erro_permissao("CAMPANHAS")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    situacao = (dados.get("situacao") or "").strip().upper()
    if situacao not in SITUACOES_CAMPANHA:
        return jsonify({"ok": False, "erro": "Situação inválida."}), 400

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT situacao, data_inicio FROM pdv_campanhas
            WHERE id_pdv_campanha = %s AND cod_empresa = %s
        """, (id_campanha, _empresa()))
        campanha = cur.fetchone()
        if not campanha:
            return jsonify({"ok": False, "erro": "Campanha não encontrada."}), 404
        if campanha["situacao"] == "ENCERRADA":
            return jsonify({
                "ok": False,
                "erro": "Esta campanha já foi encerrada. Crie uma nova para valer de novo.",
            }), 400

        if situacao == "ENCERRADA":
            hoje = date.today()
            # campanha encerrada antes mesmo de começar não pode ter término
            # anterior ao início
            fim = max(hoje, campanha["data_inicio"])
            cur.execute("""
                UPDATE pdv_campanhas
                   SET situacao = 'ENCERRADA', data_fim = %s, atualizado_em = now()
                 WHERE id_pdv_campanha = %s AND cod_empresa = %s
            """, (fim, id_campanha, _empresa()))
        else:
            cur.execute("""
                UPDATE pdv_campanhas SET situacao = %s, atualizado_em = now()
                WHERE id_pdv_campanha = %s AND cod_empresa = %s
            """, (situacao, id_campanha, _empresa()))
        conn.commit()
    finally:
        cur.close()
    return jsonify({"ok": True})


# ─── BUSCA DE PRODUTO (SKU / DESCRIÇÃO) ──────────────────────────────────────
# O SKU é a porta de entrada da venda: é ele que o leitor de código de barras
# lê. A busca por descrição é o caminho alternativo, para quando a etiqueta
# não lê ou a peça não tem etiqueta.

# 60 era pouco para navegar uma marca inteira (a maior d'O Closet tem ~1.100
# peças). 200 cabe na rolagem do modal sem pesar, e o aviso de "refine" cobre
# o resto.
LIMITE_BUSCA_PRODUTOS = 200


def _condicoes_descricao(termo):
    """
    Traduz o que foi digitado em condições sobre a descrição.

    A busca aceita **vários termos na mesma linha**, e todos precisam bater
    (E, não OU) — é assim que `*regata *UV50` acha a regata que também é UV50.

        regata            → descrição COMEÇA com "regata"
        *bossa            → contém "bossa" em qualquer posição
        *regata *uv50     → contém os dois
        regata *uv50      → começa com "regata" E contém "uv50"
        top speed power   → COMEÇA com a frase inteira

    A última linha é a razão de a frase sem asterisco não ser quebrada: quem
    digita várias palavras sem asterisco está escrevendo o começo do nome, e
    "começa com top E começa com speed" seria impossível de satisfazer.

    Devolve (condições, parâmetros).
    """
    termo = (termo or "").strip()
    if not termo:
        return [], []

    # nenhum asterisco: a frase inteira é o começo do nome
    if "*" not in termo:
        if len(termo) < 2:
            return [], []
        return ["p.descricao ILIKE %s"], [f"{termo}%"]

    condicoes, parametros = [], []
    for parte in termo.split():
        if parte.startswith("*"):
            alvo = parte.lstrip("*").strip()
            if not alvo:
                continue                      # asterisco solto não filtra nada
            condicoes.append("p.descricao ILIKE %s")
            parametros.append(f"%{alvo}%")
        else:
            if len(parte) < 2:
                continue
            condicoes.append("p.descricao ILIKE %s")
            parametros.append(f"{parte}%")
    return condicoes, parametros


def _produto_para_tela(cur, produto, promocoes=None):
    """Produto no formato que a tela de venda usa, já com a promoção do dia."""
    if promocoes is None:
        promocoes = promocoes_do_dia(cur, _empresa())
    promo = promocoes.get(produto["id_pdv_produto"])
    return {
        "id_pdv_produto": produto["id_pdv_produto"],
        "sku": produto["sku"],
        "descricao": produto["descricao"],
        "unidade": produto.get("unidade") or "UN",
        "marca": produto.get("marca"),
        "tamanho": produto.get("tamanho"),
        "cor": produto.get("cor"),
        "preco_venda": float(produto["preco_venda"] or 0),
        "custo_atual": float(produto.get("custo_atual") or 0),
        "quantidade_atual": float(produto.get("quantidade_atual") or 0),
        "promocao": promo,
    }


@pdv_bp.route("/api/produtos/sku/<path:sku>")
def api_produto_por_sku(sku):
    """
    Leitura do SKU — o caminho do leitor de código de barras.

    Busca exata, porque o leitor entrega o código exatamente como está na
    etiqueta. Sem resultado, a tela avisa e a vendedora usa a busca por
    descrição.
    """
    erro = _erro_permissao("VENDER")
    if erro:
        return erro

    cur = _cursor()
    try:
        cur.execute("""
            SELECT p.id_pdv_produto, p.sku, p.descricao, p.unidade, p.marca,
                   p.cor, p.tamanho, p.preco_venda, p.custo_atual,
                   pf.quantidade_atual
            FROM pdv_produtos_filiais pf
            JOIN pdv_produtos p ON p.id_pdv_produto = pf.id_pdv_produto
            WHERE pf.cod_empresa = %s AND pf.cod_filial = %s
              AND pf.situacao = 'ATIVO' AND p.ativo
              AND upper(p.sku) = upper(%s)
        """, (_empresa(), _cod_filial(), (sku or "").strip()))
        produto = cur.fetchone()
        if not produto:
            return jsonify({"ok": False, "erro": f"SKU {sku} não encontrado."}), 404
        return jsonify({"ok": True, "produto": _produto_para_tela(cur, dict(produto))})
    finally:
        cur.close()


@pdv_bp.route("/api/produtos/buscar")
def api_buscar_produtos():
    """
    Busca por descrição, devolvendo sempre o SKU.

    Sem asterisco, procura o que **começa** com o texto e devolve em ordem
    alfabética — é a busca de quem sabe o início do nome da peça.

    Com asterisco (`*regata`), procura o texto em **qualquer posição** — é a
    busca de quem lembra só um pedaço.
    """
    erro = _erro_permissao("VENDER")
    if erro:
        return erro

    termo = (request.args.get("termo") or "").strip()
    marca = (request.args.get("marca") or "").strip()

    condicoes_termo, parametros_termo = _condicoes_descricao(termo)

    # Um dos dois basta: escolhida a marca, dá para listar tudo dela sem
    # digitar descrição nenhuma.
    if not marca and not condicoes_termo:
        return jsonify({
            "ok": False,
            "erro": "Digite ao menos 2 letras ou escolha uma marca.",
        }), 400

    condicoes = ["pf.cod_empresa = %s", "pf.cod_filial = %s",
                 "pf.situacao = 'ATIVO'", "p.ativo"] + condicoes_termo
    parametros = [_empresa(), _cod_filial()] + parametros_termo

    if marca:
        condicoes.append("p.marca = %s")
        parametros.append(marca)

    cur = _cursor()
    try:
        cur.execute(f"""
            SELECT p.id_pdv_produto, p.sku, p.descricao, p.unidade, p.marca,
                   p.cor, p.tamanho, p.preco_venda, p.custo_atual,
                   pf.quantidade_atual
            FROM pdv_produtos_filiais pf
            JOIN pdv_produtos p ON p.id_pdv_produto = pf.id_pdv_produto
            WHERE {' AND '.join(condicoes)}
            ORDER BY p.descricao
            LIMIT %s
        """, parametros + [LIMITE_BUSCA_PRODUTOS + 1])
        achados = [dict(r) for r in cur.fetchall()]

        promocoes = promocoes_do_dia(cur, _empresa())
        produtos = [_produto_para_tela(cur, p, promocoes)
                    for p in achados[:LIMITE_BUSCA_PRODUTOS]]
    finally:
        cur.close()

    return jsonify({
        "ok": True,
        "produtos": produtos,
        # avisa que existe mais coisa do que coube, para a vendedora refinar
        "excedeu": len(achados) > LIMITE_BUSCA_PRODUTOS,
        "limite": LIMITE_BUSCA_PRODUTOS,
    })


# ─── PRODUTO NA LOJA ─────────────────────────────────────────────────────────
# O cadastro é da empresa; a lista de itens é de cada loja. Aqui se inclui uma
# peça na loja pelo SKU e se oculta o que saiu de linha — sem apagar nada,
# porque venda e movimento apontam para o item.

@pdv_bp.route("/api/estoque/incluir-sku", methods=["POST"])
def api_incluir_produto_na_loja():
    erro = _erro_permissao("PRODUTOS_FILIAL")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        resultado = incluir_por_sku(cur, _empresa(), _cod_filial(),
                                    dados.get("sku"))
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao incluir: {e}"}), 500
    finally:
        cur.close()

    if resultado["reativado"]:
        aviso = f"{resultado['descricao']} voltou a aparecer nesta loja."
    elif resultado["ja_existia"]:
        aviso = f"{resultado['descricao']} já estava nesta loja."
    else:
        aviso = f"{resultado['descricao']} incluído nesta loja."
    return jsonify({"ok": True, "aviso": aviso, **resultado})


@pdv_bp.route("/api/estoque/ocultar", methods=["POST"])
def api_ocultar_produto_na_loja():
    erro = _erro_permissao("PRODUTOS_FILIAL")
    if erro:
        return erro

    dados = request.get_json(silent=True) or {}
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        ocultar(cur, _empresa(), _cod_filial(), dados.get("id_pdv_produto"),
                bool(dados.get("ocultar", True)))
        conn.commit()
    except ValueError as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": str(e)}), 400
    finally:
        cur.close()
    return jsonify({"ok": True})


@pdv_bp.route("/estoque/obsoletos")
def obsoletos_da_loja():
    """
    Itens zerados e parados há mais de 6 meses nesta loja.

    É uma sugestão de faxina, não uma limpeza automática: sumir sozinho com um
    item que a compradora esperava repor seria pior do que a lista comprida.
    """
    redir = _checar_acesso("PRODUTOS_FILIAL")
    if redir:
        return redir

    cur = _cursor()
    try:
        candidatos = candidatos_a_ocultar(cur, _empresa(), _cod_filial())

        cur.execute("""
            SELECT p.id_pdv_produto, p.sku, p.descricao, p.marca,
                   pf.ocultado_em, pf.ultimo_movimento_em
            FROM pdv_produtos_filiais pf
            JOIN pdv_produtos p ON p.id_pdv_produto = pf.id_pdv_produto
            WHERE pf.cod_empresa = %s AND pf.cod_filial = %s
              AND pf.situacao = 'OCULTO'
            ORDER BY p.descricao
        """, (_empresa(), _cod_filial()))
        ocultos = [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()

    return render_template(
        "pdv/estoque_obsoletos.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.consultar_estoque"),
        candidatos=candidatos,
        ocultos=ocultos,
        meses=MESES_SEM_MOVIMENTO,
    )


# ─── CAIXA DO DIA ────────────────────────────────────────────────────────────
# Um dia, todas as contas (ou uma), em grid. Valor positivo entra, negativo
# sai, e cada conta mostra saldo inicial → saldo final.
#
# O grid mostra TUDO o que passou pela conta no dia, inclusive o que veio de
# venda, de baixa de título ou de transferência. Mas só o lançamento MANUAL é
# editável: alterar aqui um lançamento que nasceu de uma venda faria o caixa
# deixar de bater com a venda que o gerou.

ORIGENS_EDITAVEIS = ("MANUAL",)


@pdv_bp.route("/financeiro/caixa")
def caixa_do_dia():
    redir = _checar_acesso("CAIXA")
    if redir:
        return redir

    dia = (request.args.get("data") or date.today().isoformat()).strip()
    filtro_conta = (request.args.get("id_conta") or "").strip()

    cur = _cursor()
    try:
        contas = _contas_ativas(cur)
        if not contas:
            flash("Cadastre ao menos uma conta financeira (caixa da loja, banco…).",
                  "error")
            return redirect(url_for("pdv.menu_cadastros"))

        escolhidas = [c for c in contas
                      if not filtro_conta
                      or str(c["id_pdv_conta_financeira"]) == filtro_conta]

        condicao = ""
        parametros = [_empresa(), dia]
        if filtro_conta:
            condicao = " AND l.id_pdv_conta_financeira = %s"
            parametros.append(int(filtro_conta))

        cur.execute(f"""
            SELECT l.id_pdv_lancamento, l.id_pdv_conta_financeira, l.data_lancamento,
                   l.valor, l.historico, l.tipo_origem, l.id_origem,
                   l.id_transferencia, l.conciliado, c.nome AS nome_conta
            FROM pdv_lancamentos_financeiros l
            JOIN pdv_contas_financeiras c
              ON c.id_pdv_conta_financeira = l.id_pdv_conta_financeira
            WHERE l.cod_empresa = %s AND l.data_lancamento = %s{condicao}
            ORDER BY c.ordem, c.nome, l.id_pdv_lancamento
        """, parametros)
        lancamentos = [dict(r) for r in cur.fetchall()]

        # cada conta com o seu saldo inicial (o que havia antes deste dia)
        blocos = []
        for conta in escolhidas:
            id_conta = conta["id_pdv_conta_financeira"]
            do_dia = [l for l in lancamentos
                      if l["id_pdv_conta_financeira"] == id_conta]
            inicial = saldo_ate(cur, _empresa(), id_conta, dia)

            entradas = sum(float(l["valor"]) for l in do_dia if l["valor"] > 0)
            saidas = sum(-float(l["valor"]) for l in do_dia if l["valor"] < 0)

            # o saldo corrente vai sendo calculado linha a linha, para a tela
            # mostrar como a conta chegou ao saldo final
            corrente = inicial
            for l in do_dia:
                corrente = round(corrente + float(l["valor"]), 2)
                l["saldo"] = corrente
                l["editavel"] = l["tipo_origem"] in ORIGENS_EDITAVEIS

            blocos.append({
                **conta,
                "lancamentos": do_dia,
                "saldo_inicial": inicial,
                "entradas": round(entradas, 2),
                "saidas": round(saidas, 2),
                "saldo_final": round(inicial + entradas - saidas, 2),
            })
    finally:
        cur.close()

    total = {
        "saldo_inicial": round(sum(b["saldo_inicial"] for b in blocos), 2),
        "entradas": round(sum(b["entradas"] for b in blocos), 2),
        "saidas": round(sum(b["saidas"] for b in blocos), 2),
        "saldo_final": round(sum(b["saldo_final"] for b in blocos), 2),
    }

    return render_template(
        "pdv/caixa_dia.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("pdv.menu_financeiro"),
        dia=dia,
        contas=contas,
        filtro_conta=filtro_conta,
        blocos=blocos,
        total=total,
        origens=ORIGENS_LANCAMENTO,
        pode_lancar=pode("LANCAMENTOS"),
    )


@pdv_bp.route("/api/financeiro/caixa", methods=["POST"])
@pdv_bp.route("/api/financeiro/caixa/<int:id_lancamento>",
              methods=["PUT", "DELETE"])
def api_caixa(id_lancamento=None):
    """
    Grava, altera e apaga o lançamento manual do caixa do dia.

    Lançamento que nasceu de outro documento (venda, baixa, transferência) não
    passa por aqui: ele é consequência daquele fato e só muda se o fato mudar.
    """
    erro = _erro_permissao("LANCAMENTOS")
    if erro:
        return erro

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        if id_lancamento:
            cur.execute("""
                SELECT tipo_origem FROM pdv_lancamentos_financeiros
                WHERE id_pdv_lancamento = %s AND cod_empresa = %s
            """, (id_lancamento, _empresa()))
            atual = cur.fetchone()
            if not atual:
                return jsonify({"ok": False, "erro": "Lançamento não encontrado."}), 404
            if atual["tipo_origem"] not in ORIGENS_EDITAVEIS:
                return jsonify({
                    "ok": False,
                    "erro": ("Este lançamento veio de outro documento (venda, baixa, "
                             "transferência) e não se altera pelo caixa."),
                }), 400

        if request.method == "DELETE":
            cur.execute("""
                DELETE FROM pdv_lancamentos_financeiros
                WHERE id_pdv_lancamento = %s AND cod_empresa = %s
            """, (id_lancamento, _empresa()))
            conn.commit()
            return jsonify({"ok": True})

        dados = request.get_json(silent=True) or {}
        id_conta = dados.get("id_pdv_conta_financeira")
        valor = round(_num(dados.get("valor")), 2)
        historico = (dados.get("historico") or "").strip()
        dia = (dados.get("data_lancamento") or date.today().isoformat()).strip()

        if not id_conta:
            return jsonify({"ok": False, "erro": "Informe a conta."}), 400
        if not historico:
            return jsonify({"ok": False, "erro": "Informe a descrição."}), 400
        if _centavos(valor) == 0:
            return jsonify({
                "ok": False,
                "erro": "Informe um valor diferente de zero (positivo entra, negativo sai).",
            }), 400

        if id_lancamento:
            cur.execute("""
                UPDATE pdv_lancamentos_financeiros
                   SET id_pdv_conta_financeira = %s, valor = %s, historico = %s,
                       atualizado_em = now()
                 WHERE id_pdv_lancamento = %s AND cod_empresa = %s
            """, (id_conta, valor, historico, id_lancamento, _empresa()))
        else:
            id_lancamento = lancar(
                cur, _empresa(), id_conta, dia, valor, historico,
                tipo_origem="MANUAL", id_usuario=session.get("id_usuario"))
        conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"ok": False, "erro": f"Erro ao gravar: {e}"}), 500
    finally:
        cur.close()

    return jsonify({"ok": True, "id": id_lancamento})
