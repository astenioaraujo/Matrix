"""O que é combustível e o que não é, para as telas de vendas.

ARLA é vendido no posto e vem em `vendas_diarias` como qualquer outro produto.
Já foi tratado como não combustível e ficava fora do painel, da margem
unitária e da margem bruta que o Financeiro lê — decisão revertida em
28/10/2026 a pedido do cliente: a margem do ARLA deve contar junto com a de
combustível em todas as telas. A lista mora aqui, e não em cada tela, porque
todas precisam responder a mesma coisa — divergir aqui faria o resultado
financeiro não fechar com o painel.
"""

import re


PRODUTOS_NAO_COMBUSTIVEL = ()

# Pronto para entrar num WHERE. O %% é escapado porque as consultas levam
# parâmetros: o psycopg desfaz o escape ao montar o SQL.
FILTRO_SQL_COMBUSTIVEL = "".join(
    f" AND descricao NOT ILIKE '%%{p}%%'" for p in PRODUTOS_NAO_COMBUSTIVEL
)


# Código do produto (o de `combustiveis`) de uma descrição de vendas_diarias.
# Mesma regra de palavra-chave do CASE de services/estoques_service.py: mudar
# uma sem a outra faria as telas divergirem.
# Vendido no posto, mas não é combustível: fica fora do grid e não vale nem
# aviso — a ausência dele não é uma diferença a explicar.
PRODUTOS_IGNORADOS_MARGEM = PRODUTOS_NAO_COMBUSTIVEL

PALAVRAS_PRODUTO_MARGEM = [
    ("NATURAL", "GN"),
    ("GNV", "GN"),
    ("S10", "C5"),
    ("S500", "C4"),
    ("ADIT", "C2"),
    ("ETAN", "C3"),
    ("PODIUM", "C6"),
    ("GASOL", "C1"),
    ("DIESEL", "C4"),
]


def cod_produto_das_vendas(descricao_venda):
    """Código do produto para uma descrição vinda de vendas_diarias, pela mesma
    regra de palavra-chave da Consulta de Estoques. Devolve None para o que não
    é combustível (ARLA, por exemplo)."""
    txt = re.sub(r"[^A-Z0-9]", "", str(descricao_venda or "").upper())
    if not txt:
        return None

    if any(palavra in txt for palavra in PRODUTOS_IGNORADOS_MARGEM):
        return None

    for palavra, cod_produto in PALAVRAS_PRODUTO_MARGEM:
        if palavra in txt:
            return cod_produto

    return None
