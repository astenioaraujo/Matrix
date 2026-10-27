"""O que é combustível e o que não é, para as telas de vendas.

ARLA é vendido no posto e vem em `vendas_diarias` como qualquer outro produto.
Já foi tratado como não combustível e ficava fora do painel, da margem
unitária e da margem bruta que o Financeiro lê — decisão revertida em
28/10/2026 a pedido do cliente: a margem do ARLA deve contar junto com a de
combustível em todas as telas. A lista mora aqui, e não em cada tela, porque
todas precisam responder a mesma coisa — divergir aqui faria o resultado
financeiro não fechar com o painel.
"""

PRODUTOS_NAO_COMBUSTIVEL = ()

# Pronto para entrar num WHERE. O %% é escapado porque as consultas levam
# parâmetros: o psycopg desfaz o escape ao montar o SQL.
FILTRO_SQL_COMBUSTIVEL = "".join(
    f" AND descricao NOT ILIKE '%%{p}%%'" for p in PRODUTOS_NAO_COMBUSTIVEL
)
