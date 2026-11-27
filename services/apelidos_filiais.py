"""Nomes alternativos de posto no WebPostos (provisório).

Caso 1 — posto renomeado: durante a troca, os relatórios trazem o mesmo
posto com os dois nomes, até a data da mudança no nome antigo, dali em
diante no novo. O cadastro (`filiais.nome_filial_importacao`) fica com o
nome NOVO; o antigo é apelido dele.

Caso 2 — relatório com nome mais completo: o mesmo posto, no mesmo
relatório, mas um tipo diferente de exportação do WebPostos usa o nome do
posto por extenso, mais longo do que o que está cadastrado em
nome_filial_importacao (cadastrado a partir de outro relatório, truncado).

Nos dois casos, tudo o que vier na chave cai no posto do valor.

Usado pela importação de vendas diárias, pelas importações de PDF do fluxo
de caixa (sintético e analítico) e pela importação de Faltas e Sobras de
Caixa. Quando os arquivos pararem de trazer o nome alternativo, apagar a
entrada.
"""

APELIDOS_FILIAL_IMPORTACAO = {
    "EMP012": {
        # Carnaubais (filial 11): "AUTO POSTO SAO JOAO CARNAUBAIS" virou
        # "POSTO CARNAUBAIS" em novembro/2026.
        "AUTO POSTO SAO JOAO CARNAUBAIS": "POSTO CARNAUBAIS",
        # LO (filial 6) e LO GNV (filial 7): nome_filial_importacao veio
        # truncado da importação principal ("COM", "GN"); o relatório de
        # Sobras e Faltas de Caixa traz o nome completo (27/11/2026).
        "POSTO 30 DE SETEMBRO LESTE OESTE COMBUSTIVEIS": "POSTO 30 DE SETEMBRO LESTE OESTE COM",
        "POSTO 30 DE SETEMBRO - LESTE OESTE GNV": "POSTO 30 DE SETEMBRO - LESTE OESTE GN",
    },
}


def apelidos_da_empresa(cod_empresa):
    """{nome antigo: nome novo} da empresa (vazio se não houver)."""
    return APELIDOS_FILIAL_IMPORTACAO.get(str(cod_empresa or "").strip(), {})
