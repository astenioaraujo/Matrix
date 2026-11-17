"""Nomes antigos de posto renomeado no WebPostos (provisório).

Durante a troca, os relatórios do WebPostos trazem o mesmo posto com os dois
nomes: até a data da mudança no nome antigo, dali em diante no novo. O
cadastro (`filiais.nome_filial_importacao`) fica com o nome NOVO; o antigo é
apelido dele, e tudo o que vier no nome antigo cai no posto atual.

Usado pela importação de vendas diárias e pelas importações de PDF do fluxo
de caixa (sintético e analítico). Quando os arquivos pararem de trazer o nome
antigo, apagar a entrada.
"""

APELIDOS_FILIAL_IMPORTACAO = {
    # Carnaubais (filial 11): "AUTO POSTO SAO JOAO CARNAUBAIS" virou
    # "POSTO CARNAUBAIS" em novembro/2026.
    "EMP012": {"AUTO POSTO SAO JOAO CARNAUBAIS": "POSTO CARNAUBAIS"},
}


def apelidos_da_empresa(cod_empresa):
    """{nome antigo: nome novo} da empresa (vazio se não houver)."""
    return APELIDOS_FILIAL_IMPORTACAO.get(str(cod_empresa or "").strip(), {})
