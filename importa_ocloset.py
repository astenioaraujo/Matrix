"""
Importação do CSV de Contas a Pagar do sistema O Closet para a tabela
temporária `importacoes` (mesma tabela usada pelas importações WebPostos).

Layout do arquivo (separador ';', codificação UTF-8 com BOM):

    Vencimento;Emissão;Descrição;Contato;Documento;Categoria;Conta bancária;
    Forma de pagamento;Origem;Nº;Valor;Valor pago;Restante;Status;Data pagamento

A última linha do arquivo é um rodapé ("312 lançamento(s)") e é descartada.

A classificação reaproveita `classificacoes_automaticas`: o histórico gravado é
"<Categoria> | <Descrição> | <Contato>", de forma que o motor de classificação por texto
(`classificar_lancamentos_importados`) encontre a categoria dentro do histórico
tanto na importação quanto no botão "Reclassificar Lançamentos".

Todos os valores são gravados negativos: o arquivo é de contas a pagar e o CSV
traz os valores sem sinal.
"""

import csv
import io

from datetime import datetime
from decimal import Decimal, InvalidOperation

from psycopg2.extras import execute_values


COLUNAS_ESPERADAS = [
    "Vencimento",
    "Emissão",
    "Descrição",
    "Contato",
    "Documento",
    "Categoria",
    "Conta bancária",
    "Forma de pagamento",
    "Origem",
    "Nº",
    "Valor",
    "Valor pago",
    "Restante",
    "Status",
    "Data pagamento",
]

# Único status que entra no fluxo de caixa. Vencido, Parcial, Cancelado e
# qualquer outro são descartados.
STATUS_ACEITO = "pago"


def _texto(valor):
    return str(valor or "").strip()


def _converter_data(texto):
    texto = _texto(texto)

    if not texto:
        return None

    for formato in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return datetime.strptime(texto, formato).date()
        except ValueError:
            continue

    return None


def _converter_valor(texto):
    texto = _texto(texto)

    if not texto:
        return Decimal("0")

    negativo = texto.startswith("-")
    texto = texto.lstrip("-+").replace("R$", "").strip()

    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")

    try:
        valor = Decimal(texto)
    except InvalidOperation:
        return Decimal("0")

    return -valor if negativo else valor


def _decodificar(conteudo):
    if isinstance(conteudo, str):
        return conteudo

    for codificacao in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return conteudo.decode(codificacao)
        except UnicodeDecodeError:
            continue

    return conteudo.decode("utf-8", errors="replace")


def ler_csv_ocloset(conteudo):
    """Devolve a lista de dicionários do CSV, sem o rodapé."""
    texto = _decodificar(conteudo).lstrip("﻿")

    leitor = csv.DictReader(io.StringIO(texto), delimiter=";")

    if not leitor.fieldnames:
        raise ValueError("Arquivo CSV vazio.")

    cabecalho = [_texto(c).lstrip("﻿") for c in leitor.fieldnames]

    faltando = [c for c in COLUNAS_ESPERADAS if c not in cabecalho]
    if faltando:
        raise ValueError(
            "O arquivo não parece ser o CSV de Contas a Pagar do O Closet. "
            "Colunas ausentes: " + ", ".join(faltando)
        )

    linhas = []

    for linha in leitor:
        # Rodapé do relatório ("312 lançamento(s)") e linhas em branco: nenhuma
        # linha de lançamento real fica sem vencimento nem data de pagamento.
        tem_data = (
            _converter_data(linha.get("Vencimento"))
            or _converter_data(linha.get("Data pagamento"))
        )

        if not tem_data:
            continue

        linhas.append({k: _texto(v) for k, v in linha.items() if k})

    return linhas


def montar_registros_ocloset(
    linhas,
    cod_empresa,
    cod_filial,
    nome_filial,
    base_data="pagamento",
):
    """
    Traduz as linhas do CSV para os registros da tabela `importacoes`.

    Só entram os lançamentos com Status "Pago".

    base_data:
        "pagamento" -> Data pagamento (cai para Vencimento quando vazia)
        "vencimento" -> Vencimento
    """
    registros = []
    ignorados = []

    for indice, linha in enumerate(linhas, start=2):
        status = linha.get("Status", "")

        if status.lower() != STATUS_ACEITO:
            ignorados.append((indice, linha, f"Status '{status or '(vazio)'}'"))
            continue

        data_pagamento = _converter_data(linha.get("Data pagamento"))
        data_vencimento = _converter_data(linha.get("Vencimento"))

        if base_data == "vencimento":
            data = data_vencimento or data_pagamento
        else:
            data = data_pagamento or data_vencimento

        if not data:
            ignorados.append((indice, linha, "Sem data de pagamento nem vencimento"))
            continue

        valor = _converter_valor(linha.get("Valor"))

        if valor == 0:
            ignorados.append((indice, linha, "Valor zerado"))
            continue

        # É um arquivo de contas a PAGAR: o CSV traz tudo positivo, mas no fluxo
        # de caixa toda linha é saída. Inclusive as devoluções de venda, que são
        # classificadas como venda justamente para reduzir a receita.
        valor = -abs(valor)

        categoria = linha.get("Categoria", "")
        descricao = linha.get("Descrição", "")
        contato = linha.get("Contato", "")
        documento = linha.get("Documento", "")

        # O contato entra no histórico para permitir classificar por fornecedor
        # quando a linha vem sem categoria no CSV.
        historico = " | ".join(p for p in (categoria, descricao, contato) if p)

        if not historico:
            historico = "(sem descrição)"

        complemento = documento

        registros.append({
            "cod_empresa": cod_empresa,
            "cod_filial": cod_filial,
            "nome_filial": nome_filial,
            "conta_banco": linha.get("Conta bancária", "") or None,
            "data": data,
            "ano": data.year,
            "mes": data.month,
            "historico": historico,
            "valor": valor,
            "grupo": None,
            "conta": None,
            "descricao_conta": None,
            "complemento": complemento or None,
        })

    return registros, ignorados


def inserir_registros_ocloset(cur, registros):
    if not registros:
        return 0

    valores = [
        (
            r["cod_empresa"],
            r["cod_filial"],
            r["nome_filial"],
            r["conta_banco"],
            r["data"],
            r["ano"],
            r["mes"],
            r["historico"],
            r["valor"],
            r["grupo"],
            r["conta"],
            r["descricao_conta"],
            r["complemento"],
        )
        for r in registros
    ]

    execute_values(cur, """
        INSERT INTO importacoes (
            cod_empresa,
            cod_filial,
            nome_filial,
            conta_banco,
            data,
            ano,
            mes,
            historico,
            valor,
            grupo,
            conta,
            descricao_conta,
            complemento
        )
        VALUES %s
    """, valores)

    return len(registros)


def resumo_por_mes(registros):
    resumo = {}

    for r in registros:
        chave = (r["ano"], r["mes"])
        item = resumo.setdefault(chave, {"ano": r["ano"], "mes": r["mes"], "qtd": 0, "valor": Decimal("0")})
        item["qtd"] += 1
        item["valor"] += r["valor"]

    return [resumo[k] for k in sorted(resumo)]


def Importa_CSV_OCloset(
    arquivo_csv,
    cod_empresa_fixo,
    conn,
    cod_filial=1,
    base_data="pagamento",
    limpar_antes=False,
):
    """
    Lê o CSV, grava em `importacoes` e tenta classificar.
    Devolve um dicionário de auditoria para a tela.
    """
    from importa_web_postos import classificar_lancamentos_importados

    cod_empresa = str(cod_empresa_fixo).strip()

    conteudo = arquivo_csv.read() if hasattr(arquivo_csv, "read") else arquivo_csv
    linhas = ler_csv_ocloset(conteudo)

    if not linhas:
        raise ValueError("Nenhuma linha de lançamento encontrada no arquivo.")

    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT nome_filial
            FROM filiais
            WHERE cod_empresa = %s
              AND cod_filial = %s
        """, (cod_empresa, int(cod_filial)))

        linha_filial = cur.fetchone()

        if not linha_filial:
            raise ValueError(
                f"Filial {cod_filial} não cadastrada para a empresa {cod_empresa}."
            )

        nome_filial = linha_filial[0]

        registros, ignorados = montar_registros_ocloset(
            linhas,
            cod_empresa=cod_empresa,
            cod_filial=int(cod_filial),
            nome_filial=nome_filial,
            base_data=base_data,
        )

        if not registros:
            raise ValueError(
                "Nenhuma linha do arquivo se qualificou para importação "
                f"({len(ignorados)} descartada(s))."
            )

        if limpar_antes:
            cur.execute("DELETE FROM importacoes WHERE cod_empresa = %s", (cod_empresa,))

        total_importado = inserir_registros_ocloset(cur, registros)
        conn.commit()

    finally:
        cur.close()

    total_classificado = classificar_lancamentos_importados(cod_empresa, conn)

    return {
        "total_linhas_arquivo": len(linhas),
        "total_importado": total_importado,
        "total_ignorado": len(ignorados),
        "total_classificado": total_classificado,
        "total_valor": sum((r["valor"] for r in registros), Decimal("0")),
        "por_mes": resumo_por_mes(registros),
        "ignorados": [
            {
                "linha": indice,
                "descricao": dados.get("Descrição", ""),
                "valor": dados.get("Valor", ""),
                "motivo": motivo,
            }
            for indice, dados, motivo in ignorados
        ],
    }
