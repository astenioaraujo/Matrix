"""Importação da planilha "Sobras e Faltas de Caixa" (WebPostos).

Uma aba só, com blocos em sequência:

    Filial: NOME DA FILIAL
    Funcionário: MATRICULA - NOME
    Data | Turno | PDV | Sobra (R$) | Falta (R$) | Diferença (R$) | Acumulado (R$)
    <linhas de lançamento>
    (linha "Subtotal:")
    Funcionário: MATRICULA - NOME
    ...
    Filial: OUTRA FILIAL
    ...

Sobra vem sempre >= 0 e Falta sempre <= 0 — a diferença do dia é a soma das
duas, nunca persistida (calculada na consulta).
"""

import datetime
import re
from decimal import Decimal, InvalidOperation

import openpyxl
from psycopg2.extras import execute_values


def _normalizar(texto):
    texto = str(texto or "").strip().lower()
    texto = re.sub(r"\s+", " ", texto)
    return texto


def _decimal(valor):
    if valor is None or valor == "":
        return Decimal("0")
    try:
        return Decimal(str(valor))
    except InvalidOperation:
        return Decimal("0")


def _carregar_filiais_importacao(cur, cod_empresa):
    """Funciona com cursor comum ou RealDictCursor — a rota usa o segundo."""
    cur.execute("""
        SELECT cod_filial, nome_filial,
               COALESCE(nome_filial_importacao, '') AS nome_filial_importacao
        FROM filiais
        WHERE cod_empresa = %s
          AND ativo = true
    """, (cod_empresa,))

    filiais = {}
    for linha in cur.fetchall() or []:
        if isinstance(linha, dict):
            cod_filial = linha["cod_filial"]
            nome_filial = linha["nome_filial"]
            nome_importacao = linha["nome_filial_importacao"]
        else:
            cod_filial, nome_filial, nome_importacao = linha

        nome_norm = _normalizar(nome_importacao)
        if nome_norm:
            filiais[nome_norm] = {"cod_filial": int(cod_filial), "nome_filial": nome_filial}
    return filiais


def ler_planilha_faltas_sobras(arquivo):
    """Lê o arquivo (upload do Flask ou caminho) e devolve uma lista de
    registros crus, um por linha de lançamento, com o nome da filial tal
    como está na planilha (ainda sem casar com cod_filial)."""

    wb = openpyxl.load_workbook(arquivo, data_only=True)
    ws = wb.active

    registros = []
    filial_atual = None
    matricula_atual = None
    nome_atual = None

    for row in ws.iter_rows(min_row=1, values_only=True):
        a = row[0] if len(row) > 0 else None

        if isinstance(a, str) and a.strip().startswith("Filial:"):
            filial_atual = a.split(":", 1)[1].strip()
            matricula_atual = None
            nome_atual = None
            continue

        if isinstance(a, str) and a.strip().startswith("Funcionário:"):
            resto = a.split(":", 1)[1].strip()
            if "-" in resto:
                matricula_atual, nome_atual = resto.split("-", 1)
                matricula_atual = matricula_atual.strip()
                nome_atual = nome_atual.strip()
            else:
                matricula_atual = None
                nome_atual = resto.strip()
            continue

        if not isinstance(a, datetime.datetime) and not isinstance(a, datetime.date):
            continue

        if not filial_atual or not nome_atual:
            continue

        turno = row[1] if len(row) > 1 else None
        pdv = row[2] if len(row) > 2 else None
        sobra = _decimal(row[3] if len(row) > 3 else None)
        falta = _decimal(row[4] if len(row) > 4 else None)

        registros.append({
            "filial_planilha": filial_atual,
            "matricula": matricula_atual,
            "nome_funcionario": nome_atual,
            "data": a.date() if isinstance(a, datetime.datetime) else a,
            "turno": int(turno) if turno not in (None, "") else None,
            "pdv": str(pdv).strip() if pdv else None,
            "sobra": sobra,
            "falta": falta,
        })

    return registros


def importar_faltas_sobras(cur, cod_empresa, id_usuario, arquivo):
    """Importa a planilha para `caixas_faltas_sobras`, só das filiais da
    empresa logada (`filiais.nome_filial_importacao`). Filiais da planilha
    que não têm cadastro de importação são ignoradas (podem ser de outra
    empresa no mesmo arquivo) e relatadas no resultado.

    Reimportação substitui o período: por filial, apaga o intervalo de
    datas que veio na planilha e grava de novo — mesmo desenho das outras
    importações do WebPostos (o arquivo sempre traz o período fechado).
    """

    registros_crus = ler_planilha_faltas_sobras(arquivo)

    if not registros_crus:
        raise ValueError("A planilha não tem nenhuma linha de lançamento reconhecível.")

    filiais_importacao = _carregar_filiais_importacao(cur, cod_empresa)

    por_filial = {}
    filiais_ignoradas = set()

    for r in registros_crus:
        nome_norm = _normalizar(r["filial_planilha"])
        filial = filiais_importacao.get(nome_norm)

        if not filial:
            filiais_ignoradas.add(r["filial_planilha"])
            continue

        cod_filial = filial["cod_filial"]
        por_filial.setdefault(cod_filial, {
            "nome_filial": filial["nome_filial"],
            "registros": [],
        })["registros"].append(r)

    if not por_filial:
        raise ValueError(
            "Nenhuma filial da planilha bate com o cadastro desta empresa "
            "(campo 'nome_filial_importacao' em Filiais). "
            f"Filiais encontradas na planilha: {', '.join(sorted(filiais_ignoradas))}."
        )

    resumo_filiais = []
    total_importado = 0

    for cod_filial, dados in por_filial.items():
        regs = dados["registros"]
        datas = [r["data"] for r in regs]
        data_min, data_max = min(datas), max(datas)

        cur.execute("""
            DELETE FROM caixas_faltas_sobras
             WHERE cod_empresa = %s AND cod_filial = %s
               AND data BETWEEN %s AND %s
        """, (cod_empresa, cod_filial, data_min, data_max))

        linhas = [
            (
                cod_empresa, cod_filial, r["data"], r["turno"], r["pdv"],
                r["matricula"], r["nome_funcionario"], r["sobra"], r["falta"],
                id_usuario,
            )
            for r in regs
        ]

        execute_values(cur, """
            INSERT INTO caixas_faltas_sobras (
                cod_empresa, cod_filial, data, turno, pdv,
                matricula_funcionario, nome_funcionario, sobra, falta,
                id_usuario_importou
            ) VALUES %s
        """, linhas)

        total_importado += len(linhas)
        resumo_filiais.append({
            "cod_filial": cod_filial,
            "nome_filial": dados["nome_filial"],
            "qtd": len(linhas),
            "data_min": data_min,
            "data_max": data_max,
        })

    resumo_filiais.sort(key=lambda x: x["cod_filial"])

    return {
        "total_importado": total_importado,
        "filiais": resumo_filiais,
        "filiais_ignoradas": sorted(filiais_ignoradas),
    }


def _primeiro_nome(nome):
    partes = str(nome or "").split()
    return partes[0] if partes else ""


def nomes_exibicao(nomes_completos):
    """Mapa {nome_completo: rótulo curto} para cabeçalho de coluna.

    Usa o primeiro nome; quando duas pessoas do mesmo conjunto começam
    igual, estende com a palavra seguinte até desempatar.
    """

    nomes = sorted(set(nomes_completos))
    rotulo = {n: _primeiro_nome(n) for n in nomes}

    agrupado = {}
    for n, r in rotulo.items():
        agrupado.setdefault(r, []).append(n)

    for r, grupo in agrupado.items():
        if len(grupo) <= 1:
            continue
        for n in grupo:
            palavras = str(n).split()
            qtd = 2
            novo = " ".join(palavras[:qtd])
            while novo in rotulo.values() and qtd < len(palavras):
                qtd += 1
                novo = " ".join(palavras[:qtd])
            rotulo[n] = novo

    return rotulo
