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

    # Mesmo mecanismo de apelidos da importação de vendas/fluxo de caixa
    # (services/apelidos_filiais.py): aqui cobre o caso de "Sobras e Faltas
    # de Caixa" trazer o nome do posto mais completo do que o cadastrado em
    # nome_filial_importacao (ex. EMP012 LO/LO GNV, que a importação
    # principal registrou truncado em "COM"/"GN").
    from services.apelidos_filiais import apelidos_da_empresa

    for antigo, novo in apelidos_da_empresa(cod_empresa).items():
        filial = filiais.get(_normalizar(novo))
        if filial:
            filiais.setdefault(_normalizar(antigo), filial)

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


def _sincronizar_funcionarios(cur, cod_empresa, por_filial):
    """Cadastra em `caixas_faltas_sobras_funcionarios` quem apareceu na
    planilha e ainda não tinha cadastro (`UNIQUE (cod_empresa,
    nome_funcionario)`) — é assim que a transferência de filial passa a
    valer para ele. A identidade é pelo **nome**, não pela matrícula: nos
    dados reais (EMP012) a matrícula se repete entre postos — cada posto
    numera os próprios funcionários do zero, então não identifica ninguém
    na empresa toda. Funcionário já cadastrado não é alterado aqui: a
    filial dele passa a ser governada pela transferência, não pela
    planilha.

    Devolve avisos de possível homônimo: nome que aparece numa filial
    diferente da do cadastro (ou em mais de uma filial na própria
    planilha) sem nenhuma transferência registrada que explique a
    mudança — pode ser a mesma pessoa que mudou de posto sem passar pela
    tela de Transferências, ou duas pessoas diferentes com o mesmo nome.
    Não resolve sozinho (ficaria arriscado demais adivinhar): só avisa,
    pra alguém decidir — transferir de verdade, ou cadastrar o segundo
    nome diferenciado (ex. "CARLOS (FILIAL X)") na tela de Funcionários."""
    cur.execute("SELECT cod_filial, nome_filial FROM filiais WHERE cod_empresa = %s", (cod_empresa,))
    nome_filial_por_cod = {
        (f["cod_filial"] if isinstance(f, dict) else f[0]): (f["nome_filial"] if isinstance(f, dict) else f[1])
        for f in (cur.fetchall() or [])
    }

    filiais_por_nome = {}
    for cod_filial, dados in por_filial.items():
        for r in dados["registros"]:
            nome = (r["nome_funcionario"] or "").strip()
            if not nome:
                continue
            filiais_por_nome.setdefault(nome, set()).add(cod_filial)

    avisos = []
    for nome, filiais_planilha in filiais_por_nome.items():
        cur.execute("""
            SELECT id, cod_filial FROM caixas_faltas_sobras_funcionarios
             WHERE cod_empresa = %s AND nome_funcionario = %s
        """, (cod_empresa, nome))
        existente = cur.fetchone()

        if not existente:
            cod_filial_cadastro = sorted(filiais_planilha)[0]
            cur.execute("""
                INSERT INTO caixas_faltas_sobras_funcionarios (cod_empresa, nome_funcionario, cod_filial)
                VALUES (%s, %s, %s)
                ON CONFLICT (cod_empresa, nome_funcionario) DO NOTHING
            """, (cod_empresa, nome, cod_filial_cadastro))
            filiais_sem_explicacao = filiais_planilha - {cod_filial_cadastro}
        else:
            id_funcionario = existente["id"] if isinstance(existente, dict) else existente[0]
            cod_filial_cadastro = existente["cod_filial"] if isinstance(existente, dict) else existente[1]

            filiais_inesperadas = filiais_planilha - {cod_filial_cadastro}
            filiais_sem_explicacao = set()
            if filiais_inesperadas:
                cur.execute("""
                    SELECT DISTINCT cod_filial_destino
                      FROM caixas_faltas_sobras_funcionarios_movimentacoes
                     WHERE id_funcionario = %s
                """, (id_funcionario,))
                filiais_com_transferencia = {
                    (row["cod_filial_destino"] if isinstance(row, dict) else row[0])
                    for row in (cur.fetchall() or [])
                }
                filiais_sem_explicacao = filiais_inesperadas - filiais_com_transferencia

        if filiais_sem_explicacao:
            nomes_filiais = ", ".join(
                nome_filial_por_cod.get(cf, str(cf)) for cf in sorted(filiais_sem_explicacao)
            )
            avisos.append(
                f'"{nome}" apareceu também em {nomes_filiais}, filial diferente do cadastro '
                f"({nome_filial_por_cod.get(cod_filial_cadastro, cod_filial_cadastro)}), sem "
                "transferência registrada — confira se não são duas pessoas com o mesmo nome "
                "(tela Funcionários / Transferências)."
            )

    return avisos


def recalcular_filial_faltas_funcionario(cur, cod_empresa, id_funcionario):
    """Refaz o `cod_filial` de todo o histórico de `caixas_faltas_sobras`
    daquele funcionário (pelo nome), a partir da linha do tempo de
    `caixas_faltas_sobras_funcionarios_movimentacoes` — chamada depois de
    toda transferência, nunca na importação: a planilha continua marcando
    a filial de cada lançamento; é a transferência que manda a partir da
    data informada, para trás e para a frente, até a próxima transferência.
    Depende do mesmo nome vindo igual em todas as planilhas — nome
    divergente (duas pessoas com "CARLOS", por exemplo) não casa e precisa
    ser corrigido na origem (WebPostos)."""
    cur.execute("""
        SELECT nome_funcionario, cod_filial FROM caixas_faltas_sobras_funcionarios
         WHERE id = %s AND cod_empresa = %s
    """, (id_funcionario, cod_empresa))
    funcionario = cur.fetchone()
    if not funcionario:
        return 0

    if isinstance(funcionario, dict):
        nome = funcionario["nome_funcionario"]
        cod_filial_atual = funcionario["cod_filial"]
    else:
        nome, cod_filial_atual = funcionario

    nome = (nome or "").strip()
    if not nome:
        return 0

    cur.execute("""
        SELECT data_movimento, cod_filial_origem, cod_filial_destino
          FROM caixas_faltas_sobras_funcionarios_movimentacoes
         WHERE id_funcionario = %s
         ORDER BY data_movimento ASC, id ASC
    """, (id_funcionario,))
    movimentos = cur.fetchall() or []

    def _campo(linha, chave, indice):
        return linha[chave] if isinstance(linha, dict) else linha[indice]

    segmentos = []  # (data_inicio ou None, data_fim ou None, cod_filial)
    if not movimentos:
        segmentos.append((None, None, cod_filial_atual))
    else:
        primeiro_origem = _campo(movimentos[0], "cod_filial_origem", 1)
        if primeiro_origem is not None:
            segmentos.append((None, _campo(movimentos[0], "data_movimento", 0), primeiro_origem))
        for i, mov in enumerate(movimentos):
            inicio = _campo(mov, "data_movimento", 0)
            destino = _campo(mov, "cod_filial_destino", 2)
            fim = _campo(movimentos[i + 1], "data_movimento", 0) if i + 1 < len(movimentos) else None
            segmentos.append((inicio, fim, destino))

    total_alterado = 0
    for inicio, fim, cod_filial in segmentos:
        condicoes = ["cod_empresa = %s", "nome_funcionario = %s", "cod_filial <> %s"]
        parametros = [cod_empresa, nome, cod_filial]
        if inicio is not None:
            condicoes.append("data >= %s")
            parametros.append(inicio)
        if fim is not None:
            condicoes.append("data < %s")
            parametros.append(fim)

        cur.execute(f"""
            UPDATE caixas_faltas_sobras
               SET cod_filial = %s
             WHERE {' AND '.join(condicoes)}
        """, (cod_filial, *parametros))
        total_alterado += cur.rowcount

    return total_alterado


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

    avisos_homonimos = _sincronizar_funcionarios(cur, cod_empresa, por_filial)

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
        "avisos_homonimos": avisos_homonimos,
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
