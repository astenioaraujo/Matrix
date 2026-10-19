"""Análise de Vendas: os números saem de vendas_diarias, o diagnóstico sai da IA.

Duas metades de propósito. Tudo o que é número (litros, preço, custo, margem,
comparação com a rede e com o período anterior) é calculado aqui, em Python, e
é o que a tela mostra. A IA recebe esses mesmos números e escreve o
diagnóstico e as sugestões de preço — ela não faz conta que a tela exiba, para
que um número inventado não tenha por onde entrar.

Nada é gravado: a análise é refeita a cada clique.
"""

import json
import os
import threading
import time
import uuid
from collections import defaultdict
from datetime import timedelta

MODELO_ANALISE = "claude-opus-5-5"

# Variação do custo médio do dia que conta como "o custo mudou de patamar";
# abaixo disto é arredondamento da média, não compra nova.
VARIACAO_MINIMA_CUSTO = 0.03

# Faixa de preço que vendeu menos que isto no período é exceção (um abastecimento
# com desconto), não política de preço. E no máximo estas, as de maior volume.
FATIA_MINIMA_FAIXA = 0.04
MAXIMO_FAIXAS = 6

SITUACOES_POSTO = ["PRECO_BAIXO", "PRECO_ALTO", "CUSTO_ALTO", "EQUILIBRADO", "ATENCAO"]
ACOES_SUGESTAO = ["SUBIR_PRECO", "BAIXAR_PRECO", "MANTER", "REVISAR_CUSTO"]

# Jobs em memória, como o IMPORT_PROGRESS das importações: o gunicorn roda um
# worker só (ver gunicorn.conf.py), então a consulta do andamento cai no mesmo
# processo que iniciou a análise.
_JOBS = {}
_JOBS_LOCK = threading.Lock()
_VALIDADE_JOB_SEGUNDOS = 3600


def _zero():
    return {"q": 0.0, "v": 0.0, "q_custo": 0.0, "custo": 0.0}


def _somar(alvo, q, v, custo, custo_invalido):
    alvo["q"] += q
    alvo["v"] += v
    # Linha com custo fora de escala é venda de verdade (litros e valor
    # contam), mas o custo dela não entra na média.
    if not custo_invalido:
        alvo["q_custo"] += q
        alvo["custo"] += custo


def _metricas(a, dias):
    """Litros/dia, preço, custo e margem de um acumulado."""
    q = a["q"]
    if q <= 0:
        return None
    preco = a["v"] / q
    custo = (a["custo"] / a["q_custo"]) if a["q_custo"] > 0 else None
    margem_unit = (preco - custo) if custo is not None else None
    return {
        "litros": q,
        "litros_dia": q / dias,
        "valor": a["v"],
        "preco": preco,
        "custo": custo,
        "margem_unit": margem_unit,
        "margem_total": (margem_unit * q) if margem_unit is not None else None,
    }


def _variacao_pct(atual, anterior):
    if atual is None or anterior in (None, 0):
        return None
    return (atual - anterior) / abs(anterior) * 100


def _diferenca(a, b):
    if a is None or b is None:
        return None
    return a - b


def _mudancas(serie, minimo):
    """Datas em que o valor do dia mudou de patamar: [(data, valor), ...].

    `serie` é [(data, valor)]. O primeiro item é o patamar com que o período
    abriu.
    """
    mudancas = []
    ultimo = None
    for dia, valor in serie:
        if valor is None:
            continue
        if ultimo is None or abs(valor - ultimo) >= minimo - 1e-9:
            mudancas.append((dia, valor))
            ultimo = valor
    return mudancas


def _faixas_de_preco(precos_do_dia):
    """Os preços praticados no período, com a fatia de cada um e de quando a quando.

    vendas_diarias traz uma linha por preço praticado: o mesmo combustível sai
    no mesmo dia a vários preços (cartão, à vista, frota, convênio). O preço
    médio do dia oscila com a mistura sem ninguém ter mexido em nada; a
    mudança de preço de verdade aparece aqui, como uma faixa que deixa de
    vender num dia e outra que começa no seguinte.
    """
    faixas = {}
    total = 0.0
    for dia, por_preco in precos_do_dia.items():
        for preco, litros in por_preco.items():
            f = faixas.setdefault(preco, {"preco": preco, "litros": 0.0, "de": dia, "ate": dia})
            f["litros"] += litros
            f["de"] = min(f["de"], dia)
            f["ate"] = max(f["ate"], dia)
            total += litros

    if total <= 0:
        return []

    relevantes = [
        dict(f, fatia_pct=f["litros"] / total * 100)
        for f in faixas.values()
        if f["litros"] / total >= FATIA_MINIMA_FAIXA
    ]
    relevantes.sort(key=lambda f: -f["litros"])
    return sorted(relevantes[:MAXIMO_FAIXAS], key=lambda f: (f["de"], -f["preco"]))


def coletar_analise(cur, cod_empresa, data_ini, data_fim, filiais, produtos,
                    mapear_produto, fator_custo_invalido):
    """Monta os números da análise para o período e o período anterior.

    `filiais`: [{cod_filial, nome_filial}]; `produtos`: [{cod_produto,
    descricao}] na ordem de exibição; `mapear_produto`: descrição da venda ->
    cod_produto (None para o que não é combustível).
    """
    dias = (data_fim - data_ini).days + 1
    ant_fim = data_ini - timedelta(days=1)
    ant_ini = ant_fim - timedelta(days=dias - 1)

    cur.execute("""
        SELECT
            data,
            cod_filial,
            descricao,
            ROUND(preco_venda, 2) AS preco_venda,
            (preco_venda > 0 AND COALESCE(custo, 0) > preco_venda * %s) AS custo_invalido,
            SUM(quantidade) AS quantidade,
            SUM(valor) AS valor,
            SUM(COALESCE(custo, 0) * quantidade) AS custo_total
        FROM vendas_diarias
        WHERE cod_empresa = %s
          AND data BETWEEN %s AND %s
        GROUP BY 1, 2, 3, 4, 5
    """, (fator_custo_invalido, cod_empresa, ant_ini, data_fim))
    registros = cur.fetchall() or []

    nomes_filiais = {int(f["cod_filial"]): f["nome_filial"] for f in filiais}
    nomes_produtos = {str(p["cod_produto"]).strip(): p["descricao"] for p in produtos}

    atual = defaultdict(_zero)       # (filial, produto)
    anterior = defaultdict(_zero)
    por_dia = defaultdict(lambda: defaultdict(_zero))  # (filial, produto) -> dia
    # (filial, produto) -> dia -> preço praticado -> litros
    precos_do_dia = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    custos_invalidos = defaultdict(lambda: {"litros": 0.0, "dias": set()})
    dias_com_venda = set()

    for r in registros:
        cod_filial = int(r["cod_filial"])
        if cod_filial not in nomes_filiais:
            continue
        cod_produto = mapear_produto(r["descricao"])
        if not cod_produto or cod_produto not in nomes_produtos:
            continue

        q = float(r["quantidade"] or 0)
        v = float(r["valor"] or 0)
        custo = float(r["custo_total"] or 0)
        invalido = bool(r["custo_invalido"])
        chave = (cod_filial, cod_produto)

        if r["data"] >= data_ini:
            _somar(atual[chave], q, v, custo, invalido)
            _somar(por_dia[chave][r["data"]], q, v, custo, invalido)
            if q > 0 and r["preco_venda"]:
                precos_do_dia[chave][r["data"]][float(r["preco_venda"])] += q
            dias_com_venda.add(r["data"])
            if invalido:
                custos_invalidos[chave]["litros"] += q
                custos_invalidos[chave]["dias"].add(r["data"])
        else:
            _somar(anterior[chave], q, v, custo, invalido)

    def bloco(acum_atual, acum_anterior):
        m = _metricas(acum_atual, dias)
        a = _metricas(acum_anterior, dias)
        if not m:
            return None
        m["anterior"] = a
        m["var_volume_pct"] = _variacao_pct(m["litros_dia"], a["litros_dia"]) if a else None
        m["var_preco"] = _diferenca(m["preco"], a["preco"]) if a else None
        m["var_custo"] = _diferenca(m["custo"], a["custo"]) if a else None
        m["var_margem_unit"] = _diferenca(m["margem_unit"], a["margem_unit"]) if a else None
        m["var_margem_total_pct"] = (
            _variacao_pct(m["margem_total"], a["margem_total"]) if a else None
        )
        return m

    # Rede, por produto
    rede = []
    rede_por_produto = {}
    for p in produtos:
        cod_produto = str(p["cod_produto"]).strip()
        soma_atual, soma_anterior = _zero(), _zero()
        for (cod_filial, cp), a in atual.items():
            if cp == cod_produto:
                for k in soma_atual:
                    soma_atual[k] += a[k]
        for (cod_filial, cp), a in anterior.items():
            if cp == cod_produto:
                for k in soma_anterior:
                    soma_anterior[k] += a[k]
        m = bloco(soma_atual, soma_anterior)
        if not m:
            continue
        m["cod_produto"] = cod_produto
        m["produto"] = p["descricao"]
        rede.append(m)
        rede_por_produto[cod_produto] = m

    # Postos
    postos = []
    for f in filiais:
        cod_filial = int(f["cod_filial"])
        linhas = []
        total_atual, total_anterior = _zero(), _zero()

        for p in produtos:
            cod_produto = str(p["cod_produto"]).strip()
            chave = (cod_filial, cod_produto)
            if chave not in atual:
                continue
            m = bloco(atual[chave], anterior.get(chave, _zero()))
            if not m:
                continue

            for k in total_atual:
                total_atual[k] += atual[chave][k]
                total_anterior[k] += anterior.get(chave, _zero())[k]

            r = rede_por_produto.get(cod_produto)
            m["cod_produto"] = cod_produto
            m["produto"] = p["descricao"]
            m["dif_preco_rede"] = _diferenca(m["preco"], r["preco"]) if r else None
            m["dif_custo_rede"] = _diferenca(m["custo"], r["custo"]) if r else None
            m["dif_margem_rede"] = _diferenca(m["margem_unit"], r["margem_unit"]) if r else None

            serie = []
            for dia in sorted(por_dia[chave]):
                d = _metricas(por_dia[chave][dia], 1)
                if d:
                    serie.append((dia, d))
            m["faixas_preco"] = _faixas_de_preco(precos_do_dia[chave])
            m["mudancas_custo"] = _mudancas(
                [(dia, d["custo"]) for dia, d in serie], VARIACAO_MINIMA_CUSTO
            )
            m["litros_por_dia"] = [(dia, d["litros"]) for dia, d in serie]

            invalido = custos_invalidos.get(chave)
            m["custo_invalido"] = (
                {"litros": invalido["litros"], "dias": sorted(invalido["dias"])}
                if invalido else None
            )
            linhas.append(m)

        if not linhas:
            continue

        total = bloco(total_atual, total_anterior)
        postos.append({
            "cod_filial": cod_filial,
            "nome": nomes_filiais[cod_filial],
            "total": total,
            "produtos": linhas,
        })

    soma_atual, soma_anterior = _zero(), _zero()
    for a in atual.values():
        for k in soma_atual:
            soma_atual[k] += a[k]
    for a in anterior.values():
        for k in soma_anterior:
            soma_anterior[k] += a[k]

    return {
        "periodo": {"inicio": data_ini, "fim": data_fim, "dias": dias},
        "periodo_anterior": {"inicio": ant_ini, "fim": ant_fim, "dias": dias},
        "dias_com_venda": len(dias_com_venda),
        "rede_total": bloco(soma_atual, soma_anterior),
        "rede": rede,
        "postos": postos,
    }


# ---------------------------------------------------------------------------
# IA
# ---------------------------------------------------------------------------

# Degraus de preço para os quais a conta de empate vai pronta para a IA.
CENTAVOS_EMPATE = [5, 10, 15, 20, 25, 30]


def variacao_volume_empate(margem_unit, centavos, subir):
    """Quanto o volume pode mudar até a margem total do posto empatar.

    Subindo `centavos`, devolve quanto % do volume dá para perder e ainda
    ganhar o mesmo em reais: c / (m + c). Baixando, quanto % a mais é preciso
    vender para empatar: c / (m − c). None quando não há margem para a conta
    (margem zerada ou negativa, ou corte maior que a própria margem).
    """
    if margem_unit is None or margem_unit <= 0 or centavos <= 0:
        return None
    c = centavos / 100
    if subir:
        return c / (margem_unit + c) * 100
    if c >= margem_unit:
        return None
    return c / (margem_unit - c) * 100


def _empate_para_ia(margem_unit):
    if margem_unit is None or margem_unit <= 0:
        return None
    subir, baixar = {}, {}
    for c in CENTAVOS_EMPATE:
        perda = variacao_volume_empate(margem_unit, c, True)
        ganho = variacao_volume_empate(margem_unit, c, False)
        subir[str(c)] = _n(perda, 1)
        if ganho is not None:
            baixar[str(c)] = _n(ganho, 1)
    return {"subir_pode_perder_pct": subir, "baixar_precisa_ganhar_pct": baixar}


def _n(valor, casas=3):
    return None if valor is None else round(valor, casas)


def _d(dia):
    return dia.strftime("%d/%m")


def _resumo_para_ia(m):
    a = m.get("anterior") or {}
    return {
        "litros_dia": _n(m["litros_dia"], 0),
        "litros_dia_anterior": _n(a.get("litros_dia"), 0),
        "var_volume_pct": _n(m["var_volume_pct"], 1),
        "preco": _n(m["preco"]),
        "preco_anterior": _n(a.get("preco")),
        "custo": _n(m["custo"]),
        "custo_anterior": _n(a.get("custo")),
        "margem_unit": _n(m["margem_unit"]),
        "margem_unit_anterior": _n(a.get("margem_unit")),
        "margem_total": _n(m["margem_total"], 0),
        "margem_total_anterior": _n(a.get("margem_total"), 0),
    }


def montar_dados_ia(nome_empresa, analise):
    """O que vai para a IA: os mesmos números da tela, enxutos."""
    postos = []
    for p in analise["postos"]:
        produtos = []
        for m in p["produtos"]:
            item = {"produto": m["produto"]}
            item.update(_resumo_para_ia(m))
            item.update({
                "dif_preco_rede": _n(m["dif_preco_rede"]),
                "dif_custo_rede": _n(m["dif_custo_rede"]),
                "dif_margem_rede": _n(m["dif_margem_rede"]),
                "faixas_preco": [
                    {"preco": _n(f["preco"], 2), "fatia_pct": round(f["fatia_pct"]),
                     "de": _d(f["de"]), "ate": _d(f["ate"])}
                    for f in m["faixas_preco"]
                ],
                "mudancas_custo": [[_d(d), _n(v, 2)] for d, v in m["mudancas_custo"]],
                "litros_por_dia": [[_d(d), round(v)] for d, v in m["litros_por_dia"]],
            })
            empate = _empate_para_ia(m["margem_unit"])
            if empate:
                item["empate"] = empate
            if m["custo_invalido"]:
                item["custo_com_erro_de_digitacao"] = {
                    "litros": round(m["custo_invalido"]["litros"]),
                    "dias": [_d(d) for d in m["custo_invalido"]["dias"]],
                }
            produtos.append(item)

        postos.append({
            "cod_filial": p["cod_filial"],
            "nome": p["nome"],
            "total": _resumo_para_ia(p["total"]),
            "produtos": produtos,
        })

    rede = []
    for m in analise["rede"]:
        item = {"produto": m["produto"]}
        item.update(_resumo_para_ia(m))
        rede.append(item)

    per, ant = analise["periodo"], analise["periodo_anterior"]
    return {
        "empresa": nome_empresa,
        "periodo": f'{per["inicio"]:%d/%m/%Y} a {per["fim"]:%d/%m/%Y} ({per["dias"]} dias)',
        "periodo_anterior": f'{ant["inicio"]:%d/%m/%Y} a {ant["fim"]:%d/%m/%Y}',
        "rede_total": _resumo_para_ia(analise["rede_total"]) if analise["rede_total"] else None,
        "rede_por_produto": rede,
        "postos": postos,
    }


INSTRUCOES_ANALISTA = """\
Você é o analista de preços de uma rede de postos de combustível no Nordeste do \
Brasil. Quem lê é o dono da rede e a equipe que define o preço de bomba de cada \
posto. Eles querem saber onde mexer no preço, em quantos centavos e por quê.

O dilema que você analisa: preço alto demais derruba o volume e a margem total \
do posto cai; preço baixo demais entrega margem por litro sem que o volume a \
mais compense. O que interessa é a margem total em reais (margem por litro × \
litros), não o volume nem a margem por litro isoladamente.

Você recebe um JSON com o período analisado e o período imediatamente anterior, \
de mesma duração. Valores em reais por litro; `litros_dia` é a média diária; \
`margem_total` é em reais no período inteiro. Em cada posto e produto:
- `dif_*_rede` é a diferença para a média da rede no mesmo produto (positivo = \
o posto está acima da rede).
- `preco` é a média do período, misturando todas as faixas de preço do posto.
- `faixas_preco` são os preços efetivamente praticados: cada posto vende o mesmo \
combustível a vários preços no mesmo dia (cartão, à vista, frota, convênio). \
Cada faixa traz a fatia do volume e o primeiro e o último dia em que vendeu. \
Mudança de preço aparece como uma faixa que termina num dia e outra que começa \
em seguida (6,67 até 06/10 e 6,87 a partir de 06/10 é um aumento de 20 \
centavos naquela faixa). Faixa que cobre o período inteiro não mudou. É daí que \
saem frases como "a partir de 06/10"; a sugestão em centavos vale para as \
faixas em vigor no fim do período.
- `mudancas_custo` lista os dias em que o custo médio mudou de patamar; o \
primeiro item é o patamar com que o período abriu.
- `litros_por_dia` é o volume diário. Fim de semana vende diferente de dia útil: \
compare antes e depois de uma mudança de preço por dias equivalentes, sem tomar \
a queda de um domingo por reação a preço.
- `empate` é a conta que decide se mexer no preço compensa, já feita sobre a \
margem por litro atual. `subir_pode_perder_pct["15"] = 13.6` quer dizer: \
subindo 15 centavos, o posto pode perder até 13,6% do volume e ainda ganhar o \
mesmo em reais; acima disso, perde. `baixar_precisa_ganhar_pct["10"] = 9.1` quer \
dizer: baixando 10 centavos, precisa vender 9,1% a mais só para empatar. Compare \
com o que o volume fez de verdade: se o posto baixou o preço e o volume subiu \
menos que o empate pedia, o corte não se pagou.
- `custo_com_erro_de_digitacao` marca litros cujo custo veio absurdo do sistema \
de origem; eles já ficaram fora do custo médio. Mencione, sem analisar margem \
em cima deles.

Como raciocinar:
- Custo do posto bem acima da rede no mesmo produto costuma ser compra antiga \
ainda no tanque (ou frete maior). Diga isso; não é motivo, por si só, para subir \
o preço, mas explica a margem apertada.
- Preço subiu e o volume segurou: há espaço, talvez para mais. Preço subiu e o \
volume caiu mais do que a margem por litro ganhou: passou do ponto.
- Preço baixou (ou está abaixo da rede) e o volume não respondeu: está dando \
margem de graça; sugira subir.
- Margem por litro bem abaixo da rede com volume estável ou crescente é o \
candidato clássico a subir 5, 10 ou 20 centavos.
- Não conhece o preço dos concorrentes de cada cidade. Quando a leitura depender \
disso, diga em uma frase que vale conferir a concorrência local antes de mexer.

Regras da resposta:
- Use só os números do JSON. Não invente preço, volume nem data.
- Cite os números que sustentam cada afirmação (preço, margem, variação de \
volume, data da mudança).
- Sugestão de preço em centavos inteiros, de 5 em 5 quando fizer sentido. \
`centavos` é sempre positivo; a direção vem de `acao`. Use 0 em MANTER e \
REVISAR_CUSTO.
- Toda sugestão de subir ou baixar cita a conta de empate no `motivo`, em \
palavras ("subindo 15 centavos, o posto pode perder até 14% do volume e ainda \
empata"), ao lado do que o volume já fez no período. É o que dá segurança a quem \
vai mexer na bomba.
- Uma sugestão por produto que mereça; produto sem nada a dizer fica de fora. \
Posto sem nada a mexer recebe situação EQUILIBRADO e nenhuma sugestão.
- Inclua todos os postos do JSON, com o mesmo `cod_filial`.
- Português do Brasil, direto, sem jargão de consultoria. Frases completas, \
curtas. Valores como R$ 5,89 e variações como -4,2%.
- `resumo_geral`: um parágrafo com o que aconteceu na rede no período e a \
principal recomendação. `diagnostico_rede`: de 3 a 6 pontos que valem para a \
rede ou para grupos de postos, começando pelo de maior impacto em reais.
"""

ESQUEMA_RESPOSTA = {
    "type": "object",
    "properties": {
        "resumo_geral": {"type": "string"},
        "diagnostico_rede": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "titulo": {"type": "string"},
                    "texto": {"type": "string"},
                },
                "required": ["titulo", "texto"],
                "additionalProperties": False,
            },
        },
        "postos": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "cod_filial": {"type": "integer"},
                    "situacao": {"type": "string", "enum": SITUACOES_POSTO},
                    "diagnostico": {"type": "string"},
                    "sugestoes": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "produto": {"type": "string"},
                                "acao": {"type": "string", "enum": ACOES_SUGESTAO},
                                "centavos": {"type": "integer"},
                                "motivo": {"type": "string"},
                            },
                            "required": ["produto", "acao", "centavos", "motivo"],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": ["cod_filial", "situacao", "diagnostico", "sugestoes"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["resumo_geral", "diagnostico_rede", "postos"],
    "additionalProperties": False,
}


class ErroAnaliseIA(Exception):
    """Falha com mensagem pronta para a tela."""


def ia_configurada():
    return bool(os.getenv("ANTHROPIC_API_KEY"))


def pedir_diagnostico(dados_ia):
    """Chama o Claude e devolve o dicionário no formato de ESQUEMA_RESPOSTA."""
    import anthropic

    if not ia_configurada():
        raise ErroAnaliseIA(
            "A chave da IA não está configurada no servidor (ANTHROPIC_API_KEY)."
        )

    client = anthropic.Anthropic()

    try:
        with client.beta.messages.stream(
            model=MODELO_ANALISE,
            max_tokens=32000,
            output_config={
                "effort": "high",
                "format": {"type": "json_schema", "schema": ESQUEMA_RESPOSTA},
            },
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
            system=INSTRUCOES_ANALISTA,
            messages=[{
                "role": "user",
                "content": json.dumps(dados_ia, ensure_ascii=False, separators=(",", ":")),
            }],
        ) as stream:
            resposta = stream.get_final_message()
    except anthropic.AuthenticationError:
        raise ErroAnaliseIA("A chave da IA foi recusada (ANTHROPIC_API_KEY inválida).")
    except anthropic.RateLimitError:
        raise ErroAnaliseIA("A IA está no limite de uso agora. Tente de novo em um minuto.")
    except anthropic.APIStatusError as e:
        raise ErroAnaliseIA(f"A IA devolveu um erro ({e.status_code}). Tente de novo.")
    except anthropic.APIConnectionError:
        raise ErroAnaliseIA("Não foi possível falar com a IA. Confira a conexão do servidor.")

    if resposta.stop_reason == "refusal":
        raise ErroAnaliseIA("A IA não respondeu a esta análise.")
    if resposta.stop_reason == "max_tokens":
        raise ErroAnaliseIA("A resposta da IA ficou grande demais. Tente um período menor.")

    texto = next((b.text for b in resposta.content if b.type == "text"), "")
    try:
        return json.loads(texto)
    except ValueError:
        raise ErroAnaliseIA("A IA devolveu uma resposta que não pôde ser lida. Tente de novo.")


def anexar_empate(resultado, dados_ia):
    """Grava em cada sugestão de subir/baixar a conta de empate, feita aqui.

    A IA cita a conta no texto, mas o número que a tela destaca é este,
    calculado sobre a margem do Matrix — não o que a IA escreveu.
    """
    margens = {}
    for p in dados_ia["postos"]:
        for item in p["produtos"]:
            margens[(p["cod_filial"], item["produto"].strip().upper())] = item["margem_unit"]

    for p in resultado.get("postos", []):
        for s in p.get("sugestoes", []):
            if s.get("acao") not in ("SUBIR_PRECO", "BAIXAR_PRECO"):
                continue
            margem = margens.get((p.get("cod_filial"), str(s.get("produto", "")).strip().upper()))
            centavos = int(s.get("centavos") or 0)
            subir = s["acao"] == "SUBIR_PRECO"
            pct = variacao_volume_empate(margem, centavos, subir)
            if pct is None:
                if not subir and margem is not None and centavos > 0:
                    s["empate"] = "O corte é maior que a margem por litro: vende abaixo do custo."
                continue
            numero = f"{pct:.1f}".replace(".", ",")
            s["empate"] = (
                f"Empata se o volume cair até {numero}%."
                if subir else
                f"Precisa vender {numero}% a mais só para empatar."
            )


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

def iniciar_job(dono, dados_ia):
    """Dispara a chamada da IA em segundo plano e devolve o id do job.

    `dono` identifica quem pode consultar o resultado.
    """
    job_id = uuid.uuid4().hex
    agora = time.time()

    with _JOBS_LOCK:
        for antigo in [j for j, v in _JOBS.items()
                       if agora - v["criado"] > _VALIDADE_JOB_SEGUNDOS]:
            del _JOBS[antigo]
        _JOBS[job_id] = {"dono": dono, "status": "processando", "criado": agora}

    def trabalhar():
        try:
            resultado = pedir_diagnostico(dados_ia)
            anexar_empate(resultado, dados_ia)
            atualizacao = {"status": "concluido", "resultado": resultado}
        except ErroAnaliseIA as e:
            atualizacao = {"status": "erro", "mensagem": str(e)}
        except Exception as e:
            atualizacao = {"status": "erro", "mensagem": f"Erro inesperado na análise: {e}"}
        with _JOBS_LOCK:
            if job_id in _JOBS:
                _JOBS[job_id].update(atualizacao)

    threading.Thread(target=trabalhar, daemon=True).start()
    return job_id


def consultar_job(job_id, dono):
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if not job or job["dono"] != dono:
            return None
        return {k: v for k, v in job.items() if k not in ("dono", "criado")}
