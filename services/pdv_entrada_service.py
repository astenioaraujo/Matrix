"""
Entrada de Mercadorias do PDV Matrix.

Uma nota de entrada concluída produz efeitos em três subsistemas, sem misturar
responsabilidades:

    Nota Fiscal de Entrada  → registra a compra
    Estoque                 → entra a mercadoria, com o custo calculado
    Contas a Pagar          → as obrigações, parcela a parcela

E **a entrada da mercadoria não é o pagamento dela**: nada aqui toca o fluxo
de caixa. A saída de dinheiro só nasce quando o título for efetivamente pago.
"""


def ratear_frete(itens, valor_frete):
    """
    Distribui o frete entre os itens, proporcionalmente ao valor de cada um.

    Trabalha em centavos e joga a sobra de arredondamento no **último** item,
    para a soma dos rateios fechar exatamente com o frete — dois centavos
    perdidos aqui viram custo errado no estoque e diferença que ninguém acha
    depois.

    `itens` são dicionários com `_total` (valor do item). Grava `_frete` em
    cada um.
    """
    frete_centavos = int(round((valor_frete or 0) * 100))
    base_centavos = int(round(sum(i["_total"] for i in itens) * 100))

    if frete_centavos <= 0 or base_centavos <= 0:
        for item in itens:
            item["_frete"] = 0.0
        return

    acumulado = 0
    for indice, item in enumerate(itens):
        if indice == len(itens) - 1:
            parcela = frete_centavos - acumulado
        else:
            valor_centavos = int(round(item["_total"] * 100))
            parcela = frete_centavos * valor_centavos // base_centavos
            acumulado += parcela
        item["_frete"] = parcela / 100.0


def custo_unitario(item):
    """
    Custo que vai para o estoque: mercadoria + frete, por unidade.

    O documento é explícito — o custo considera não apenas o valor da
    mercadoria, mas os componentes que devam ser incorporados a ele, entre
    eles o frete vinculado à compra.
    """
    quantidade = float(item.get("_quantidade") or 0)
    if quantidade <= 0:
        return 0.0
    return round((item["_total"] + item.get("_frete", 0.0)) / quantidade, 4)


def gerar_titulos_pagar(cur, cod_empresa, id_nota, id_fornecedor, nome_fornecedor,
                        parcelas, documento=None):
    """
    Cria as obrigações da nota, uma linha por parcela.

    `parcelas` é a lista [{valor, data_vencimento}] já validada por quem
    chamou. Roda na transação da nota.
    """
    total = len(parcelas)
    for numero, parcela in enumerate(parcelas, start=1):
        cur.execute("""
            INSERT INTO pdv_titulos_pagar
                (cod_empresa, id_pdv_nota_entrada, id_pdv_fornecedor, nome_fornecedor,
                 numero_parcela, total_parcelas, valor, data_vencimento, documento, situacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'ABERTO')
        """, (cod_empresa, id_nota, id_fornecedor, nome_fornecedor,
              numero, total, parcela["valor"], parcela["data_vencimento"], documento))


def parcelar(valor_total, quantidade, primeira_data, intervalo_dias=30):
    """
    Sugestão de parcelamento: divide em centavos e joga a sobra na última
    parcela, de modo que a soma feche com o total da nota.

    Devolve [{numero, valor, data_vencimento}]. É só sugestão — a tela deixa
    editar valor e vencimento de cada uma.
    """
    from datetime import timedelta

    quantidade = max(int(quantidade or 1), 1)
    centavos = int(round((valor_total or 0) * 100))
    base = centavos // quantidade

    parcelas = []
    for numero in range(1, quantidade + 1):
        valor = base if numero < quantidade else centavos - base * (quantidade - 1)
        parcelas.append({
            "numero": numero,
            "valor": valor / 100.0,
            "data_vencimento": (primeira_data + timedelta(days=intervalo_dias * (numero - 1))).isoformat(),
        })
    return parcelas
