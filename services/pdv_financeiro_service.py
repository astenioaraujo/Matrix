"""
Financeiro / Fluxo de Caixa do PDV Matrix.

`pdv_lancamentos_financeiros` é o extrato da empresa e a estrutura central do
módulo: tudo o que representar entrada ou saída de dinheiro, direta ou
indiretamente, produz uma linha nela. É dela que saem os saldos, os extratos
por conta, o Caixa Geral e a conciliação bancária.

**Fluxo de Caixa é dinheiro.** Título a receber não é dinheiro; título a pagar
não é saída. O lançamento nasce no momento em que o valor efetivamente se
movimenta:

    Nota a Prazo → Títulos a Receber : NÃO lança (só reorganiza a obrigação)
    Nota ou Título → Baixa           : LANÇA (+) — entrou dinheiro
    Título a Pagar → Pagamento       : LANÇA (−) — saiu dinheiro
"""

from datetime import date


# Origens possíveis de um lançamento, para a tela saber de onde ele veio.
ORIGENS_LANCAMENTO = {
    "VENDA":         "Venda",
    "NOTA_PRAZO":    "Baixa de nota a prazo",
    "TITULO":        "Baixa de título a receber",
    "TITULO_PAGAR":  "Pagamento a fornecedor",
    "TRANSFERENCIA": "Transferência entre contas",
    "MANUAL":        "Lançamento manual",
}


def lancar(cur, cod_empresa, id_conta, data_lancamento, valor, historico,
           tipo_origem="MANUAL", id_origem=None, id_transferencia=None,
           id_usuario=None):
    """
    Grava um lançamento. Entrada positiva, saída negativa, como num extrato
    bancário. Recebe o cursor de quem chamou para rodar na mesma transação do
    fato que o originou (a baixa, o pagamento) — dinheiro registrado sem o
    fato, ou o contrário, é o que a conciliação nunca mais fecha.

    Devolve o id do lançamento.
    """
    cur.execute("""
        INSERT INTO pdv_lancamentos_financeiros
            (cod_empresa, id_pdv_conta_financeira, data_lancamento, valor,
             historico, tipo_origem, id_origem, id_transferencia, id_usuario)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id_pdv_lancamento
    """, (cod_empresa, id_conta, data_lancamento, valor, historico,
          tipo_origem, id_origem, id_transferencia, id_usuario))
    linha = cur.fetchone()
    return linha[0] if isinstance(linha, tuple) else linha["id_pdv_lancamento"]


def transferir(cur, cod_empresa, id_conta_origem, id_conta_destino, data_movimento,
               valor, historico=None, id_usuario=None):
    """
    Transferência entre contas: dois lançamentos amarrados.

        Conta de origem:  − valor
        Conta de destino: + valor

    Para a empresa não houve entrada nem saída — só mudou de lugar. As duas
    pernas compartilham `id_transferencia` (o id da primeira), que é o que
    permite ao Caixa Geral não ler movimento interno como receita ou despesa.
    """
    if id_conta_origem == id_conta_destino:
        raise ValueError("A conta de origem e a de destino têm que ser diferentes.")
    if valor <= 0:
        raise ValueError("O valor da transferência tem que ser maior que zero.")

    texto = historico or "Transferência entre contas"

    id_saida = lancar(cur, cod_empresa, id_conta_origem, data_movimento, -abs(valor),
                      texto, tipo_origem="TRANSFERENCIA", id_usuario=id_usuario)
    # as duas pernas apontam para o id da primeira
    cur.execute("""
        UPDATE pdv_lancamentos_financeiros SET id_transferencia = %s
        WHERE id_pdv_lancamento = %s
    """, (id_saida, id_saida))

    lancar(cur, cod_empresa, id_conta_destino, data_movimento, abs(valor), texto,
           tipo_origem="TRANSFERENCIA", id_transferencia=id_saida, id_usuario=id_usuario)

    return id_saida


def saldo_ate(cur, cod_empresa, id_conta, data_limite):
    """
    Saldo da conta imediatamente ANTES de `data_limite`: o saldo inicial do
    cadastro mais tudo o que se movimentou até ali.

    Não existe coluna de saldo em lugar nenhum — saldo é sempre calculado.
    """
    cur.execute("""
        SELECT saldo_inicial, data_saldo_inicial
        FROM pdv_contas_financeiras
        WHERE id_pdv_conta_financeira = %s AND cod_empresa = %s
    """, (id_conta, cod_empresa))
    conta = cur.fetchone()
    if not conta:
        return 0.0

    saldo = float(conta["saldo_inicial"] or 0)

    cur.execute("""
        SELECT COALESCE(SUM(valor), 0) AS total
        FROM pdv_lancamentos_financeiros
        WHERE cod_empresa = %s AND id_pdv_conta_financeira = %s
          AND data_lancamento < %s
    """, (cod_empresa, id_conta, data_limite))
    return round(saldo + float(cur.fetchone()["total"] or 0), 2)


def extrato_conta(cur, cod_empresa, id_conta, data_de, data_ate):
    """
    Extrato da conta no período:

        Saldo inicial + entradas − saídas = Saldo final
    """
    saldo_inicial = saldo_ate(cur, cod_empresa, id_conta, data_de)

    cur.execute("""
        SELECT id_pdv_lancamento, data_lancamento, valor, historico,
               tipo_origem, id_origem, id_transferencia, conciliado
        FROM pdv_lancamentos_financeiros
        WHERE cod_empresa = %s AND id_pdv_conta_financeira = %s
          AND data_lancamento BETWEEN %s AND %s
        ORDER BY data_lancamento, id_pdv_lancamento
    """, (cod_empresa, id_conta, data_de, data_ate))
    lancamentos = [dict(r) for r in cur.fetchall()]

    saldo = saldo_inicial
    entradas = 0.0
    saidas = 0.0
    for l in lancamentos:
        valor = float(l["valor"] or 0)
        saldo = round(saldo + valor, 2)
        l["saldo"] = saldo
        if valor >= 0:
            entradas += valor
        else:
            saidas += -valor

    return {
        "saldo_inicial": saldo_inicial,
        "lancamentos": lancamentos,
        "entradas": round(entradas, 2),
        "saidas": round(saidas, 2),
        "saldo_final": saldo,
    }


def caixa_geral(cur, cod_empresa, data_de, data_ate):
    """
    Posição consolidada: uma linha por conta financeira, com saldo inicial,
    entradas, saídas e saldo final, mais o total geral da empresa.

    As transferências internas aparecem em cada conta (os saldos individuais
    mudaram mesmo), mas são somadas à parte para que ninguém leia o total de
    entradas da empresa como receita: numa transferência, o que entrou numa
    conta saiu de outra.
    """
    cur.execute("""
        SELECT id_pdv_conta_financeira, nome, tipo
        FROM pdv_contas_financeiras
        WHERE cod_empresa = %s AND ativo
        ORDER BY ordem, nome
    """, (cod_empresa,))
    contas = [dict(r) for r in cur.fetchall()]

    linhas = []
    for conta in contas:
        id_conta = conta["id_pdv_conta_financeira"]
        saldo_inicial = saldo_ate(cur, cod_empresa, id_conta, data_de)

        cur.execute("""
            SELECT
                COALESCE(SUM(valor) FILTER (WHERE valor > 0), 0) AS entradas,
                COALESCE(-SUM(valor) FILTER (WHERE valor < 0), 0) AS saidas,
                COALESCE(SUM(valor) FILTER (WHERE valor > 0 AND id_transferencia IS NOT NULL), 0) AS entradas_transf,
                COALESCE(-SUM(valor) FILTER (WHERE valor < 0 AND id_transferencia IS NOT NULL), 0) AS saidas_transf
            FROM pdv_lancamentos_financeiros
            WHERE cod_empresa = %s AND id_pdv_conta_financeira = %s
              AND data_lancamento BETWEEN %s AND %s
        """, (cod_empresa, id_conta, data_de, data_ate))
        totais = cur.fetchone()

        entradas = float(totais["entradas"] or 0)
        saidas = float(totais["saidas"] or 0)
        linhas.append({
            **conta,
            "saldo_inicial": saldo_inicial,
            "entradas": entradas,
            "saidas": saidas,
            "entradas_transferencia": float(totais["entradas_transf"] or 0),
            "saidas_transferencia": float(totais["saidas_transf"] or 0),
            "saldo_final": round(saldo_inicial + entradas - saidas, 2),
        })

    total = {
        "saldo_inicial": round(sum(l["saldo_inicial"] for l in linhas), 2),
        "entradas": round(sum(l["entradas"] for l in linhas), 2),
        "saidas": round(sum(l["saidas"] for l in linhas), 2),
        "entradas_transferencia": round(sum(l["entradas_transferencia"] for l in linhas), 2),
        "saidas_transferencia": round(sum(l["saidas_transferencia"] for l in linhas), 2),
        "saldo_final": round(sum(l["saldo_final"] for l in linhas), 2),
    }
    return {"contas": linhas, "total": total}


# ─── BAIXAS ──────────────────────────────────────────────────────────────────
# É por aqui que o dinheiro entra e sai. Cada baixa grava o fato no seu módulo
# E o lançamento no fluxo de caixa, na mesma transação.

def baixar_nota_prazo(cur, cod_empresa, id_nota, id_conta, data_baixa, valor,
                      id_usuario=None):
    """
    Recebe uma nota a prazo. O cliente pode pagar por qualquer meio — isso não
    altera a venda original, onde a forma continua sendo "Nota a Prazo". O
    pagamento é uma operação nova, do Financeiro.
    """
    cur.execute("""
        SELECT valor, valor_baixado, situacao, nome_cliente, id_pdv_venda
        FROM pdv_notas_prazo
        WHERE id_pdv_nota_prazo = %s AND cod_empresa = %s
    """, (id_nota, cod_empresa))
    nota = cur.fetchone()
    if not nota:
        raise ValueError("Nota a prazo não encontrada.")
    if nota["situacao"] != "ABERTA":
        raise ValueError("Esta nota não está em aberto.")

    saldo = round(float(nota["valor"] or 0) - float(nota["valor_baixado"] or 0), 2)
    if valor <= 0 or round(valor, 2) > saldo:
        raise ValueError(f"Valor inválido. Saldo em aberto da nota: R$ {saldo:.2f}.")

    baixado = round(float(nota["valor_baixado"] or 0) + valor, 2)
    quitada = baixado >= round(float(nota["valor"] or 0), 2)

    cur.execute("""
        UPDATE pdv_notas_prazo
           SET valor_baixado = %s,
               situacao = %s,
               data_baixa = CASE WHEN %s THEN %s ELSE data_baixa END,
               id_pdv_conta_financeira = %s,
               atualizado_em = now()
         WHERE id_pdv_nota_prazo = %s AND cod_empresa = %s
    """, (baixado, "BAIXADA" if quitada else "ABERTA", quitada, data_baixa,
          id_conta, id_nota, cod_empresa))

    return lancar(cur, cod_empresa, id_conta, data_baixa, valor,
                  f"Recebimento de nota a prazo — {nota['nome_cliente'] or 'cliente'}",
                  tipo_origem="NOTA_PRAZO", id_origem=id_nota, id_usuario=id_usuario)


def converter_nota_em_titulos(cur, cod_empresa, id_nota, parcelas):
    """
    Transforma a nota a prazo em títulos a receber.

    **Não gera lançamento financeiro**: não houve entrada de dinheiro, apenas
    a reorganização de uma obrigação do cliente em parcelas com vencimento. A
    rastreabilidade título → nota → venda → vendedor tem que continuar de pé.
    """
    cur.execute("""
        SELECT valor, valor_baixado, situacao, id_pdv_cliente
        FROM pdv_notas_prazo
        WHERE id_pdv_nota_prazo = %s AND cod_empresa = %s
    """, (id_nota, cod_empresa))
    nota = cur.fetchone()
    if not nota:
        raise ValueError("Nota a prazo não encontrada.")
    if nota["situacao"] != "ABERTA":
        raise ValueError("Só uma nota em aberto pode ser convertida em títulos.")

    saldo = round(float(nota["valor"] or 0) - float(nota["valor_baixado"] or 0), 2)
    soma = round(sum(float(p["valor"]) for p in parcelas), 2)
    if int(round(soma * 100)) != int(round(saldo * 100)):
        raise ValueError(
            f"A soma dos títulos (R$ {soma:.2f}) tem que fechar com o saldo da nota "
            f"(R$ {saldo:.2f})."
        )

    total = len(parcelas)
    for numero, parcela in enumerate(parcelas, start=1):
        cur.execute("""
            INSERT INTO pdv_titulos_receber
                (cod_empresa, id_pdv_nota_prazo, id_pdv_cliente, numero_parcela,
                 total_parcelas, valor, data_vencimento, situacao)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'ABERTO')
        """, (cod_empresa, id_nota, nota["id_pdv_cliente"], numero, total,
              parcela["valor"], parcela["data_vencimento"]))

    cur.execute("""
        UPDATE pdv_notas_prazo SET situacao = 'CONVERTIDA', atualizado_em = now()
        WHERE id_pdv_nota_prazo = %s AND cod_empresa = %s
    """, (id_nota, cod_empresa))


def baixar_titulo_receber(cur, cod_empresa, id_titulo, id_conta, data_baixa, valor,
                          id_usuario=None):
    """Recebe um título. Entrou dinheiro, então lança."""
    cur.execute("""
        SELECT t.valor, t.valor_baixado, t.situacao, c.nome AS nome_cliente
        FROM pdv_titulos_receber t
        LEFT JOIN pdv_clientes c ON c.id_pdv_cliente = t.id_pdv_cliente
        WHERE t.id_pdv_titulo = %s AND t.cod_empresa = %s
    """, (id_titulo, cod_empresa))
    titulo = cur.fetchone()
    if not titulo:
        raise ValueError("Título não encontrado.")
    if titulo["situacao"] != "ABERTO":
        raise ValueError("Este título não está em aberto.")

    saldo = round(float(titulo["valor"] or 0) - float(titulo["valor_baixado"] or 0), 2)
    if valor <= 0 or round(valor, 2) > saldo:
        raise ValueError(f"Valor inválido. Saldo em aberto do título: R$ {saldo:.2f}.")

    baixado = round(float(titulo["valor_baixado"] or 0) + valor, 2)
    quitado = baixado >= round(float(titulo["valor"] or 0), 2)

    cur.execute("""
        UPDATE pdv_titulos_receber
           SET valor_baixado = %s, situacao = %s,
               data_baixa = CASE WHEN %s THEN %s ELSE data_baixa END,
               id_pdv_conta_financeira = %s, atualizado_em = now()
         WHERE id_pdv_titulo = %s AND cod_empresa = %s
    """, (baixado, "BAIXADO" if quitado else "ABERTO", quitado, data_baixa,
          id_conta, id_titulo, cod_empresa))

    return lancar(cur, cod_empresa, id_conta, data_baixa, valor,
                  f"Recebimento de título — {titulo['nome_cliente'] or 'cliente'}",
                  tipo_origem="TITULO", id_origem=id_titulo, id_usuario=id_usuario)


def baixar_titulo_pagar(cur, cod_empresa, id_titulo, id_conta, data_baixa, valor,
                        id_usuario=None):
    """
    Paga um título ao fornecedor. Saiu dinheiro, então lança **negativo** — é
    aqui, e só aqui, que a compra vira saída de caixa.
    """
    cur.execute("""
        SELECT valor, valor_baixado, situacao, nome_fornecedor, documento
        FROM pdv_titulos_pagar
        WHERE id_pdv_titulo_pagar = %s AND cod_empresa = %s
    """, (id_titulo, cod_empresa))
    titulo = cur.fetchone()
    if not titulo:
        raise ValueError("Título não encontrado.")
    if titulo["situacao"] != "ABERTO":
        raise ValueError("Este título não está em aberto.")

    saldo = round(float(titulo["valor"] or 0) - float(titulo["valor_baixado"] or 0), 2)
    if valor <= 0 or round(valor, 2) > saldo:
        raise ValueError(f"Valor inválido. Saldo em aberto do título: R$ {saldo:.2f}.")

    baixado = round(float(titulo["valor_baixado"] or 0) + valor, 2)
    quitado = baixado >= round(float(titulo["valor"] or 0), 2)

    cur.execute("""
        UPDATE pdv_titulos_pagar
           SET valor_baixado = %s, situacao = %s,
               data_baixa = CASE WHEN %s THEN %s ELSE data_baixa END,
               id_pdv_conta_financeira = %s, atualizado_em = now()
         WHERE id_pdv_titulo_pagar = %s AND cod_empresa = %s
    """, (baixado, "BAIXADO" if quitado else "ABERTO", quitado, data_baixa,
          id_conta, id_titulo, cod_empresa))

    return lancar(cur, cod_empresa, id_conta, data_baixa, -abs(valor),
                  (f"Pagamento a {titulo['nome_fornecedor'] or 'fornecedor'}"
                   + (f" — {titulo['documento']}" if titulo["documento"] else "")),
                  tipo_origem="TITULO_PAGAR", id_origem=id_titulo, id_usuario=id_usuario)
