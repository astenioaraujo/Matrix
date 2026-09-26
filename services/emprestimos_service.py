import calendar
from datetime import date


def somar_meses(data, quantidade):
    """Anda `quantidade` meses a partir de `data`, mantendo o dia-base (dia da
    primeira parcela). Dia inexistente no mês de chegada (ex.: base dia 31 e
    o mês só tem 30) cai no último dia daquele mês."""
    mes_total = data.month - 1 + quantidade
    ano = data.year + mes_total // 12
    mes = mes_total % 12 + 1
    dia = min(data.day, calendar.monthrange(ano, mes)[1])
    return date(ano, mes, dia)


def gerar_parcelas(valor_parcela, quantidade_parcelas, data_primeiro_vencimento, meses_carencia=0):
    """Sugestão de parcelas: cada linha sem carência recebe o mesmo
    `valor_parcela` de principal — normalmente a divisão simples do valor
    contratado, mas o chamador pode informar o valor real da parcela quando
    o banco embute uma taxa e ela não bate com a divisão pura (a tela deixa
    editar esse valor antes de gerar). juros começa em zero — cada linha é
    editável depois, como célula de planilha. Vencimentos no mesmo dia do
    mês, a partir da data base.

    `meses_carencia` são as primeiras linhas geradas sem principal —
    quantidade_parcelas continua sendo o total de linhas, carência incluída.
    Não existe carência de juros: valor_juros nunca é calculado, nasce
    zerado em toda linha e é digitado à mão."""
    quantidade = max(int(quantidade_parcelas or 1), 1)
    carencia = min(max(int(meses_carencia or 0), 0), quantidade - 1)
    valor_parcela = round(float(valor_parcela or 0), 2)

    parcelas = []
    for numero in range(1, quantidade + 1):
        principal = 0.0 if numero <= carencia else valor_parcela
        parcelas.append({
            "numero_parcela": numero,
            "data_vencimento": somar_meses(data_primeiro_vencimento, numero - 1),
            "valor_principal": principal,
            "valor_juros": 0.0,
        })
    return parcelas
