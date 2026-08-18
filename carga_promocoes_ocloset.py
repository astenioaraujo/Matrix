"""
Carga de implantação: as promoções que já estavam valendo n'O Closet.

O arquivo de estoque não tem coluna de preço promocional, mas ele se revela:
quando `Valor em estoque (venda)` é menor que `Quantidade × Preço de venda`, a
peça está com desconto. O percentual sai daí:

    desconto = 1 − (valor_venda ÷ quantidade) ÷ preço_de_venda

Os percentuais reais do arquivo de 13/08/2026 são 25, 30, 35, 40, 50 e 55% —
alguns aparecem como 30,01 ou 54,99 por causa do arredondamento de centavos do
próprio arquivo, então o percentual é arredondado ao inteiro.

Tudo entra numa **campanha só**, porque `pdv_campanhas_itens` guarda o
percentual por item: cada peça fica com o desconto que era o dela.

    python3 carga_promocoes_ocloset.py <arquivo.csv> [--empresa EMP013]
                                       [--inicio AAAA-MM-DD] [--fim AAAA-MM-DD]
                                       [--nome "..."] [--aplicar]

Sem `--aplicar` só mostra o que faria.
"""

import argparse
import sys
from datetime import date

from psycopg2.extras import RealDictCursor

from db import get_connection
from services.pdv_campanhas_service import carregar_itens_por_percentual


def _numero(valor, padrao=0.0):
    v = (valor or "").strip()
    if not v:
        return padrao
    v = v.replace(".", "").replace(",", ".")
    try:
        return float(v)
    except ValueError:
        return padrao


def descontos_do_arquivo(caminho):
    """
    Devolve {sku: percentual} para as peças em promoção.

    Uma peça pode aparecer em mais de uma linha (uma por loja); o desconto é o
    mesmo nas duas, então a primeira que aparecer define.
    """
    import csv

    with open(caminho, encoding="utf-8-sig", newline="") as arquivo:
        linhas = list(csv.DictReader(arquivo, delimiter=";"))

    descontos = {}
    for linha in linhas:
        quantidade = _numero(linha.get("Quantidade"))
        preco = _numero(linha.get("Preço de venda"))
        valor = _numero(linha.get("Valor em estoque (venda)"))
        if quantidade <= 0 or preco <= 0:
            continue
        # sem diferença, não há promoção nenhuma nesta peça
        if abs(quantidade * preco - valor) <= 0.01:
            continue

        efetivo = valor / quantidade
        percentual = round((1 - efetivo / preco) * 100)
        if percentual <= 0 or percentual >= 100:
            continue
        descontos.setdefault((linha.get("SKU") or "").strip(), percentual)

    return descontos, len(linhas)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("arquivo")
    parser.add_argument("--empresa", default="EMP013")
    parser.add_argument("--nome", default=None)
    parser.add_argument("--inicio", default=None,
                        help="padrão: a data do nome do arquivo")
    parser.add_argument("--fim", default="2026-12-31")
    parser.add_argument("--aplicar", action="store_true")
    args = parser.parse_args()

    from services.pdv_importacao_estoque_service import data_do_nome
    referencia = args.inicio or (data_do_nome(args.arquivo) or date.today()).isoformat()
    nome = args.nome or f"Promoções vigentes em {referencia[8:10]}/{referencia[5:7]}/{referencia[:4]}"

    descontos, total_linhas = descontos_do_arquivo(args.arquivo)
    print(f"Arquivo: {args.arquivo} — {total_linhas} linhas")
    print(f"Peças em promoção: {len(descontos)} SKUs")

    faixas = {}
    for pct in descontos.values():
        faixas[pct] = faixas.get(pct, 0) + 1
    for pct in sorted(faixas):
        print(f"   {pct:3}%  {faixas[pct]:4} SKUs")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # do SKU para o produto já cadastrado
    cur.execute("""
        SELECT id_pdv_produto, sku FROM pdv_produtos
        WHERE cod_empresa = %s AND sku IS NOT NULL
    """, (args.empresa,))
    por_sku = {r["sku"]: r["id_pdv_produto"] for r in cur.fetchall()}

    itens = []
    sem_produto = []
    for sku, percentual in descontos.items():
        id_produto = por_sku.get(sku)
        if not id_produto:
            sem_produto.append(sku)
            continue
        itens.append({"id_pdv_produto": id_produto, "percentual": percentual})

    print(f"\nProdutos encontrados no cadastro: {len(itens)}")
    if sem_produto:
        print(f"SKUs sem produto cadastrado (ignorados): {len(sem_produto)}")
        print("   " + ", ".join(sem_produto[:10]))

    print(f"\nCampanha: {nome}")
    print(f"Período : {referencia} a {args.fim}")

    if not args.aplicar:
        print("\n(simulação — rode com --aplicar para gravar)")
        return 0

    if not itens:
        print("\nNada a gravar.")
        return 1

    try:
        cur.execute("""
            INSERT INTO pdv_campanhas
                (cod_empresa, nome, data_inicio, data_fim, percentual_desconto,
                 observacao, situacao)
            VALUES (%s, %s, %s, %s, 0, %s, 'ATIVA')
            RETURNING id_pdv_campanha
        """, (args.empresa, nome, referencia, args.fim,
              "Carga de implantação: descontos deduzidos do arquivo de estoque "
              f"({args.arquivo}). Cada item tem o percentual que já era o dele."))
        id_campanha = cur.fetchone()["id_pdv_campanha"]

        incluidos, atualizados = carregar_itens_por_percentual(
            cur, args.empresa, id_campanha, itens)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"\nErro: {e}")
        return 1
    finally:
        cur.close()

    print(f"\nCampanha {id_campanha} criada — {incluidos} itens incluídos, "
          f"{atualizados} atualizados.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
