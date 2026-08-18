"""
Importação do arquivo de estoque da loja (CSV).

Layout d'O Closet (`estoque-AAAA-MM-DD.csv`, separador `;`, UTF-8 com BOM):

    SKU;Produto;Marca;Categoria;Cor;Tamanho;Loja;Quantidade;Reservado;
    Disponível;Estoque mínimo;Estoque máximo;Abaixo do mínimo;Preço de venda;
    Preço de compra;Valor em estoque (custo);Valor em estoque (venda);
    Vendido 30d;Vendido 90d;Vendido 12 meses;Última venda

Colunas **ignoradas de propósito**, por serem calculadas ou virem de outro
lugar: `Disponível`, `Abaixo do mínimo`, os dois `Valor em estoque`, os três
`Vendido` e `Última venda` (esta sai da tabela de vendas).

> **Atenção ao "Valor em estoque (venda)"**: em 677 das 2.184 linhas do
> arquivo de 13/08/2026 ele NÃO é `quantidade × Preço de venda` — é menor,
> porque a peça está em promoção (descontos de 25% a 55%). O `preco_venda`
> importado é sempre o **preço cheio** da coluna "Preço de venda"; o preço
> promocional não tem campo próprio no PDV ainda.

O SKU **não é único no arquivo**: a mesma peça aparece uma vez por loja quando
tem estoque em mais de uma. Isso vira **um produto** (mesmo SKU, mesmo preço,
mesma cor e tamanho) com um movimento por linha, cada um no seu canal.

    "Loja Principal + E-commerce" → canal Loja Física
    "Loja Outlet"                 → canal Outlet
    "Recebimento (a conferir)"    → canal Loja Física (mercadoria já na loja,
                                    ainda não conferida; o histórico registra)

Como o saldo entra:

  * produto **sem nenhum movimento**: uma ENTRADA por linha do arquivo, no
    canal daquela linha — é a carga inicial.
  * produto **que já tem movimento**: um AJUSTE só, da diferença entre o que o
    arquivo diz e o saldo atual. É o que torna a reimportação segura: rodar o
    mesmo arquivo duas vezes não dobra o estoque.
"""

import csv
import io
from datetime import datetime


COLUNAS_OBRIGATORIAS = ["SKU", "Produto", "Loja", "Quantidade"]

# Como o valor da coluna "Loja" se traduz em canal de venda.
MAPA_CANAIS = {
    "loja principal + e-commerce": "Loja Física",
    "loja principal": "Loja Física",
    "e-commerce": "E-commerce",
    "loja outlet": "Outlet",
    "outlet": "Outlet",
    "recebimento (a conferir)": "Loja Física",
}


def _texto(valor, limite=None):
    v = (valor or "").strip()
    if not v:
        return None
    return v[:limite] if limite else v


def _numero(valor, padrao=0.0):
    """1.234,56 → 1234.56. Vazio ou ilegível vira o padrão."""
    v = (valor or "").strip()
    if not v:
        return padrao
    v = v.replace(".", "").replace(",", ".")
    try:
        return float(v)
    except ValueError:
        return padrao


def data_do_nome(nome_arquivo):
    """estoque-2026-08-13.csv → date(2026, 8, 13)."""
    import re
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", nome_arquivo or "")
    if not m:
        return None
    try:
        return datetime.strptime(m.group(0), "%Y-%m-%d").date()
    except ValueError:
        return None


def ler_csv(conteudo_bytes):
    """
    Devolve (linhas, avisos). Levanta ValueError se o layout não bater.

    Uma linha por registro do arquivo, já com os campos convertidos.
    """
    try:
        texto = conteudo_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        texto = conteudo_bytes.decode("latin-1")

    leitor = csv.DictReader(io.StringIO(texto), delimiter=";")
    cabecalho = [(c or "").strip() for c in (leitor.fieldnames or [])]

    faltando = [c for c in COLUNAS_OBRIGATORIAS if c not in cabecalho]
    if faltando:
        raise ValueError(
            "O arquivo não tem o layout esperado. Colunas ausentes: "
            + ", ".join(faltando)
        )

    linhas = []
    avisos = []
    for numero, bruta in enumerate(leitor, start=2):
        sku = _texto(bruta.get("SKU"))
        descricao = _texto(bruta.get("Produto"))
        if not sku or not descricao:
            avisos.append(f"Linha {numero} ignorada: sem SKU ou sem descrição.")
            continue

        loja = _texto(bruta.get("Loja")) or ""
        canal = MAPA_CANAIS.get(loja.strip().lower())
        if canal is None:
            avisos.append(f"Linha {numero}: loja \"{loja}\" desconhecida — entra sem canal.")

        linhas.append({
            "linha": numero,
            "sku": sku,
            "descricao": descricao,
            "marca": _texto(bruta.get("Marca")),
            "categoria": _texto(bruta.get("Categoria")),
            "cor": _texto(bruta.get("Cor")),
            "tamanho": _texto(bruta.get("Tamanho")),
            "loja": loja,
            "canal": canal,
            "quantidade": _numero(bruta.get("Quantidade")),
            "reservado": _numero(bruta.get("Reservado")),
            "estoque_minimo": _numero(bruta.get("Estoque mínimo")),
            "estoque_maximo": _numero(bruta.get("Estoque máximo")),
            "preco_venda": _numero(bruta.get("Preço de venda")),
            "preco_compra": _numero(bruta.get("Preço de compra")),
        })

    if not linhas:
        raise ValueError("O arquivo não tem nenhuma linha válida.")

    return linhas, avisos


def agrupar_por_sku(linhas):
    """
    Junta as linhas do mesmo SKU num produto só.

    Os dados descritivos vêm da primeira linha; quantidade e reservado somam;
    mínimo e máximo ficam com o maior valor informado. As quantidades por
    canal são preservadas, uma por linha de origem.
    """
    produtos = {}
    for linha in linhas:
        p = produtos.get(linha["sku"])
        if not p:
            p = {
                "sku": linha["sku"],
                "descricao": linha["descricao"],
                "marca": linha["marca"],
                "categoria": linha["categoria"],
                "cor": linha["cor"],
                "tamanho": linha["tamanho"],
                "preco_venda": linha["preco_venda"],
                "preco_compra": linha["preco_compra"],
                "reservado": 0.0,
                "estoque_minimo": 0.0,
                "estoque_maximo": 0.0,
                "quantidade": 0.0,
                "por_canal": [],
            }
            produtos[linha["sku"]] = p

        p["reservado"] += linha["reservado"]
        p["quantidade"] += linha["quantidade"]
        p["estoque_minimo"] = max(p["estoque_minimo"], linha["estoque_minimo"])
        p["estoque_maximo"] = max(p["estoque_maximo"], linha["estoque_maximo"])
        # preço de compra às vezes vem vazio numa das linhas
        if not p["preco_compra"] and linha["preco_compra"]:
            p["preco_compra"] = linha["preco_compra"]
        if linha["quantidade"]:
            p["por_canal"].append({
                "canal": linha["canal"],
                "loja": linha["loja"],
                "quantidade": linha["quantidade"],
            })
    return list(produtos.values())


def importar(cur, cod_empresa, cod_filial, produtos, data_referencia,
             canais_por_nome, id_usuario=None):
    """
    Grava os produtos e o estoque. Roda na transação de quem chamou.

    **Em lote, de propósito.** O arquivo tem ~2.200 produtos e o banco é
    remoto: uma consulta por produto (como faz `movimentar()`, que é a via
    correta para movimento avulso) sairia a milhares de idas e voltas e a
    importação não terminaria. Aqui tudo é resolvido em poucas instruções.

    A regra continua valendo: **nenhum saldo sem movimento**. Os movimentos
    são inseridos e o `quantidade_atual` é recalculado a partir deles, na
    mesma transação — o que muda é só a forma de gravar, não o princípio.

    Devolve o resumo da carga.
    """
    from psycopg2.extras import execute_values

    resumo = {"produtos_novos": 0, "produtos_atualizados": 0,
              "movimentos": 0, "pecas": 0.0}
    if not produtos:
        return resumo

    # 1. o que já existe, numa consulta só
    # o saldo que interessa é o desta loja
    cur.execute("""
        SELECT p.id_pdv_produto, p.sku,
               COALESCE(pf.quantidade_atual, 0) AS quantidade_atual
        FROM pdv_produtos p
        LEFT JOIN pdv_produtos_filiais pf
               ON pf.id_pdv_produto = p.id_pdv_produto
              AND pf.cod_empresa = p.cod_empresa AND pf.cod_filial = %s
        WHERE p.cod_empresa = %s AND p.sku IS NOT NULL
    """, (cod_filial, cod_empresa))
    existentes = {r["sku"]: {"id": r["id_pdv_produto"],
                             "saldo": float(r["quantidade_atual"] or 0)}
                  for r in cur.fetchall()}

    novos = [p for p in produtos if p["sku"] not in existentes]
    atualizar = [p for p in produtos if p["sku"] in existentes]

    # 2. produtos novos
    if novos:
        # fetch=True é obrigatório aqui: execute_values grava em páginas e, sem
        # ele, o RETURNING só traz a última — os produtos das páginas
        # anteriores ficariam sem id.
        inseridos = execute_values(cur, """
            INSERT INTO pdv_produtos
                (cod_empresa, sku, descricao, unidade, marca, categoria, cor,
                 tamanho, preco_venda, custo_atual, ultimo_preco_compra,
                 ativo, ordem)
            VALUES %s
            RETURNING id_pdv_produto, sku
        """, [(cod_empresa, p["sku"], p["descricao"], "UN", p["marca"], p["categoria"],
               p["cor"], p["tamanho"], p["preco_venda"], p["preco_compra"],
               p["preco_compra"], True, 10) for p in novos],
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            fetch=True)
        for linha in inseridos:
            existentes[linha["sku"]] = {"id": linha["id_pdv_produto"], "saldo": 0.0}
        resumo["produtos_novos"] = len(novos)

    # 3. produtos que já existiam: descrição e preços vêm do arquivo; o custo
    #    só é sobrescrito quando o arquivo traz preço de compra
    if atualizar:
        execute_values(cur, """
            UPDATE pdv_produtos AS p SET
                descricao = v.descricao, marca = v.marca, categoria = v.categoria,
                cor = v.cor, tamanho = v.tamanho, preco_venda = v.preco_venda,
                custo_atual = CASE WHEN v.preco_compra > 0 THEN v.preco_compra
                                   ELSE p.custo_atual END,
                ultimo_preco_compra = CASE WHEN v.preco_compra > 0 THEN v.preco_compra
                                           ELSE p.ultimo_preco_compra END,
                atualizado_em = now()
            FROM (VALUES %s) AS v(id_pdv_produto, descricao, marca, categoria, cor,
                                  tamanho, preco_venda, preco_compra)
            WHERE p.id_pdv_produto = v.id_pdv_produto
        """, [(existentes[p["sku"]]["id"], p["descricao"], p["marca"], p["categoria"],
               p["cor"], p["tamanho"], p["preco_venda"], p["preco_compra"])
              for p in atualizar],
            template="(%s,%s,%s,%s,%s,%s,%s::numeric,%s::numeric)")
        resumo["produtos_atualizados"] = len(atualizar)

    # 3b. reservado, mínimo e máximo são valores DA LOJA: vão para a linha da
    #     filial, criando-a quando ainda não existir
    execute_values(cur, """
        INSERT INTO pdv_produtos_filiais
            (cod_empresa, cod_filial, id_pdv_produto, quantidade_reservada,
             estoque_minimo, estoque_maximo, origem_inclusao)
        VALUES %s
        ON CONFLICT (cod_empresa, cod_filial, id_pdv_produto)
        DO UPDATE SET quantidade_reservada = EXCLUDED.quantidade_reservada,
                      estoque_minimo = EXCLUDED.estoque_minimo,
                      estoque_maximo = EXCLUDED.estoque_maximo,
                      atualizado_em = now()
    """, [(cod_empresa, cod_filial, existentes[p["sku"]]["id"], p["reservado"],
           p["estoque_minimo"], p["estoque_maximo"], "AUTOMATICA")
          for p in produtos],
        template="(%s,%s,%s,%s::numeric,%s::numeric,%s::numeric,%s)")

    # 4. quem já tem histórico de estoque (uma consulta só)
    cur.execute("""
        SELECT DISTINCT id_pdv_produto FROM pdv_estoque_movimentos
        WHERE cod_empresa = %s
    """, (cod_empresa,))
    com_historico = {r["id_pdv_produto"] for r in cur.fetchall()}

    # 5. monta os movimentos:
    #    sem histórico  → carga inicial, uma entrada por linha do arquivo, no
    #                     canal daquela linha
    #    com histórico  → só o ajuste da diferença, para reimportar não dobrar
    movimentos = []
    for p in produtos:
        id_produto = existentes[p["sku"]]["id"]
        if id_produto in com_historico:
            diferenca = round(p["quantidade"] - existentes[p["sku"]]["saldo"], 3)
            if not diferenca:
                continue
            id_canal = canais_por_nome.get(
                p["por_canal"][0]["canal"] if p["por_canal"] else None)
            movimentos.append((
                cod_empresa, cod_filial, id_produto, data_referencia, "AJUSTE",
                diferenca, p["preco_compra"], "IMPORTACAO", None,
                f"Acerto pelo arquivo de estoque "
                f"({existentes[p['sku']]['saldo']:g} → {p['quantidade']:g})",
                id_usuario, id_canal))
        else:
            for parcela in p["por_canal"]:
                movimentos.append((
                    cod_empresa, cod_filial, id_produto, data_referencia, "ENTRADA",
                    parcela["quantidade"], p["preco_compra"], "IMPORTACAO", None,
                    f"Carga do estoque — {parcela['loja']}",
                    id_usuario, canais_por_nome.get(parcela["canal"])))

    if movimentos:
        execute_values(cur, """
            INSERT INTO pdv_estoque_movimentos
                (cod_empresa, cod_filial, id_pdv_produto, data_movimento, tipo,
                 quantidade, custo_unitario, tipo_origem, id_origem, historico,
                 id_usuario, id_pdv_canal)
            VALUES %s
        """, movimentos,
            template="(%s,%s,%s,%s,%s,%s::numeric,%s::numeric,%s,%s,%s,%s,%s)")

        # o saldo sai dos movimentos recém-gravados, nunca de um número solto
        deltas = {}
        for mov in movimentos:
            deltas[mov[2]] = deltas.get(mov[2], 0) + mov[5]
        # execute_values só liga o placeholder do VALUES: empresa, filial e data
        # viajam dentro de cada tupla, não como parâmetros soltos
        execute_values(cur, """
            UPDATE pdv_produtos_filiais AS pf
               SET quantidade_atual = pf.quantidade_atual + v.delta,
                   ultimo_movimento_em = v.data_movimento,
                   atualizado_em = now()
            FROM (VALUES %s) AS v(cod_empresa, cod_filial, id_pdv_produto,
                                  delta, data_movimento)
            WHERE pf.id_pdv_produto = v.id_pdv_produto
              AND pf.cod_empresa = v.cod_empresa
              AND pf.cod_filial = v.cod_filial
        """, [(cod_empresa, cod_filial, id_produto, delta, data_referencia)
              for id_produto, delta in deltas.items()],
            template="(%s,%s::int,%s::int,%s::numeric,%s::date)",
            page_size=500)

        resumo["movimentos"] = len(movimentos)
        resumo["pecas"] = round(sum(m[5] for m in movimentos), 3)

    return resumo
