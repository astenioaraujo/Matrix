import os
import re
import tempfile
import pdfplumber
import psycopg2

from datetime import datetime
from decimal import Decimal, InvalidOperation
from psycopg2.extras import execute_values


def get_connection():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres.uaafkuovkzkozmscyapw",
        password="DataMatrix@1962#",
        host="aws-1-us-east-1.pooler.supabase.com",
        port=5432,
    )


def normalizar_texto(texto):
    texto = str(texto or "").strip().lower()
    texto = re.sub(r"\s+", " ", texto)
    return texto


def converter_decimal(valor):
    if valor is None:
        return Decimal("0")

    texto = str(valor).strip()

    if texto == "" or texto.lower() == "nan":
        return Decimal("0")

    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")

    try:
        return Decimal(texto)
    except InvalidOperation:
        try:
            return Decimal(str(float(valor)))
        except Exception:
            return Decimal("0")


def extrair_codigo_hierarquico(historico):
    texto = str(historico or "").strip()

    match = re.match(r"^(\d+(?:\.\d+)*)\s*[-–_]", texto)
    if match:
        return match.group(1)

    match = re.match(r"^(\d+(?:\.\d+)*)\b", texto)
    if match:
        return match.group(1)

    return ""


def montar_prefixos_sinteticos(codigos):
    """
    Exemplo:
    Se existe 2.3.4, então 2 e 2.3 são sintéticas.
    Isso evita comparar cada conta contra todas as outras.
    """
    prefixos = set()

    for codigo in codigos:
        partes = str(codigo or "").split(".")

        if len(partes) <= 1:
            continue

        for i in range(1, len(partes)):
            prefixos.add(".".join(partes[:i]))

    return prefixos


def carregar_filiais_importacao(cur, cod_empresa):
    cur.execute("""
        SELECT cod_filial, nome_filial, COALESCE(nome_filial_importacao, '')
        FROM filiais
        WHERE cod_empresa = %s
          AND ativo = true
    """, (cod_empresa,))

    filiais = {}

    for cod_filial, nome_filial, nome_importacao in cur.fetchall() or []:
        nome_norm = normalizar_texto(nome_importacao)

        if nome_norm:
            filiais[nome_norm] = {
                "cod_filial": int(cod_filial),
                "nome_filial": nome_filial,
            }

    return filiais


def detectar_filial_na_linha(linha, filiais_importacao):
    linha_norm = normalizar_texto(linha)

    if linha_norm.startswith("filial:"):
        nome_pdf = linha.split(":", 1)[1].strip()
        nome_pdf_norm = normalizar_texto(nome_pdf)

        filial = filiais_importacao.get(nome_pdf_norm)

        if not filial:
            raise ValueError(
                f"Filial NÃO encontrada para importação do PDF.\n"
                f"Nome no PDF='{nome_pdf}'\n\n"
                f"Verifique o campo 'nome_filial_importacao' na tabela FILIAIS."
            )

        return filial["cod_filial"], filial["nome_filial"]

    filial = filiais_importacao.get(linha_norm)

    if filial:
        return filial["cod_filial"], filial["nome_filial"]

    return None, None


def inserir_importacoes_em_lote(cur, registros):
    if not registros:
        return 0

    valores = [
        (
            r["cod_empresa"],
            r["cod_filial"],
            r["nome_filial"],
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


def classificar_lancamentos_importados(cod_empresa_fixo, conn):
    cod_empresa = str(cod_empresa_fixo).strip()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                ca.texto,
                ca.cod_grupo,
                ca.cod_conta,
                cg.descricao
            FROM classificacoes_automaticas ca
            LEFT JOIN contas_gerenciais cg
                   ON cg.cod_empresa = ca.cod_empresa
                  AND cg.cod_grupo = ca.cod_grupo
                  AND cg.cod_conta = ca.cod_conta
            WHERE ca.cod_empresa = %s
              AND TRIM(COALESCE(ca.texto, '')) <> ''
            ORDER BY LENGTH(TRIM(ca.texto)) DESC, LOWER(ca.texto)
        """, (cod_empresa,))

        classificacoes = []

        for texto, cod_grupo, cod_conta, descricao in cur.fetchall() or []:
            texto_norm = normalizar_texto(texto)

            if texto_norm:
                classificacoes.append((
                    texto_norm,
                    cod_grupo,
                    cod_conta,
                    descricao
                ))

        if not classificacoes:
            return 0

        cur.execute("""
            SELECT id_lancamento, COALESCE(historico, '')
            FROM importacoes
            WHERE cod_empresa = %s
              AND grupo IS NULL
              AND conta IS NULL
        """, (cod_empresa,))

        atualizacoes = []

        for id_lancamento, historico in cur.fetchall() or []:
            historico_norm = normalizar_texto(historico)

            if not historico_norm:
                continue

            for texto_norm, cod_grupo, cod_conta, descricao in classificacoes:
                if texto_norm in historico_norm:
                    atualizacoes.append((
                        cod_grupo,
                        cod_conta,
                        descricao,
                        id_lancamento,
                        cod_empresa,
                    ))
                    break

        if atualizacoes:
            cur.executemany("""
                UPDATE importacoes
                SET grupo = %s,
                    conta = %s,
                    descricao_conta = %s
                WHERE id_lancamento = %s
                  AND cod_empresa = %s
            """, atualizacoes)

        return len(atualizacoes)

    finally:
        cur.close()


def calcular_auditoria_movimento_pdf(conn, cod_empresa):
    cod_empresa = str(cod_empresa).strip()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT
                cod_filial,
                nome_filial,
                ano,
                mes,
                historico,
                valor
            FROM importacoes
            WHERE cod_empresa = %s
            ORDER BY cod_filial, ano, mes, historico
        """, (cod_empresa,))

        linhas = cur.fetchall() or []

    finally:
        cur.close()

    grupos = {}

    for cod_filial, nome_filial, ano, mes, historico, valor in linhas:
        chave = (
            int(cod_filial),
            str(nome_filial or ""),
            int(ano),
            int(mes),
        )

        if chave not in grupos:
            grupos[chave] = {
                "linhas": [],
                "recebimentos_pdf": Decimal("0"),
                "pagamentos_pdf": Decimal("0"),
            }

        valor_dec = Decimal(str(valor or 0))
        codigo = extrair_codigo_hierarquico(historico)
        historico_norm = normalizar_texto(historico)

        grupos[chave]["linhas"].append({
            "codigo": codigo,
            "valor": valor_dec,
        })

        if codigo == "1" and "receb" in historico_norm:
            grupos[chave]["recebimentos_pdf"] += valor_dec

        if codigo == "2" and "pag" in historico_norm:
            grupos[chave]["pagamentos_pdf"] += valor_dec

    auditoria = []
    total_pdf = Decimal("0")
    total_sistema = Decimal("0")

    for chave, dados in grupos.items():
        cod_filial, nome_filial, ano, mes = chave

        codigos = [
            item["codigo"]
            for item in dados["linhas"]
            if item["codigo"]
        ]

        prefixos_sinteticos = montar_prefixos_sinteticos(codigos)

        movimento_sistema = Decimal("0")

        for item in dados["linhas"]:
            codigo = item["codigo"]

            if not codigo:
                continue

            if codigo in prefixos_sinteticos:
                continue

            movimento_sistema += item["valor"]

        recebimentos_pdf = dados["recebimentos_pdf"]
        pagamentos_pdf = dados["pagamentos_pdf"]
        movimento_pdf = recebimentos_pdf + pagamentos_pdf
        diferenca = movimento_sistema - movimento_pdf

        total_pdf += movimento_pdf
        total_sistema += movimento_sistema

        auditoria.append({
            "cod_filial": cod_filial,
            "nome_filial": nome_filial,
            "ano": ano,
            "mes": mes,
            "recebimentos_pdf": recebimentos_pdf,
            "pagamentos_pdf": pagamentos_pdf,
            "movimento_pdf": movimento_pdf,
            "movimento_sistema": movimento_sistema,
            "diferenca": diferenca,
            "status": "OK" if abs(diferenca) < Decimal("0.01") else "DIVERGENTE",
        })

    return {
        "linhas": auditoria,
        "total_pdf": total_pdf,
        "total_sistema": total_sistema,
        "diferenca_total": total_sistema - total_pdf,
    }


def remover_contas_sinteticas_importadas(conn, cod_empresa):
    cod_empresa = str(cod_empresa).strip()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT id_lancamento, historico
            FROM importacoes
            WHERE cod_empresa = %s
        """, (cod_empresa,))

        linhas = cur.fetchall() or []

        codigos_por_id = {}

        for id_lancamento, historico in linhas:
            codigo = extrair_codigo_hierarquico(historico)

            if codigo:
                codigos_por_id[id_lancamento] = codigo

        prefixos_sinteticos = montar_prefixos_sinteticos(codigos_por_id.values())

        ids_sinteticas = [
            id_lancamento
            for id_lancamento, codigo in codigos_por_id.items()
            if codigo in prefixos_sinteticos
        ]

        if ids_sinteticas:
            cur.execute("""
                DELETE FROM importacoes
                WHERE cod_empresa = %s
                  AND id_lancamento = ANY(%s)
            """, (cod_empresa, ids_sinteticas))

        return len(ids_sinteticas)

    finally:
        cur.close()


def somar_importacoes_empresa(conn, cod_empresa):
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT COALESCE(SUM(valor), 0)
            FROM importacoes
            WHERE cod_empresa = %s
        """, (str(cod_empresa).strip(),))

        total = cur.fetchone()[0]
        return Decimal(str(total or 0))

    finally:
        cur.close()


def Importa_Fluxo_Caixa_PDF(
    arquivo_pdf,
    cod_empresa_fixo,
    importar_colunas="ultima"
):
    if not cod_empresa_fixo:
        raise ValueError("Cod_empresa da sessão não foi informado.")

    cod_empresa = str(cod_empresa_fixo).strip()

    nome_original = arquivo_pdf.filename or "fluxo_caixa.pdf"
    nome_seguro = re.sub(r"[^a-zA-Z0-9_.-]", "_", nome_original)

    caminho_temp = None
    conn = None
    cur = None

    total_importado = 0
    registros_pendentes = []
    tamanho_lote = 300

    meses_map = {
        "jan": 1,
        "fev": 2,
        "mar": 3,
        "abr": 4,
        "mai": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "set": 9,
        "out": 10,
        "nov": 11,
        "dez": 12,
    }

    regex_valor = r"-?\d{1,3}(?:\.\d{3})*,\d{2}|-?\d+,\d{2}"
    regex_conta = r"^\s*\d+(?:\.\d+)*\s*[-–_]"
    regex_meses = r"(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)\.?\s+(\d{4})"

    def limpar_historico(linha):
        texto = re.sub(r"#\s*\d+\s*,\s*\d+", "", linha)
        texto = re.sub(regex_valor, "", texto)
        texto = re.sub(r"\s+", " ", texto)
        return texto.strip()

    def flush_lote():
        nonlocal total_importado, registros_pendentes

        if registros_pendentes:
            total_importado += inserir_importacoes_em_lote(cur, registros_pendentes)
            registros_pendentes = []

    def gravar_linha_pdf(linha, cod_filial, nome_filial, meses_linha):
        linha = re.sub(r"\s+", " ", linha or "").strip()

        if not linha:
            return

        if not cod_filial or not nome_filial:
            return

        if not meses_linha:
            return

        valores_txt = re.findall(regex_valor, linha)

        if not valores_txt:
            return

        if importar_colunas == "ultima":
            valores_txt_usar = [valores_txt[-1]]
            meses_usar = [meses_linha[-1]]
        else:
            qtd = min(len(valores_txt), len(meses_linha))
            valores_txt_usar = valores_txt[-qtd:]
            meses_usar = meses_linha[-qtd:]

        historico = limpar_historico(linha)

        if not historico:
            return

        for idx, valor_txt in enumerate(valores_txt_usar):
            valor = converter_decimal(valor_txt)

            if abs(valor) < Decimal("0.01"):
                continue

            ano = meses_usar[idx]["ano"]
            mes = meses_usar[idx]["mes"]
            data_lancamento = datetime(ano, mes, 1).date().isoformat()

            registros_pendentes.append({
                "cod_empresa": cod_empresa,
                "cod_filial": cod_filial,
                "nome_filial": nome_filial,
                "data": data_lancamento,
                "ano": ano,
                "mes": mes,
                "historico": historico,
                "valor": valor,
                "grupo": None,
                "conta": None,
                "descricao_conta": None,
                "complemento": "Importado de PDF",
            })

            if len(registros_pendentes) >= tamanho_lote:
                flush_lote()

    try:
        with tempfile.NamedTemporaryFile(
            prefix="fluxo_caixa_",
            suffix=f"_{nome_seguro}",
            delete=False
        ) as tmp:
            caminho_temp = tmp.name
            arquivo_pdf.stream.seek(0)
            tmp.write(arquivo_pdf.read())

        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            DELETE FROM importacoes
            WHERE cod_empresa = %s
        """, (cod_empresa,))

        filiais_importacao = carregar_filiais_importacao(cur, cod_empresa)

        filial_atual = None
        nome_filial_atual = None
        meses_detectados = []

        with pdfplumber.open(caminho_temp) as pdf:
            for pagina in pdf.pages:
                texto = pagina.extract_text(x_tolerance=1, y_tolerance=3) or ""
                linhas = texto.splitlines()
                buffer_linha = ""

                for linha_original in linhas:
                    linha = re.sub(r"\s+", " ", linha_original.strip())

                    if not linha:
                        continue

                    linha_lower = linha.lower()

                    cod_filial_detectada, nome_filial_detectada = detectar_filial_na_linha(
                        linha,
                        filiais_importacao
                    )

                    if cod_filial_detectada is not None:
                        if buffer_linha:
                            gravar_linha_pdf(
                                buffer_linha,
                                filial_atual,
                                nome_filial_atual,
                                meses_detectados
                            )

                        filial_atual = cod_filial_detectada
                        nome_filial_atual = nome_filial_detectada
                        buffer_linha = ""
                        continue

                    meses_encontrados = re.findall(regex_meses, linha_lower)

                    if meses_encontrados:
                        if buffer_linha:
                            gravar_linha_pdf(
                                buffer_linha,
                                filial_atual,
                                nome_filial_atual,
                                meses_detectados
                            )

                        meses_detectados = [
                            {
                                "mes": meses_map[mes_txt],
                                "ano": int(ano_txt),
                            }
                            for mes_txt, ano_txt in meses_encontrados
                        ]

                        buffer_linha = ""
                        continue

                    if filial_atual is None or not meses_detectados:
                        continue

                    if "saldo inicial" in linha_lower or "saldo final" in linha_lower:
                        if buffer_linha:
                            gravar_linha_pdf(
                                buffer_linha,
                                filial_atual,
                                nome_filial_atual,
                                meses_detectados
                            )

                        buffer_linha = ""
                        continue

                    if linha_lower.startswith(("total ", "totais ")):
                        if buffer_linha:
                            gravar_linha_pdf(
                                buffer_linha,
                                filial_atual,
                                nome_filial_atual,
                                meses_detectados
                            )

                        buffer_linha = ""
                        continue

                    if re.match(regex_conta, linha):
                        if buffer_linha:
                            gravar_linha_pdf(
                                buffer_linha,
                                filial_atual,
                                nome_filial_atual,
                                meses_detectados
                            )

                        buffer_linha = linha
                    else:
                        if buffer_linha:
                            buffer_linha = f"{buffer_linha} {linha}"
                        else:
                            buffer_linha = linha

                if buffer_linha:
                    gravar_linha_pdf(
                        buffer_linha,
                        filial_atual,
                        nome_filial_atual,
                        meses_detectados
                    )

        flush_lote()

        auditoria_movimento = calcular_auditoria_movimento_pdf(conn, cod_empresa)

        qtd_sinteticas_removidas = remover_contas_sinteticas_importadas(
            conn,
            cod_empresa
        )

        total_classificado = classificar_lancamentos_importados(
            cod_empresa_fixo=cod_empresa,
            conn=conn
        )

        soma_importada = somar_importacoes_empresa(conn, cod_empresa)

        conn.commit()

        return {
            "total_importado": total_importado,
            "total_sinteticas_removidas": qtd_sinteticas_removidas,
            "total_classificado": total_classificado,
            "auditoria_movimento": auditoria_movimento["linhas"],
            "total_pdf": auditoria_movimento["total_pdf"],
            "total_sistema": auditoria_movimento["total_sistema"],
            "diferenca_total": auditoria_movimento["diferenca_total"],
            "saldo_final_pdf": auditoria_movimento["total_pdf"],
            "soma_importada": soma_importada,
            "diferenca_saldo": auditoria_movimento["diferenca_total"],
        }

    except Exception:
        if conn:
            conn.rollback()
        raise

    finally:
        if cur:
            cur.close()

        if conn:
            conn.close()

        if caminho_temp and os.path.exists(caminho_temp):
            os.remove(caminho_temp)


# Compatibilidade: mantidas apenas para não quebrar imports antigos.
# Como você informou que não usa mais Excel, elas ficam desativadas.
def Importa_Web_Postos(*args, **kwargs):
    raise ValueError("Importação Excel desativada. Use Importa_Fluxo_Caixa_PDF.")


def Importa_Web_Postos_Arquivos(*args, **kwargs):
    raise ValueError("Importação Excel desativada. Use Importa_Fluxo_Caixa_PDF.")