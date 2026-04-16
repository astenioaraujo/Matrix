import os
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
import psycopg2
import warnings
warnings.filterwarnings(
    "ignore",
    message="Workbook contains no default style, apply openpyxl's default"
)

COLUNA_HISTORICO_PADRAO = "A"
COLUNA_VALOR_PADRAO = "F"


def get_connection():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres.uaafkuovkzkozmscyapw",
        password="DataMatrix@1962#",
        host="aws-1-us-east-1.pooler.supabase.com",
        port=5432,
    )


def listar_arquivos_excel(diretorio):
    arquivos = []
    for nome in os.listdir(diretorio):
        if nome.startswith("~$"):
            continue
        if nome.lower().endswith((".xlsx", ".xls")):
            arquivos.append(os.path.join(diretorio, nome))
    arquivos.sort()
    return arquivos


def extrair_cod_filial_do_nome_arquivo(caminho_arquivo):
    nome = os.path.basename(caminho_arquivo)
    nome_sem_ext = os.path.splitext(nome)[0]

    match = re.search(r"\bV\s*(\d+)\b", nome_sem_ext, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))

    match = re.search(
        r"fluxo\s*de\s*caixa\s*-\s*(\d+)",
        nome_sem_ext,
        flags=re.IGNORECASE
    )
    if match:
        return int(match.group(1))

    match = re.match(r"^\s*(\d+)", nome_sem_ext)
    if match:
        return int(match.group(1))

    raise ValueError(f"Código da filial inválido no nome do arquivo: {nome}")


def buscar_nome_filial(conn, cod_empresa, cod_filial):
    cur = conn.cursor()
    cur.execute("""
        SELECT nome_filial
        FROM filiais
        WHERE cod_empresa = %s
          AND cod_filial = %s
          AND ativo = true
    """, (str(cod_empresa).strip(), int(cod_filial)))
    r = cur.fetchone()
    cur.close()

    if not r:
        raise ValueError(
            f"Filial não cadastrada ou inativa. Empresa={cod_empresa}, Filial={cod_filial}. "
            f"Cadastre a filial antes de importar o arquivo."
        )

    return r[0]


def converter_decimal(valor):
    if valor is None:
        return Decimal("0")

    texto = str(valor).strip()

    if texto == "" or texto.lower() == "nan":
        return Decimal("0")

    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    else:
        texto = texto

    try:
        return Decimal(texto)
    except InvalidOperation:
        try:
            return Decimal(str(float(valor)))
        except Exception:
            return Decimal("0")


def letra_para_indice_coluna(coluna):
    coluna = str(coluna or "").strip().upper()

    if not coluna or not coluna.isalpha():
        raise ValueError(f"Coluna inválida: {coluna}")

    resultado = 0
    for ch in coluna:
        resultado = resultado * 26 + (ord(ch) - ord("A") + 1)

    return resultado - 1


def ler_arquivo_excel(caminho_arquivo):
    if caminho_arquivo.lower().endswith(".xlsx"):
        xls = pd.ExcelFile(caminho_arquivo)
    else:
        xls = pd.ExcelFile(caminho_arquivo, engine="xlrd")

    nome_aba = xls.sheet_names[0]

    if caminho_arquivo.lower().endswith(".xlsx"):
        df = pd.read_excel(
            caminho_arquivo,
            sheet_name=nome_aba,
            dtype=str,
            header=None
        )
    else:
        df = pd.read_excel(
            caminho_arquivo,
            sheet_name=nome_aba,
            dtype=str,
            engine="xlrd",
            header=None
        )

    df = df.fillna("")
    return df


def obter_valor_por_letra(df, row, letra_coluna, caminho_arquivo):
    indice = letra_para_indice_coluna(letra_coluna)

    if indice >= len(df.columns):
        raise ValueError(
            f"A coluna '{letra_coluna}' não foi encontrada no arquivo {os.path.basename(caminho_arquivo)}"
        )

    return row.iloc[indice]


def inserir_importacao(cur, registro):
    cur.execute("""
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
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        registro["cod_empresa"],
        registro["cod_filial"],
        registro["nome_filial"],
        registro["data"],
        registro["ano"],
        registro["mes"],
        registro["historico"],
        registro["valor"],
        registro["grupo"],
        registro["conta"],
        registro["descricao_conta"],
        registro["complemento"],
    ))


def normalizar_texto(texto):
    texto = str(texto or "").strip().lower()
    texto = re.sub(r"\s+", " ", texto)
    return texto


def classificar_lancamentos_importados(cod_empresa_fixo, conn=None):
    if not cod_empresa_fixo:
        raise ValueError("Cod_empresa não informado para classificação.")

    cod_empresa = str(cod_empresa_fixo).strip()
    fechar_conexao = False

    if conn is None:
        conn = get_connection()
        fechar_conexao = True

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
        classificacoes = cur.fetchall()

        cur.execute("""
            SELECT
                id_lancamento,
                COALESCE(historico, '')
            FROM importacoes
            WHERE cod_empresa = %s
        """, (cod_empresa,))
        lancamentos = cur.fetchall()

        total_classificados = 0

        for id_lancamento, historico in lancamentos:
            historico_norm = normalizar_texto(historico)

            if not historico_norm:
                continue

            grupo_encontrado = None
            conta_encontrada = None
            descricao_conta_encontrada = None

            for texto_class, cod_grupo, cod_conta, descricao_conta in classificacoes:
                texto_norm = normalizar_texto(texto_class)

                if not texto_norm:
                    continue

                if texto_norm in historico_norm:
                    grupo_encontrado = cod_grupo
                    conta_encontrada = cod_conta
                    descricao_conta_encontrada = descricao_conta
                    break

            if grupo_encontrado is not None and conta_encontrada is not None:
                cur.execute("""
                    UPDATE importacoes
                    SET grupo = %s,
                        conta = %s,
                        descricao_conta = %s
                    WHERE id_lancamento = %s
                      AND cod_empresa = %s
                """, (
                    grupo_encontrado,
                    conta_encontrada,
                    descricao_conta_encontrada,
                    id_lancamento,
                    cod_empresa
                ))

                if cur.rowcount > 0:
                    total_classificados += 1

        if fechar_conexao:
            conn.commit()

        return total_classificados

    except Exception:
        if fechar_conexao:
            conn.rollback()
        raise

    finally:
        cur.close()
        if fechar_conexao:
            conn.close()


def extrair_codigo_hierarquico(historico):
    texto = str(historico or "").strip()

    match = re.match(r"^(\d+(?:\.\d+)*)\s*[-–]", texto)
    if match:
        return match.group(1)

    match = re.match(r"^(\d+(?:\.\d+)*)\b", texto)
    if match:
        return match.group(1)

    return ""


def montar_codigos_hierarquicos(df, caminho_arquivo):
    codigos = []

    for _, row in df.iterrows():
        historico = str(
            obter_valor_por_letra(df, row, COLUNA_HISTORICO_PADRAO, caminho_arquivo)
        ).strip()

        codigos.append(extrair_codigo_hierarquico(historico))

    return codigos


def eh_conta_sintetica(codigo_atual, codigo_proximo):
    if not codigo_atual or not codigo_proximo:
        return False

    return codigo_proximo.startswith(codigo_atual + ".")


def processar_dataframe(
    df,
    caminho_arquivo,
    data_lancamento,
    coluna_valor,
    cod_empresa_fixo,
    conn,
    callback_progresso=None,
    arquivo_index=1,
    total_arquivos=1,
    total_importado_atual=0
):
    cod_empresa = str(cod_empresa_fixo).strip()
    cod_filial = extrair_cod_filial_do_nome_arquivo(caminho_arquivo)
    nome_filial = buscar_nome_filial(conn, cod_empresa, cod_filial)

    total_linhas = len(df.index)
    importados = total_importado_atual

    cur = conn.cursor()
    codigos_hierarquicos = montar_codigos_hierarquicos(df, caminho_arquivo)
    data_obj = datetime.strptime(data_lancamento, "%Y-%m-%d")

    for idx_zero_based, (_, row) in enumerate(df.iterrows()):
        idx = idx_zero_based + 1

        historico = str(
            obter_valor_por_letra(df, row, COLUNA_HISTORICO_PADRAO, caminho_arquivo)
        ).strip()

        valor_bruto = obter_valor_por_letra(df, row, coluna_valor, caminho_arquivo)
        valor = converter_decimal(valor_bruto)

        codigo_atual = codigos_hierarquicos[idx_zero_based]

        codigo_proximo = ""
        for j in range(idx_zero_based + 1, len(codigos_hierarquicos)):
            if codigos_hierarquicos[j]:
                codigo_proximo = codigos_hierarquicos[j]
                break

        historico_limpo = historico.strip().lower()

        if historico_limpo == "":
            continue

        if historico_limpo == "saldo inicial":
            continue

        if abs(valor) < Decimal("0.01"):
            continue

        if eh_conta_sintetica(codigo_atual, codigo_proximo):
            continue

        registro = {
            "cod_empresa": cod_empresa,
            "cod_filial": cod_filial,
            "nome_filial": nome_filial,
            "data": data_lancamento,
            "ano": data_obj.year,
            "mes": data_obj.month,
            "historico": historico,
            "valor": valor,
            "grupo": None,
            "conta": None,
            "descricao_conta": None,
            "complemento": None,
        }

        inserir_importacao(cur, registro)
        importados += 1

        if callback_progresso and (idx % 25 == 0 or idx == total_linhas):
            percentual = int(
                (
                    ((arquivo_index - 1) + (idx / max(total_linhas, 1)))
                    / max(total_arquivos, 1)
                ) * 100
            )

            callback_progresso({
                "status": "rodando",
                "percentual": percentual,
                "mensagem": f"Importando arquivo {arquivo_index} de {total_arquivos}",
                "arquivo_atual": os.path.basename(caminho_arquivo),
                "filial_atual": nome_filial,
                "linha_atual": idx,
                "total_linhas_arquivo": total_linhas,
                "importados": importados
            })

    cur.close()
    return importados


def Importa_Web_Postos(
    diretorio,
    data_lancamento,
    coluna_valor=None,
    cod_empresa_fixo=None,
    callback_progresso=None
):
    if not cod_empresa_fixo:
        raise ValueError("Cod_empresa da sessão não foi informado.")

    coluna_valor = (coluna_valor or COLUNA_VALOR_PADRAO).strip().upper()

    arquivos = listar_arquivos_excel(diretorio)

    if not arquivos:
        raise ValueError("Nenhum arquivo Excel foi encontrado no diretório informado.")

    conn = get_connection()
    total_importado = 0
    total_classificado = 0

    try:
        total_arquivos = len(arquivos)

        for arquivo_index, caminho_arquivo in enumerate(arquivos, start=1):
            df = ler_arquivo_excel(caminho_arquivo)

            total_importado = processar_dataframe(
                df=df,
                caminho_arquivo=caminho_arquivo,
                data_lancamento=data_lancamento,
                coluna_valor=coluna_valor,
                cod_empresa_fixo=cod_empresa_fixo,
                conn=conn,
                callback_progresso=callback_progresso,
                arquivo_index=arquivo_index,
                total_arquivos=total_arquivos,
                total_importado_atual=total_importado
            )

        total_classificado = classificar_lancamentos_importados(
            cod_empresa_fixo=cod_empresa_fixo,
            conn=conn
        )

        conn.commit()
        return {
            "total_importado": total_importado,
            "total_classificado": total_classificado
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def Importa_Web_Postos_Arquivos(
    arquivos,
    data_lancamento,
    coluna_valor=None,
    cod_empresa_fixo=None,
    callback_progresso=None
):
    if not cod_empresa_fixo:
        raise ValueError("Cod_empresa da sessão não foi informado.")

    coluna_valor = (coluna_valor or COLUNA_VALOR_PADRAO).strip().upper()

    arquivos_validos = []
    for arq in arquivos:
        nome = (arq.filename or "").strip()
        if nome and nome.lower().endswith((".xlsx", ".xls")):
            arquivos_validos.append(arq)

    if not arquivos_validos:
        raise ValueError("Nenhum arquivo Excel válido foi enviado.")

    conn = get_connection()
    total_importado = 0
    total_classificado = 0

    try:
        total_arquivos = len(arquivos_validos)

        for arquivo_index, arquivo in enumerate(arquivos_validos, start=1):
            nome_temp = arquivo.filename
            caminho_temp = os.path.join("/tmp", f"importacao_{arquivo_index}_{nome_temp}")
            arquivo.save(caminho_temp)

            try:
                df = ler_arquivo_excel(caminho_temp)

                total_importado = processar_dataframe(
                    df=df,
                    caminho_arquivo=nome_temp,
                    data_lancamento=data_lancamento,
                    coluna_valor=coluna_valor,
                    cod_empresa_fixo=cod_empresa_fixo,
                    conn=conn,
                    callback_progresso=callback_progresso,
                    arquivo_index=arquivo_index,
                    total_arquivos=total_arquivos,
                    total_importado_atual=total_importado
                )
            finally:
                if os.path.exists(caminho_temp):
                    os.remove(caminho_temp)

        total_classificado = classificar_lancamentos_importados(
            cod_empresa_fixo=cod_empresa_fixo,
            conn=conn
        )

        conn.commit()
        return {
            "total_importado": total_importado,
            "total_classificado": total_classificado
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()