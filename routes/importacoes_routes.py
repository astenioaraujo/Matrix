import subprocess
import traceback
import uuid

from flask import Blueprint, render_template, request, redirect, url_for, session, jsonify
from psycopg2.extras import RealDictCursor

from db import get_connection
from importa_web_postos import (
    Importa_Web_Postos,
    Importa_Web_Postos_Arquivos,
    classificar_lancamentos_importados,
)

importacoes_bp = Blueprint("importacoes", __name__)

IMPORT_PROGRESS = {}


@importacoes_bp.route("/importacoes")
def listar_importacoes():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    somente_pendentes = (request.args.get("somente_pendentes") or "").strip() == "1"

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT COUNT(*)
            FROM importacoes
            WHERE cod_empresa = %s
        """, (session["cod_empresa"],))
        total_registros = cur.fetchone()["count"]

        cur.execute("""
            SELECT COUNT(*)
            FROM importacoes
            WHERE cod_empresa = %s
              AND (grupo IS NULL OR conta IS NULL)
        """, (session["cod_empresa"],))
        total_pendentes = cur.fetchone()["count"]

        where_extra = ""
        params = [session["cod_empresa"]]

        if somente_pendentes:
            where_extra = "AND (grupo IS NULL OR conta IS NULL)"

        cur.execute(f"""
            SELECT
                cod_filial,
                nome_filial,
                ano,
                mes,
                data,
                historico,
                valor,
                grupo,
                conta,
                descricao_conta,
                complemento
            FROM importacoes
            WHERE cod_empresa = %s
              {where_extra}
            ORDER BY data DESC, cod_filial, historico
        """, params)

        rows = cur.fetchall()
        colnames = [d[0] for d in cur.description]

    finally:
        cur.close()
        conn.close()

    mensagem = session.pop("mensagem_importacoes", "")
    erro = session.pop("erro_importacoes", "")

    return render_template(
        "importacoes.html",
        rows=rows,
        colnames=colnames,
        total_registros=total_registros,
        total_pendentes=total_pendentes,
        somente_pendentes=somente_pendentes,
        mensagem=mensagem,
        erro=erro,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
    )


@importacoes_bp.route("/importacoes/reclassificar", methods=["POST"])
def reclassificar_importacoes():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    try:
        total_classificados = classificar_lancamentos_importados(session["cod_empresa"])
        session["mensagem_importacoes"] = (
            f"Reclassificação concluída. {total_classificados} lançamento(s) classificado(s)."
        )
    except Exception as e:
        session["erro_importacoes"] = str(e)

    return redirect(url_for("importacoes.listar_importacoes"))


@importacoes_bp.route("/importacoes/limpar", methods=["POST"])
def limpar_importacoes():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            DELETE FROM importacoes
            WHERE cod_empresa = %s
        """, (session["cod_empresa"],))

        qtd = cur.rowcount
        conn.commit()
        session["mensagem_importacoes"] = f"{qtd} lançamento(s) importado(s) foram excluído(s)."

    except Exception as e:
        conn.rollback()
        session["erro_importacoes"] = str(e)

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("importacoes.listar_importacoes"))


def iniciar_progresso():
    job_id = str(uuid.uuid4())
    IMPORT_PROGRESS[job_id] = {
        "status": "iniciando",
        "percentual": 0,
        "mensagem": "Preparando...",
        "arquivo_atual": "",
        "filial_atual": "",
        "linha_atual": 0,
        "total_linhas_arquivo": 0,
        "arquivo_index": 0,
        "total_arquivos": 0,
        "importados": 0,
        "erro": "",
    }
    return job_id


@importacoes_bp.route("/importacoes/progresso/<job_id>")
def progresso_importacao(job_id):
    return jsonify(IMPORT_PROGRESS.get(job_id, {}))


@importacoes_bp.route("/selecionar_diretorio")
def selecionar_diretorio():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    try:
        script = '''
        set chosenFolder to choose folder with prompt "Selecione o diretório dos arquivos Excel"
        POSIX path of chosenFolder
        '''

        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            diretorio = result.stdout.strip()
            if diretorio:
                session["diretorio_importacao"] = diretorio

    except Exception:
        pass

    return redirect(url_for("importacoes.importar_web_postos"))


@importacoes_bp.route("/importacoes/transferir", methods=["POST"])
def transferir_importacoes():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    cod_empresa = str(session["cod_empresa"]).strip()

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT COUNT(*)
            FROM importacoes
            WHERE cod_empresa = %s
        """, (cod_empresa,))
        total_importacoes = cur.fetchone()[0]

        if total_importacoes == 0:
            session["erro_importacoes"] = "Não existem lançamentos importados para transferir."
            return redirect(url_for("importacoes.listar_importacoes"))

        cur.execute("""
            SELECT COUNT(*)
            FROM importacoes
            WHERE cod_empresa = %s
              AND (
                    grupo IS NULL OR TRIM(CAST(grupo AS TEXT)) = ''
                 OR conta IS NULL OR TRIM(CAST(conta AS TEXT)) = ''
              )
        """, (cod_empresa,))
        pendentes = cur.fetchone()[0]

        if pendentes > 0:
            session["erro_importacoes"] = (
                f"Transferência negada. Existem {pendentes} lançamento(s) sem grupo e/ou conta."
            )
            return redirect(url_for("importacoes.listar_importacoes"))

        cur.execute("""
            INSERT INTO lancamentos (
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
            SELECT
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
            FROM importacoes
            WHERE cod_empresa = %s
        """, (cod_empresa,))

        total_transferidos = cur.rowcount

        cur.execute("""
            DELETE FROM importacoes
            WHERE cod_empresa = %s
        """, (cod_empresa,))

        conn.commit()

        session["mensagem_importacoes"] = (
            f"Transferência concluída com sucesso. {total_transferidos} lançamento(s) foram enviados para a tabela de lançamentos."
        )

    except Exception as e:
        conn.rollback()
        session["erro_importacoes"] = str(e)

    finally:
        cur.close()
        conn.close()

    return redirect(url_for("importacoes.listar_importacoes"))


@importacoes_bp.route("/importacoes/importar", methods=["GET", "POST"])
def importar_web_postos():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    mensagem = ""
    erro = ""
    job_id = ""

    if request.method == "POST":
        try:
            diretorio = (request.form.get("diretorio") or "").strip()
            data_lancamento = (request.form.get("data_lancamento") or "").strip()

            coluna_valor = "F"

            if not diretorio:
                raise ValueError("Informe o diretório dos arquivos.")
            if not data_lancamento:
                raise ValueError("Informe a data do lançamento.")

            resultado = Importa_Web_Postos(
                diretorio=diretorio,
                data_lancamento=data_lancamento,
                coluna_valor=coluna_valor,
                cod_empresa_fixo=session["cod_empresa"],
                callback_progresso=None
            )

            if isinstance(resultado, dict):
                total_importado = int(resultado.get("total_importado", 0))
                total_classificado = int(resultado.get("total_classificado", 0))
            else:
                total_importado = int(resultado or 0)
                total_classificado = 0

            mensagem = (
                f"Importação concluída. {total_importado} registros importados. "
                f"{total_classificado} lançamento(s) classificado(s) automaticamente."
            )
            session["diretorio_importacao"] = diretorio

        except Exception as e:
            traceback.print_exc()
            erro = str(e)

    return render_template(
        "importar_web_postos.html",
        job_id=job_id,
        mensagem=mensagem,
        erro=erro,
        diretorio_importacao=session.get("diretorio_importacao", ""),
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
    )


@importacoes_bp.route("/importacoes/upload", methods=["GET", "POST"])
def importar_upload():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    mensagem = ""
    erro = ""

    if request.method == "POST":
        try:
            arquivos = request.files.getlist("arquivos")
            data_lancamento = (request.form.get("data_lancamento") or "").strip()

            coluna_valor = "F"

            if not arquivos:
                raise ValueError("Nenhum arquivo foi enviado.")
            if not data_lancamento:
                raise ValueError("Informe a data do lançamento.")

            resultado = Importa_Web_Postos_Arquivos(
                arquivos=arquivos,
                data_lancamento=data_lancamento,
                coluna_valor=coluna_valor,
                cod_empresa_fixo=session["cod_empresa"],
                callback_progresso=None,
            )

            if isinstance(resultado, dict):
                total_importado = int(resultado.get("total_importado", 0))
                total_classificado = int(resultado.get("total_classificado", 0))
            else:
                total_importado = int(resultado or 0)
                total_classificado = 0

            mensagem = (
                f"{total_importado} registros importados. "
                f"{total_classificado} lançamento(s) classificado(s) automaticamente."
            )

        except Exception as e:
            erro = str(e)

    return render_template(
        "importar_upload.html",
        mensagem=mensagem,
        erro=erro,
        empresa_ativa=session["cod_empresa"],
        nome_empresa_ativa=session["nome_empresa"],
    )