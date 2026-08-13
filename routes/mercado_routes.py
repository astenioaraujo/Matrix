import csv
import io
import re
from datetime import datetime

from flask import (Blueprint, render_template, redirect, url_for, session,
                   flash, request)
from psycopg2.extras import RealDictCursor, execute_values

from db import get_connection
from security_helpers import usuario_tem_permissao

mercado_bp = Blueprint("mercado", __name__, url_prefix="/mercado")


def _checar_acesso(opcao="MENU"):
    if "id_usuario" not in session or "cod_empresa" not in session:
        return redirect(url_for("auth.index"))
    tipo_global = str(session.get("tipo_global") or "").strip().lower()
    if tipo_global != "superusuario":
        id_usuario  = session["id_usuario"]
        cod_empresa = str(session["cod_empresa"]).strip()
        if not usuario_tem_permissao(id_usuario, cod_empresa, "MERCADO", opcao):
            flash("Você não tem permissão para acessar Mercado.", "error")
            return redirect(url_for("sistema.selecionar_sistema"))
    return None


# ─── MENU ────────────────────────────────────────────────────────────────────

@mercado_bp.route("/")
def menu_mercado():
    redir = _checar_acesso()
    if redir:
        return redir
    return render_template(
        "menu_mercado.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema"),
    )


# ─── ANP ─────────────────────────────────────────────────────────────────────

@mercado_bp.route("/anp")
def menu_anp():
    redir = _checar_acesso()
    if redir:
        return redir
    return render_template(
        "menu_anp.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("mercado.menu_mercado"),
    )


# ─── IMPORTAÇÃO DO CSV ───────────────────────────────────────────────────────
# O arquivo da ANP é a fotografia completa do país (~45 mil postos), não um
# incremental. Cada importação substitui todos os postos da empresa.

COLUNAS_ANP = [
    "CODIGOISIMP", "AUTORIZACAO", "DATAPUBLICACAO", "RAZAOSOCIAL", "CNPJ",
    "ENDERECO", "COMPLEMENTO", "BAIRRO", "CEP", "UF", "MUNICIPIO", "BANDEIRA",
    "DATAVINCULACAO",
]

LOTE_INSERCAO = 2000


def _texto(valor, limite=None):
    v = (valor or "").strip()
    if not v:
        return None
    return v[:limite] if limite else v


def _data_br(valor):
    """dd/mm/aaaa → date. Vazio ou inválido vira NULL: o arquivo de 12/08/2026
    trouxe uma linha com as duas datas em branco e ela não pode derrubar a
    importação inteira."""
    v = (valor or "").strip()
    if not v:
        return None
    try:
        return datetime.strptime(v, "%d/%m/%Y").date()
    except ValueError:
        return None


def _data_do_nome(nome_arquivo):
    """postos_combustiveis_ANP_2026-08-12.csv → date(2026, 8, 12)."""
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", nome_arquivo or "")
    if not m:
        return None
    try:
        return datetime.strptime(m.group(0), "%Y-%m-%d").date()
    except ValueError:
        return None


def _ler_csv_anp(conteudo_bytes):
    """Devolve (linhas, ignoradas). Levanta ValueError se o layout não bater."""
    try:
        texto = conteudo_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        # A ANP já publicou o mesmo arquivo em latin-1.
        texto = conteudo_bytes.decode("latin-1")

    leitor = csv.DictReader(io.StringIO(texto), delimiter=";")
    cabecalho = [(c or "").strip().upper() for c in (leitor.fieldnames or [])]

    faltando = [c for c in COLUNAS_ANP if c not in cabecalho]
    if faltando:
        raise ValueError(
            "O arquivo não tem o layout da ANP. Colunas ausentes: "
            + ", ".join(faltando)
        )

    linhas = []
    vistos = set()
    ignoradas = 0

    for reg in leitor:
        reg = {(k or "").strip().upper(): v for k, v in reg.items()}
        cnpj = re.sub(r"\D", "", (reg.get("CNPJ") or ""))

        # Sem CNPJ não há chave; repetido seria conflito no índice único.
        if not cnpj or cnpj in vistos:
            ignoradas += 1
            continue
        vistos.add(cnpj)

        linhas.append((
            _texto(reg.get("CODIGOISIMP"), 20),
            _texto(reg.get("AUTORIZACAO"), 40),
            _data_br(reg.get("DATAPUBLICACAO")),
            _texto(reg.get("RAZAOSOCIAL"), 150),
            cnpj,
            _texto(reg.get("ENDERECO"), 150),
            _texto(reg.get("COMPLEMENTO"), 150),
            _texto(reg.get("BAIRRO"), 80),
            re.sub(r"\D", "", (reg.get("CEP") or "")) or None,
            (_texto(reg.get("UF"), 2) or "").upper() or None,
            _texto(reg.get("MUNICIPIO"), 80),
            _texto(reg.get("BANDEIRA"), 60),
            _data_br(reg.get("DATAVINCULACAO")),
        ))

    if not linhas:
        raise ValueError("O arquivo não tem nenhuma linha válida.")

    return linhas, ignoradas


@mercado_bp.route("/anp/importar", methods=["GET", "POST"])
def importar_csv_anp():
    redir = _checar_acesso("IMPORTAR_CSV_ANP")
    if redir:
        return redir

    def pagina(**kw):
        conn = get_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT nome_arquivo, data_arquivo, qtd_postos, criado_em
                  FROM mercado_anp_importacoes
                 ORDER BY criado_em DESC
                 LIMIT 10
            """)
            historico = cur.fetchall()
        finally:
            conn.close()

        base = {
            "nome_empresa": session.get("nome_empresa"),
            "historico": historico,
            "url_voltar": url_for("mercado.menu_anp"),
        }
        base.update(kw)
        return render_template("mercado_anp_importar.html", **base)

    if request.method == "GET":
        return pagina()

    arquivo = request.files.get("arquivo")
    if not arquivo or arquivo.filename == "":
        flash("Selecione o arquivo CSV da ANP.", "error")
        return pagina()

    try:
        linhas, ignoradas = _ler_csv_anp(arquivo.read())
    except ValueError as e:
        flash(str(e), "error")
        return pagina()
    except Exception as e:
        flash(f"Erro ao ler o arquivo: {e}", "error")
        return pagina()

    data_txt = (request.form.get("data_arquivo") or "").strip()
    try:
        data_arquivo = (datetime.strptime(data_txt, "%Y-%m-%d").date()
                        if data_txt else _data_do_nome(arquivo.filename))
    except ValueError:
        flash("Data do arquivo inválida.", "error")
        return pagina()

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO mercado_anp_importacoes
                (nome_arquivo, data_arquivo, qtd_postos, id_usuario,
                 cod_empresa_origem)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id_anp_importacao
        """, (arquivo.filename[:150], data_arquivo, len(linhas),
              session.get("id_usuario"), session.get("cod_empresa")))
        id_importacao = cur.fetchone()[0]

        # Fotografia completa do país: o que estava lá sai antes de entrar o
        # novo. A base é única para todas as empresas — não há recorte por
        # tenant nesta limpeza.
        cur.execute("DELETE FROM mercado_anp_postos")
        substituidos = cur.rowcount or 0

        sql_insert = """
            INSERT INTO mercado_anp_postos (
                id_anp_importacao, codigo_isimp, autorizacao,
                data_publicacao, razao_social, cnpj, endereco, complemento,
                bairro, cep, uf, municipio, bandeira, data_vinculacao
            ) VALUES %s
        """
        dados = [(id_importacao,) + l for l in linhas]

        for i in range(0, len(dados), LOTE_INSERCAO):
            execute_values(cur, sql_insert, dados[i:i + LOTE_INSERCAO],
                           page_size=LOTE_INSERCAO)

        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f"Erro ao gravar a importação: {e}", "error")
        return pagina()
    finally:
        conn.close()

    aviso = f"Importação concluída: {len(linhas)} postos gravados"
    if substituidos:
        aviso += f" (substituíram {substituidos} da importação anterior)"
    if ignoradas:
        aviso += f"; {ignoradas} linha(s) ignorada(s) por CNPJ vazio ou repetido"
    flash(aviso + ".", "success")
    return redirect(url_for("mercado.consultar_anp"))


# ─── CONSULTA ────────────────────────────────────────────────────────────────
# Três totalizações sobre a mesma base, escolhidas por ?agrupar=.

AGRUPAMENTOS = {
    "uf": {
        "titulo": "Postos por Estado",
        "rotulo": "Estado",
        "colunas": ["uf"],
    },
    "municipio": {
        "titulo": "Postos por Cidade",
        "rotulo": "Cidade",
        "colunas": ["uf", "municipio"],
    },
    "bandeira": {
        "titulo": "Postos por Bandeira",
        "rotulo": "Bandeira",
        "colunas": ["bandeira"],
    },
}


@mercado_bp.route("/anp/consultar")
def consultar_anp():
    redir = _checar_acesso("CONSULTAR_ANP")
    if redir:
        return redir

    agrupar = (request.args.get("agrupar") or "uf").strip().lower()
    if agrupar not in AGRUPAMENTOS:
        agrupar = "uf"
    config = AGRUPAMENTOS[agrupar]

    filtro_uf       = (request.args.get("uf") or "").strip().upper()
    filtro_bandeira = (request.args.get("bandeira") or "").strip()

    # A base é nacional e única; os filtros são só recorte de leitura.
    condicoes = ["TRUE"]
    params    = []
    if filtro_uf:
        condicoes.append("uf = %s")
        params.append(filtro_uf)
    if filtro_bandeira:
        condicoes.append("bandeira = %s")
        params.append(filtro_bandeira)
    where = " AND ".join(condicoes)

    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        colunas = ", ".join(config["colunas"])
        # Percentual sai da consulta, nunca de coluna persistida.
        cur.execute(f"""
            SELECT {colunas},
                   COUNT(*) AS qtd,
                   ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS percentual
              FROM mercado_anp_postos
             WHERE {where}
             GROUP BY {colunas}
             ORDER BY COUNT(*) DESC, {colunas}
        """, params)
        totais = cur.fetchall()

        cur.execute(f"SELECT COUNT(*) AS qtd FROM mercado_anp_postos WHERE {where}",
                    params)
        total_geral = cur.fetchone()["qtd"]

        cur.execute("""
            SELECT DISTINCT uf FROM mercado_anp_postos
             WHERE uf IS NOT NULL ORDER BY uf
        """)
        ufs = [r["uf"] for r in cur.fetchall()]

        cur.execute("""
            SELECT DISTINCT bandeira FROM mercado_anp_postos
             WHERE bandeira IS NOT NULL ORDER BY bandeira
        """)
        bandeiras = [r["bandeira"] for r in cur.fetchall()]

        cur.execute("""
            SELECT nome_arquivo, data_arquivo, qtd_postos, criado_em
              FROM mercado_anp_importacoes
             ORDER BY criado_em DESC
             LIMIT 1
        """)
        ultima = cur.fetchone()
    finally:
        conn.close()

    return render_template(
        "mercado_anp_consultar.html",
        nome_empresa=session.get("nome_empresa"),
        agrupar=agrupar,
        config=config,
        totais=totais,
        total_geral=total_geral,
        ufs=ufs,
        bandeiras=bandeiras,
        filtro_uf=filtro_uf,
        filtro_bandeira=filtro_bandeira,
        ultima=ultima,
        url_voltar=url_for("mercado.menu_anp"),
    )
