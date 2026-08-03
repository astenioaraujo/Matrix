import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extensions import TRANSACTION_STATUS_INERROR

# Carrega .env apenas no ambiente local
if os.getenv("RENDER") is None:
    load_dotenv()


def _nova_conexao():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        sslmode="require",
    )


class _ConexaoDaRequisicao:
    """Conexão compartilhada por toda a requisição.

    Abrir conexão com o banco remoto custa ~600 ms, muito mais caro que as
    consultas em si. Antes, uma única página abria três: o decorador de
    permissão, o context processor do atalho da agenda e a própria rota.

    Repassa tudo para a conexão real, mas ignora close(): as ~210 chamadas
    espalhadas pelo sistema fazem `finally: conn.close()`, e a primeira
    delas fecharia a conexão que o resto da requisição ainda vai usar. Quem
    fecha de verdade é o teardown, em app.py.
    """

    def __init__(self, real):
        self._real = real

    def __getattr__(self, nome):
        return getattr(self._real, nome)

    def close(self):
        pass

    def fechar_de_verdade(self):
        self._real.close()


def get_connection():
    """Conexão do banco. Dentro de uma requisição Flask, a mesma para todos
    os chamadores; fora dela (scripts, importações, cron), uma nova."""
    try:
        from flask import g, has_app_context
    except ImportError:
        return _nova_conexao()

    if not has_app_context():
        return _nova_conexao()

    conexao = getattr(g, "_conexao_matrx", None)

    if conexao is not None and not conexao._real.closed:
        # Se uma consulta anterior falhou e ninguém deu rollback, a conexão
        # fica em transação abortada e recusa tudo que vier depois. Antes
        # isso não aparecia porque cada chamador tinha a sua conexão.
        if conexao._real.get_transaction_status() == TRANSACTION_STATUS_INERROR:
            conexao._real.rollback()
        return conexao

    conexao = _ConexaoDaRequisicao(_nova_conexao())
    g._conexao_matrx = conexao
    return conexao


def fechar_conexao_da_requisicao(_exc=None):
    """Fecha a conexão da requisição. Ligado ao teardown em app.py."""
    try:
        from flask import g
    except ImportError:
        return

    conexao = getattr(g, "_conexao_matrx", None)
    if conexao is None:
        return

    g._conexao_matrx = None
    try:
        if not conexao._real.closed:
            # descarta transação pendente — mesmo efeito que o close() de
            # antes, que também perdia o que não tinha sido commitado
            conexao._real.rollback()
    except Exception:
        pass
    try:
        conexao.fechar_de_verdade()
    except Exception:
        pass
