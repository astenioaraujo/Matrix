from db import get_connection
from psycopg2.extras import RealDictCursor

def usuario_tem_permissao(id_usuario, cod_empresa, sistema, opcao):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT 1
            FROM usuarios_permissoes
            WHERE id_usuario = %s
              AND cod_empresa = %s
              AND sistema = %s
              AND opcao = %s
              AND ativo = TRUE
            LIMIT 1
        """, (id_usuario, cod_empresa, sistema, opcao))
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def usuario_filiais_ativas(id_usuario, cod_empresa):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT cod_filial
            FROM usuarios_filiais
            WHERE id_usuario = %s
              AND cod_empresa = %s
              AND ativo = TRUE
            ORDER BY cod_filial
        """, (id_usuario, cod_empresa))
        return [str(r["cod_filial"]).strip() for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def usuario_eh_superusuario():
    from flask import session
    return session.get("tipo_global") == "superusuario"


from functools import wraps
from flask import session, redirect, url_for, flash

def permissao_obrigatoria(sistema, opcao, redirecionar_para="sistema.selecionar_sistema"):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if "id_usuario" not in session:
                return redirect(url_for("auth.index"))

            if "cod_empresa" not in session:
                return redirect(url_for("auth.escolher_empresa"))

            if session.get("tipo_global") == "superusuario":
                return f(*args, **kwargs)

            id_usuario = session["id_usuario"]
            cod_empresa = str(session["cod_empresa"]).strip()

            if not usuario_tem_permissao(id_usuario, cod_empresa, sistema, opcao):
                flash("Você não tem permissão para acessar esta opção.", "error")
                return redirect(url_for(redirecionar_para))

            return f(*args, **kwargs)
        return wrapper
    return decorator