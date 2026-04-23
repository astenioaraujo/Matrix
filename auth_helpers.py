from functools import wraps
from flask import session, redirect, url_for, flash

def login_obrigatorio(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "id_usuario" not in session:
            return redirect(url_for("auth.index"))
        if "cod_empresa" not in session:
            return redirect(url_for("auth.escolher_empresa"))
        return f(*args, **kwargs)
    return wrapper


def admin_empresa_obrigatorio(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "id_usuario" not in session:
            return redirect(url_for("auth.index"))
        if "cod_empresa" not in session:
            return redirect(url_for("auth.escolher_empresa"))

        if session.get("tipo_global") == "superusuario":
            return f(*args, **kwargs)

        if session.get("perfil_empresa") != "admin":
            flash("Acesso restrito ao administrador da empresa.", "error")
            return redirect(url_for("sistema.selecionar_sistema"))

        return f(*args, **kwargs)
    return wrapper

def superusuario_obrigatorio(f):

    @wraps(f)

    def wrapper(*args, **kwargs):

        if "id_usuario" not in session:

            return redirect(url_for("auth.index"))

        if session.get("tipo_global") != "superusuario":

            flash("Acesso permitido somente para superusuários.", "error")

            return redirect(url_for("sistema.selecionar_sistema"))

        return f(*args, **kwargs)

    return wrapper