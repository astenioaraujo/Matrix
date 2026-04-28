from flask import Blueprint, render_template, session, redirect, url_for
from security_helpers import permissao_obrigatoria

compliance_bp = Blueprint("compliance", __name__)

# ---------------------------------------
# MENU COMPLIANCE
# ---------------------------------------
@compliance_bp.route("/menu")
@permissao_obrigatoria(
    "COMPLIANCE",
    "MENU",
    redirecionar_para="sistema.selecionar_sistema",
)
def menu_compliance():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    return render_template(
        "menu_compliance.html",
        nome_empresa=session.get("nome_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema"),
        texto_voltar="← Voltar",
    )