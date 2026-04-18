from flask import Blueprint, render_template, redirect, url_for, session

sistema_bp = Blueprint("sistema", __name__)

@sistema_bp.route("/selecionar_sistema")
def selecionar_sistema():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    return render_template(
        "selecionar_sistema.html",
        nome_empresa=session.get("nome_empresa")
    )

@sistema_bp.route("/entrar_financeiro")
def entrar_financeiro():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    session["sistema_ativo"] = "financeiro"
    return redirect(url_for("financeiro.menu_empresa"))

@sistema_bp.route("/entrar_vendas")
def entrar_vendas():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    session["sistema_ativo"] = "vendas"
    return redirect(url_for("sistema.menu_vendas"))

@sistema_bp.route("/menu_vendas")
def menu_vendas():
    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    return render_template(
        "menu_vendas.html",
        nome_empresa=session.get("nome_empresa"),
        empresa_ativa=session.get("cod_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema")
    )