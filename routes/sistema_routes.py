from flask import Blueprint, render_template, redirect, url_for, session, flash
from security_helpers import usuario_tem_permissao

sistema_bp = Blueprint("sistema", __name__)


@sistema_bp.route("/selecionar_sistema")
def selecionar_sistema():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global == "superusuario":
        pode_operacoes = True
        pode_financeiro = True
        pode_vendas = True
        pode_configuracoes = True
        pode_compliance = True
        pode_vistorias = True
        pode_usuarios = True
    else:
        pode_operacoes = usuario_tem_permissao(id_usuario, cod_empresa, "OPERACOES", "MENU")
        pode_financeiro = usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "MENU")
        pode_vendas = usuario_tem_permissao(id_usuario, cod_empresa, "VENDAS", "MENU")
        pode_configuracoes = usuario_tem_permissao(id_usuario, cod_empresa, "CONFIGURACOES", "MENU")
        pode_compliance = usuario_tem_permissao(id_usuario, cod_empresa, "COMPLIANCE", "MENU")
        pode_vistorias = usuario_tem_permissao(id_usuario, cod_empresa, "VISTORIAS", "MENU")
        pode_usuarios = usuario_tem_permissao(id_usuario, cod_empresa, "USUARIOS", "MENU")

    return render_template(
        "selecionar_sistema.html",
        nome_empresa=session.get("nome_empresa"),
        pode_operacoes=pode_operacoes,
        pode_financeiro=pode_financeiro,
        pode_vendas=pode_vendas,
        pode_compliance=pode_compliance,
        pode_vistorias=pode_vistorias,
        pode_configuracoes=pode_configuracoes,
        pode_usuarios=pode_usuarios
    )


@sistema_bp.route("/entrar_financeiro")
def entrar_financeiro():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global != "superusuario":
        if not usuario_tem_permissao(id_usuario, cod_empresa, "FINANCEIRO", "MENU"):
            flash("Você não tem permissão para acessar o Financeiro.", "error")
            return redirect(url_for("sistema.selecionar_sistema"))

    session["sistema_ativo"] = "financeiro"
    return redirect(url_for("financeiro.menu_empresa"))


@sistema_bp.route("/entrar_vendas")
def entrar_vendas():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global != "superusuario":
        if not usuario_tem_permissao(id_usuario, cod_empresa, "VENDAS", "MENU"):
            flash("Você não tem permissão para acessar Vendas.", "error")
            return redirect(url_for("sistema.selecionar_sistema"))

    session["sistema_ativo"] = "vendas"
    return redirect(url_for("sistema.menu_vendas"))


@sistema_bp.route("/menu_vendas")
def menu_vendas():
    if "id_usuario" not in session:
        return redirect(url_for("auth.index"))

    if "cod_empresa" not in session:
        return redirect(url_for("auth.index"))

    id_usuario = session["id_usuario"]
    cod_empresa = str(session["cod_empresa"]).strip()
    tipo_global = str(session.get("tipo_global") or "").strip().lower()

    if tipo_global != "superusuario":
        if not usuario_tem_permissao(id_usuario, cod_empresa, "VENDAS", "MENU"):
            flash("Você não tem permissão para acessar o menu de Vendas.", "error")
            return redirect(url_for("sistema.selecionar_sistema"))

    return render_template(
        "menu_vendas.html",
        nome_empresa=session.get("nome_empresa"),
        empresa_ativa=session.get("cod_empresa"),
        url_voltar=url_for("sistema.selecionar_sistema")
    )