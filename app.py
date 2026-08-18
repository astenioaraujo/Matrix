import os
import sys

# 🔥 GARANTE QUE O PYTHON ENXERGUE A RAIZ DO PROJETO
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix

from routes.auth_routes import auth_bp
from routes.sistema_routes import sistema_bp
from routes.financeiro_routes import financeiro_bp
from routes.operacoes_routes import operacoes_bp
from routes.importacoes_routes import importacoes_bp
from routes.relatorios_routes import relatorios_bp
from routes.vendas_routes import vendas_bp
from routes.configuracoes_routes import configuracoes_bp
from routes.usuarios_routes import usuarios_bp
from routes.compliance_routes import compliance_bp
from routes.vistorias_routes import vistorias_bp
from routes.rh_routes import rh_bp
from routes.performances_routes import performances_bp
from routes.treinamentos_routes import treinamentos_bp
from routes.canivete_routes import canivete_bp
from routes.projetos_routes import projetos_bp
from routes.mercado_routes import mercado_bp
from routes.pdv_routes import pdv_bp


def formatar_numero_br(valor):
    try:
        return f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "0,00"

app = Flask(__name__)
app.secret_key = "matrix2026"
# HTTPS / PROXY / COOKIES
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

if not app.debug:
    app.config["SESSION_COOKIE_SECURE"] = True

app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

app.jinja_env.filters['br'] = formatar_numero_br


def sigla_empresa(nome):
    """Duas letras para o ícone de troca de empresa, a partir do nome fantasia.

    Primeiro caractere + o seguinte: outro dígito se o primeiro for dígito
    (30 Set → 30), senão a próxima consoante (Lucena → LC, Vilela → VL,
    Inovai → IN, O Closet → OC).
    """
    import unicodedata

    texto = "".join(
        c for c in unicodedata.normalize("NFD", str(nome or ""))
        if unicodedata.category(c) != "Mn"
    )
    texto = "".join(c for c in texto if c.isalnum())

    if not texto:
        return "?"

    primeiro = texto[0]
    resto = texto[1:]

    if primeiro.isdigit():
        seguinte = next((c for c in resto if c.isdigit()), "")
    else:
        seguinte = next(
            (c for c in resto if c.isalpha() and c.upper() not in "AEIOU"), ""
        )

    return (primeiro + seguinte).upper()


app.jinja_env.filters['sigla_empresa'] = sigla_empresa

# REGISTRO DOS BLUEPRINTS
app.register_blueprint(auth_bp)
app.register_blueprint(sistema_bp)
app.register_blueprint(financeiro_bp)
app.register_blueprint(operacoes_bp, url_prefix="/operacoes")
app.register_blueprint(importacoes_bp)
app.register_blueprint(relatorios_bp)
app.register_blueprint(vendas_bp)
app.register_blueprint(configuracoes_bp)
app.register_blueprint(usuarios_bp)
app.register_blueprint(compliance_bp, url_prefix="/compliance")
app.register_blueprint(vistorias_bp, url_prefix="/vistorias")
app.register_blueprint(rh_bp, url_prefix="/rh")
app.register_blueprint(performances_bp, url_prefix="/performances")
app.register_blueprint(treinamentos_bp)
app.register_blueprint(canivete_bp)
app.register_blueprint(projetos_bp)
app.register_blueprint(mercado_bp)
app.register_blueprint(pdv_bp)


from db import fechar_conexao_da_requisicao

# Fecha, ao fim de cada requisição, a conexão compartilhada que get_connection()
# entrega a todos os chamadores. Sem isto ela ficaria aberta.
app.teardown_appcontext(fechar_conexao_da_requisicao)


@app.context_processor
def _injetar_atalhos_topo():
    """
    Disponibiliza `pode_atalho_financas` e `pode_atalho_agenda` para toda
    página que estende base.html, para os ícones de atalho da barra superior.
    """
    from flask import session

    id_usuario = session.get("id_usuario")
    cod_empresa = session.get("cod_empresa")
    if not id_usuario or not cod_empresa:
        return {
            "pode_atalho_agenda": False,
            "pode_atalho_financas": False,
        }

    if str(session.get("tipo_global") or "").strip().lower() == "superusuario":
        return {
            "pode_atalho_agenda": True,
            "pode_atalho_financas": True,
        }

    cod_empresa = str(cod_empresa).strip()

    # Isto roda em TODA página que estende o base.html. Consultar o banco a
    # cada render custava ~100 ms para decidir se aparece um ícone na barra.
    # Fica guardado na sessão, amarrado ao usuário e à empresa: trocar de
    # empresa refaz a checagem. Todas as permissões saem de UMA consulta só,
    # então cada ícone novo não acrescenta nada ao tempo de carga. O acesso em
    # si continua sendo verificado nas rotas — aqui é só a exibição do atalho.
    cache = session.get("_atalhos_topo")
    if isinstance(cache, list) and len(cache) == 4:
        u_cache, e_cache, agenda_cache, financas_cache = cache
        if u_cache == id_usuario and e_cache == cod_empresa:
            return {
                "pode_atalho_agenda": agenda_cache,
                "pode_atalho_financas": financas_cache,
            }

    pode_agenda = False
    pode_financas = False
    try:
        from db import get_connection
        cur = get_connection().cursor()
        try:
            cur.execute("""
                SELECT sistema, opcao
                FROM usuarios_permissoes
                WHERE id_usuario = %s
                  AND cod_empresa = %s
                  AND ativo = TRUE
                  AND sistema = 'CANIVETE'
                  AND opcao IN ('MENU', 'AGENDA', 'FINANCAS_PESSOAIS_MENU')
            """, (id_usuario, cod_empresa))
            liberadas = {(r[0], r[1]) for r in cur.fetchall()}
        finally:
            cur.close()
        pode_agenda = ("CANIVETE", "AGENDA") in liberadas
        # o atalho entra no Canivete Suíço, então exige acesso ao módulo
        # além do acesso à própria tela de Finanças Pessoais
        pode_financas = (
            ("CANIVETE", "MENU") in liberadas
            and ("CANIVETE", "FINANCAS_PESSOAIS_MENU") in liberadas
        )
    except Exception:
        pass

    session.pop("_atalho_agenda", None)   # cache antigo, só da agenda
    session["_atalhos_topo"] = [
        id_usuario, cod_empresa, pode_agenda, pode_financas,
    ]
    return {
        "pode_atalho_agenda": pode_agenda,
        "pode_atalho_financas": pode_financas,
    }


if __name__ == "__main__":
    app.run(debug=True, port=5001)