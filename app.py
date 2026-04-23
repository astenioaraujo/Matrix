import os
import sys

# 🔥 GARANTE QUE O PYTHON ENXERGUE A RAIZ DO PROJETO
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask

from routes.auth_routes import auth_bp
from routes.sistema_routes import sistema_bp
from routes.financeiro_routes import financeiro_bp
from routes.operacoes_routes import operacoes_bp
from routes.importacoes_routes import importacoes_bp
from routes.relatorios_routes import relatorios_bp
from routes.vendas_routes import vendas_bp
from routes.configuracoes_routes import configuracoes_bp
from routes.usuarios_routes import usuarios_bp

def formatar_numero_br(valor):
    try:
        return f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "0,00"

app = Flask(__name__)
app.secret_key = "matrix2026"

app.jinja_env.filters['br'] = formatar_numero_br

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

if __name__ == "__main__":
    app.run(debug=True, port=5001)