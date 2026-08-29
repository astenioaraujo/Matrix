"""Classes — aulas online (prova de conceito).

O apresentador entra autenticado no Matrix; o aluno entra por link, só com
o nome. Quem pode falar é decidido aqui, ao assinar o token.
"""

from flask import Blueprint, jsonify, redirect, render_template, request, session, url_for

from auth_helpers import login_obrigatorio
from services.classes_service import gerar_token, url_livekit

classes_bp = Blueprint("classes", __name__, url_prefix="/classes")

SALA_PADRAO = "aula"


@classes_bp.route("/")
@login_obrigatorio
def painel():
    """Painel do apresentador: entra no palco e copia o link do convite."""
    sala = request.args.get("sala") or SALA_PADRAO
    link = url_for("classes.entrar", sala=sala, _external=True)
    return render_template("classes/painel.html", sala=sala, link_convite=link)


@classes_bp.route("/palco")
@login_obrigatorio
def palco():
    """Tela do apresentador."""
    sala = request.args.get("sala") or SALA_PADRAO
    return render_template(
        "classes/sala.html",
        sala=sala,
        papel="apresentador",
        nome=session.get("nome_usuario") or "Apresentador",
        url_livekit=url_livekit(),
    )


@classes_bp.route("/<sala>")
def entrar(sala):
    """Tela pública: o aluno digita o nome e entra."""
    nome = (request.args.get("nome") or "").strip()
    if not nome:
        return render_template("classes/entrar.html", sala=sala)
    return render_template(
        "classes/sala.html",
        sala=sala,
        papel="aluno",
        nome=nome[:40],
        url_livekit=url_livekit(),
    )


@classes_bp.route("/api/token", methods=["POST"])
def api_token():
    """Assina o token de entrada. Publicar é privilégio do apresentador."""
    dados = request.get_json(silent=True) or {}
    sala = (dados.get("sala") or "").strip()
    nome = (dados.get("nome") or "").strip()[:40]
    papel = dados.get("papel")

    if not sala or not nome:
        return jsonify({"erro": "Sala e nome são obrigatórios."}), 400

    # Apresentador só quem está autenticado no Matrix — o papel vindo da tela
    # não basta, senão qualquer aluno pediria um token de publicador.
    id_usuario = session.get("id_usuario")
    apresentador = papel == "apresentador" and bool(id_usuario)

    identidade = ("prof-%s" % id_usuario) if apresentador else ("aluno-%s" % nome.lower())

    return jsonify({
        "token": gerar_token(sala, identidade, nome, apresentador),
        "url": url_livekit(),
        "apresentador": apresentador,
    })
