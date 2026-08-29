"""Tokens de acesso do LiveKit para o módulo Classes.

O token é um JWT HS256 assinado com a API Secret — assinado sempre no
servidor, nunca no navegador: quem assina o token decide quem pode falar.
Sem dependência nova: a assinatura é hmac + base64url da própria stdlib.
"""

import base64
import hashlib
import hmac
import json
import os
import time

DURACAO_TOKEN = 6 * 60 * 60  # uma aula folgada


def _config():
    url = os.environ.get("LIVEKIT_URL", "")
    chave = os.environ.get("LIVEKIT_API_KEY", "")
    segredo = os.environ.get("LIVEKIT_API_SECRET", "")
    if not (url and chave and segredo):
        raise RuntimeError(
            "LiveKit não configurado: faltam LIVEKIT_URL, LIVEKIT_API_KEY "
            "ou LIVEKIT_API_SECRET no .env"
        )
    return url, chave, segredo


def _b64(dados: bytes) -> str:
    return base64.urlsafe_b64encode(dados).decode().rstrip("=")


def gerar_token(sala, identidade, nome, pode_publicar):
    """Token de entrada numa sala.

    `pode_publicar=False` é o aluno: ele vê e ouve, e o servidor não aceita
    áudio nem vídeo dele — não é microfone mudo na tela, é permissão.
    """
    url, chave, segredo = _config()
    agora = int(time.time())

    cabecalho = {"alg": "HS256", "typ": "JWT"}
    corpo = {
        "iss": chave,
        "sub": identidade,
        "name": nome,
        "nbf": agora - 10,
        "exp": agora + DURACAO_TOKEN,
        "video": {
            "room": sala,
            "roomJoin": True,
            "canSubscribe": True,
            "canPublish": bool(pode_publicar),
            "canPublishData": True,
        },
    }

    entrada = "%s.%s" % (
        _b64(json.dumps(cabecalho, separators=(",", ":")).encode()),
        _b64(json.dumps(corpo, separators=(",", ":")).encode()),
    )
    assinatura = hmac.new(segredo.encode(), entrada.encode(), hashlib.sha256).digest()
    return "%s.%s" % (entrada, _b64(assinatura))


def url_livekit():
    return _config()[0]
