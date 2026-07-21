import os
import requests
import uuid

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
BUCKET = "certificados"


def _headers():
    return {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
    }


def garantir_bucket():
    """Cria o bucket se ainda não existir."""
    url = f"{SUPABASE_URL}/storage/v1/bucket"
    r = requests.post(
        url,
        headers={**_headers(), "Content-Type": "application/json"},
        json={"id": BUCKET, "name": BUCKET, "public": True},
    )
    # 200 = criado, 409 = já existe — ambos são aceitáveis
    return r.status_code in (200, 201, 409)


def upload_arquivo(file_obj, nome_original: str, cod_empresa: str) -> str:
    """
    Faz upload de um arquivo para o Supabase Storage.
    Retorna o caminho relativo dentro do bucket (usado para montar a URL pública).
    """
    garantir_bucket()

    extensao = os.path.splitext(nome_original)[1].lower()
    nome_unico = f"{cod_empresa}/{uuid.uuid4().hex}{extensao}"

    content_type = "image/png" if extensao == ".png" else "image/jpeg"

    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{nome_unico}"
    r = requests.post(
        url,
        headers={**_headers(), "Content-Type": content_type},
        data=file_obj.read(),
    )

    if r.status_code not in (200, 201):
        raise RuntimeError(f"Erro ao fazer upload para o Storage: {r.status_code} {r.text}")

    return nome_unico


def url_publica(caminho: str) -> str:
    """Retorna a URL pública de um arquivo no bucket."""
    return f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{caminho}"


def deletar_arquivo(caminho: str):
    """Remove um arquivo do bucket (melhor esforço)."""
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{caminho}"
    requests.delete(url, headers=_headers())
