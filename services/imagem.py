import contextlib
import os
import tempfile

QUALIDADE_JPEG = 85
EXTENSOES_HEIC = {".heic", ".heif"}

_heif_registrado = False
_heif_falhou = False


def _registrar_heif():
    """Habilita o Pillow a abrir HEIC/HEIF (iPhone grava foto nesse formato
    por padrão). Sob demanda e protegida por try/except pelo mesmo motivo
    do ffmpeg em services/video.py: uma falha aqui não pode derrubar o
    import deste módulo (e com ele o app inteiro)."""
    global _heif_registrado, _heif_falhou

    if _heif_falhou or _heif_registrado:
        return not _heif_falhou

    try:
        import pillow_heif
        pillow_heif.register_heif_opener()
        _heif_registrado = True
    except Exception:
        _heif_falhou = True

    return _heif_registrado


@contextlib.contextmanager
def preparar_imagem_para_upload(file_obj, nome_original):
    """
    HEIC/HEIF (padrão de câmera do iPhone) não renderiza em <img> na
    maioria dos navegadores — só aparece o ícone de imagem quebrada,
    mesmo com o arquivo íntegro no Storage. Converte para JPEG antes do
    upload; os demais formatos (jpg, png, webp) já renderizam em qualquer
    navegador e sobem sem alteração.

    Se a conversão falhar por qualquer motivo (HEIC corrompido, Pillow sem
    suporte a HEIF no ambiente), sobe o arquivo original sem alteração —
    melhor um HEIC que abre só no Safari do que perder a foto.

    Context manager: entrega (caminho_arquivo, nome_arquivo,
    content_type_ou_None, tamanho_bytes) e apaga os arquivos temporários
    ao sair, com sucesso ou não.
    """
    extensao_original = os.path.splitext(nome_original)[1].lower()
    fd_entrada, caminho_entrada = tempfile.mkstemp(suffix=extensao_original or ".bin")
    os.close(fd_entrada)
    fd_saida, caminho_saida = tempfile.mkstemp(suffix=".jpg")
    os.close(fd_saida)

    try:
        file_obj.save(caminho_entrada)

        converteu = False

        if extensao_original in EXTENSOES_HEIC and _registrar_heif():
            try:
                from PIL import Image
                with Image.open(caminho_entrada) as imagem:
                    imagem.convert("RGB").save(caminho_saida, "JPEG", quality=QUALIDADE_JPEG)
                converteu = os.path.getsize(caminho_saida) > 0
            except Exception:
                converteu = False

        if converteu:
            caminho_final = caminho_saida
            nome_final = os.path.splitext(nome_original)[0] + ".jpg"
            content_type = "image/jpeg"
        else:
            caminho_final = caminho_entrada
            nome_final = nome_original
            content_type = None

        tamanho_bytes = os.path.getsize(caminho_final)
        yield caminho_final, nome_final, content_type, tamanho_bytes
    finally:
        for caminho in (caminho_entrada, caminho_saida):
            try:
                os.remove(caminho)
            except OSError:
                pass
