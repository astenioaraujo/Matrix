import os
import subprocess
import tempfile

LARGURA_MAXIMA = 1280
ALTURA_MAXIMA = 720
CRF = 28
TIMEOUT_SEGUNDOS = 300

_ffmpeg_binario_cache = None
_ffmpeg_binario_falhou = False


def _ffmpeg_binario():
    """
    Caminho do ffmpeg empacotado pelo imageio-ffmpeg (no requirements.txt),
    em vez de depender de um ffmpeg instalado no sistema — o Render (Web
    Service nativo, sem Dockerfile) não dá acesso de root pra instalar
    pacote de sistema operacional no build.

    Resolvido sob demanda (não na importação do módulo): se isso falhar por
    qualquer motivo, não pode derrubar o import de services.video — e com
    ele o app inteiro, já que operacoes_routes.py importa este módulo no
    carregamento das rotas.
    """
    global _ffmpeg_binario_cache, _ffmpeg_binario_falhou

    if _ffmpeg_binario_falhou:
        return None
    if _ffmpeg_binario_cache is None:
        try:
            import imageio_ffmpeg
            _ffmpeg_binario_cache = imageio_ffmpeg.get_ffmpeg_exe()
        except Exception:
            _ffmpeg_binario_falhou = True
            return None
    return _ffmpeg_binario_cache


def comprimir_video(file_obj, nome_original):
    """
    Recomprime um vídeo para no máximo 1280x720 antes do upload, já que
    celulares costumam gravar em resolução bem maior do que o necessário
    para conferir a aferição — e o Storage tem limite de tamanho por arquivo.

    Se o ffmpeg falhar por qualquer motivo (binário ausente, vídeo
    corrompido, codec não suportado) ou a compressão não render um arquivo
    menor, devolve os bytes originais sem alteração.

    Retorna (bytes, nome_arquivo, content_type_ou_None).
    """
    dados_originais = file_obj.read()
    extensao_original = os.path.splitext(nome_original)[1].lower()

    ffmpeg = _ffmpeg_binario()
    if ffmpeg is None:
        return dados_originais, nome_original, None

    with tempfile.NamedTemporaryFile(suffix=extensao_original or ".mp4") as entrada, \
         tempfile.NamedTemporaryFile(suffix=".mp4") as saida:
        entrada.write(dados_originais)
        entrada.flush()

        try:
            subprocess.run(
                [
                    ffmpeg, "-y",
                    "-i", entrada.name,
                    "-vf",
                    f"scale='min({LARGURA_MAXIMA},iw)':'min({ALTURA_MAXIMA},ih)':"
                    "force_original_aspect_ratio=decrease:force_divisible_by=2",
                    "-c:v", "libx264",
                    "-preset", "veryfast",
                    "-crf", str(CRF),
                    "-c:a", "aac",
                    "-b:a", "128k",
                    "-movflags", "+faststart",
                    saida.name,
                ],
                check=True,
                capture_output=True,
                timeout=TIMEOUT_SEGUNDOS,
            )
            with open(saida.name, "rb") as f:
                dados_comprimidos = f.read()
        except Exception:
            return dados_originais, nome_original, None

    if not dados_comprimidos or len(dados_comprimidos) >= len(dados_originais):
        return dados_originais, nome_original, None

    nome_comprimido = os.path.splitext(nome_original)[0] + ".mp4"
    return dados_comprimidos, nome_comprimido, "video/mp4"
