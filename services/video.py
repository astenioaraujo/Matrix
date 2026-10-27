import contextlib
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


@contextlib.contextmanager
def preparar_video_para_upload(file_obj, nome_original):
    """
    Grava o vídeo num arquivo temporário em disco e tenta recomprimir para
    no máximo 1280x720 antes do upload — celular grava em resolução bem
    maior do que o necessário pra conferir a aferição, e o Storage recusa
    objeto acima do limite do projeto.

    Nunca carrega o vídeo inteiro num bytes do Python: a instância no
    Render tem pouca RAM, e ler o arquivo inteiro (original + saída do
    ffmpeg) em memória já derrubou o serviço por estourar o limite. Grava
    e lê em disco, e o ffmpeg joga stdout/stderr fora (sem capturar em
    memória).

    Context manager: entrega (caminho_arquivo, nome_arquivo,
    content_type_ou_None) e apaga os arquivos temporários ao sair, com
    sucesso ou não.
    """
    extensao_original = os.path.splitext(nome_original)[1].lower()
    fd_entrada, caminho_entrada = tempfile.mkstemp(suffix=extensao_original or ".mp4")
    os.close(fd_entrada)
    fd_saida, caminho_saida = tempfile.mkstemp(suffix=".mp4")
    os.close(fd_saida)

    try:
        file_obj.save(caminho_entrada)

        comprimiu = False
        ffmpeg = _ffmpeg_binario()

        if ffmpeg is not None:
            try:
                subprocess.run(
                    [
                        ffmpeg, "-y",
                        "-i", caminho_entrada,
                        "-vf",
                        f"scale='min({LARGURA_MAXIMA},iw)':'min({ALTURA_MAXIMA},ih)':"
                        "force_original_aspect_ratio=decrease:force_divisible_by=2",
                        "-c:v", "libx264",
                        "-preset", "veryfast",
                        "-crf", str(CRF),
                        "-c:a", "aac",
                        "-b:a", "128k",
                        "-movflags", "+faststart",
                        caminho_saida,
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=TIMEOUT_SEGUNDOS,
                )
                comprimiu = (
                    os.path.getsize(caminho_saida) > 0
                    and os.path.getsize(caminho_saida) < os.path.getsize(caminho_entrada)
                )
            except Exception:
                comprimiu = False

        if comprimiu:
            yield caminho_saida, os.path.splitext(nome_original)[0] + ".mp4", "video/mp4"
        else:
            yield caminho_entrada, nome_original, None
    finally:
        for caminho in (caminho_entrada, caminho_saida):
            try:
                os.remove(caminho)
            except OSError:
                pass
