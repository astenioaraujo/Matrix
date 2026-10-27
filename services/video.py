import contextlib
import os
import re
import subprocess
import tempfile

LARGURA_MAXIMA = 1280
ALTURA_MAXIMA = 720
CRF = 28
TIMEOUT_SEGUNDOS = 300

_RE_DURACAO = re.compile(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)")

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


def _duracao_do_log(caminho_log):
    """Lê a duração (em segundos) da linha 'Duration: HH:MM:SS.ss' que o
    ffmpeg imprime no stderr ao abrir o vídeo de entrada — não depende do
    ffprobe (que o imageio-ffmpeg não empacota)."""
    try:
        with open(caminho_log, "r", errors="ignore") as f:
            conteudo = f.read()
    except OSError:
        return None

    m = _RE_DURACAO.search(conteudo)
    if not m:
        return None

    horas, minutos, segundos = m.groups()
    return int(horas) * 3600 + int(minutos) * 60 + float(segundos)


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
    e lê em disco; o stderr do ffmpeg vai pra um arquivo pequeno só pra
    extrair a duração (com `-nostats` ele não acumula uma linha por frame).

    Context manager: entrega (caminho_arquivo, nome_arquivo,
    content_type_ou_None, tamanho_bytes, duracao_segundos_ou_None) e apaga
    os arquivos temporários ao sair, com sucesso ou não.
    """
    extensao_original = os.path.splitext(nome_original)[1].lower()
    fd_entrada, caminho_entrada = tempfile.mkstemp(suffix=extensao_original or ".mp4")
    os.close(fd_entrada)
    fd_saida, caminho_saida = tempfile.mkstemp(suffix=".mp4")
    os.close(fd_saida)
    fd_log, caminho_log = tempfile.mkstemp(suffix=".log")
    os.close(fd_log)

    try:
        file_obj.save(caminho_entrada)

        comprimiu = False
        duracao_segundos = None
        ffmpeg = _ffmpeg_binario()

        if ffmpeg is not None:
            try:
                with open(caminho_log, "wb") as log:
                    subprocess.run(
                        [
                            ffmpeg, "-y", "-nostats", "-hide_banner",
                            "-i", caminho_entrada,
                            "-vf",
                            f"scale='min({LARGURA_MAXIMA},iw)':'min({ALTURA_MAXIMA},ih)':"
                            "force_original_aspect_ratio=decrease:force_divisible_by=2",
                            # preset/threads/lookahead reduzidos ao mínimo: a instância
                            # do Render tem só 512MB no total, e um vídeo de origem
                            # grande (4K) fazia o ffmpeg sozinho estourar esse limite
                            # com os buffers de decodificação/paralelismo padrão.
                            "-c:v", "libx264",
                            "-preset", "ultrafast",
                            "-crf", str(CRF),
                            "-threads", "1",
                            "-x264-params", "threads=1:lookahead_threads=1:rc-lookahead=10",
                            "-c:a", "aac",
                            "-b:a", "128k",
                            "-movflags", "+faststart",
                            caminho_saida,
                        ],
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=log,
                        timeout=TIMEOUT_SEGUNDOS,
                    )
                comprimiu = (
                    os.path.getsize(caminho_saida) > 0
                    and os.path.getsize(caminho_saida) < os.path.getsize(caminho_entrada)
                )
            except Exception:
                comprimiu = False
            finally:
                duracao_segundos = _duracao_do_log(caminho_log)

        if comprimiu:
            caminho_final = caminho_saida
            nome_final = os.path.splitext(nome_original)[0] + ".mp4"
            content_type = "video/mp4"
        else:
            caminho_final = caminho_entrada
            nome_final = nome_original
            content_type = None

        tamanho_bytes = os.path.getsize(caminho_final)
        yield caminho_final, nome_final, content_type, tamanho_bytes, duracao_segundos
    finally:
        for caminho in (caminho_entrada, caminho_saida, caminho_log):
            try:
                os.remove(caminho)
            except OSError:
                pass
