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


# ---------------------------------------------------------------------------
# Foto reduzida e carimbada (Drenagem de Tanques)
# ---------------------------------------------------------------------------
LADO_MAXIMO_FOTO = 1600
QUALIDADE_FOTO_REDUZIDA = 80
_TAG_EXIF_DATA_ORIGINAL = 36867   # DateTimeOriginal (sub-IFD Exif)
_TAG_EXIF_DATA = 306              # DateTime (IFD0)
_IFD_EXIF = 0x8769


def _data_da_foto(imagem):
    """Quando a foto foi tirada, pelo EXIF — None se a câmera não gravou."""
    from datetime import datetime

    try:
        exif = imagem.getexif()
    except Exception:
        return None

    candidatos = []
    try:
        candidatos.append(exif.get_ifd(_IFD_EXIF).get(_TAG_EXIF_DATA_ORIGINAL))
    except Exception:
        pass
    candidatos.append(exif.get(_TAG_EXIF_DATA))

    for texto in candidatos:
        if not texto:
            continue
        try:
            return datetime.strptime(str(texto).strip()[:19], "%Y:%m:%d %H:%M:%S")
        except ValueError:
            continue
    return None


def _carimbar(imagem, texto):
    """Faixa escura no rodapé da foto com o texto em branco."""
    from PIL import ImageDraw, ImageFont

    largura, altura = imagem.size
    tamanho = max(16, int(min(largura, altura) * 0.035))
    try:
        fonte = ImageFont.load_default(size=tamanho)
    except TypeError:  # Pillow < 10.1
        fonte = ImageFont.load_default()

    desenho = ImageDraw.Draw(imagem, "RGBA")
    caixa = desenho.textbbox((0, 0), texto, font=fonte)
    alt_texto = caixa[3] - caixa[1]
    margem = max(6, tamanho // 3)
    topo = altura - alt_texto - margem * 2
    desenho.rectangle([0, topo, largura, altura], fill=(0, 0, 0, 150))
    desenho.text((margem, topo + margem - caixa[1]), texto, font=fonte, fill=(255, 255, 255, 255))


@contextlib.contextmanager
def preparar_foto_reduzida(file_obj, nome_original, data_aparelho=None, legenda=""):
    """
    Reduz a foto para no máximo LADO_MAXIMO_FOTO px no lado maior (câmera de
    celular manda 4000+ px, à toa para conferir um volume drenado), corrige a
    rotação do EXIF, converte para JPEG e carimba no rodapé a data/hora em
    que a foto foi TIRADA — não a do envio.

    A data vem do EXIF; sem EXIF, de `data_aparelho` (o lastModified que o
    navegador informa — numa foto tirada na hora é o instante da captura).
    Sem as duas, a foto sobe sem carimbo e `capturado_em` volta None.

    Qualquer falha (formato que o Pillow não abre, HEIC sem suporte) sobe o
    arquivo original sem alteração — melhor a foto crua do que perdê-la.

    Entrega (caminho, nome_arquivo, content_type_ou_None, tamanho_bytes,
    capturado_em) e apaga os temporários ao sair.
    """
    extensao_original = os.path.splitext(nome_original)[1].lower()
    fd_entrada, caminho_entrada = tempfile.mkstemp(suffix=extensao_original or ".bin")
    os.close(fd_entrada)
    fd_saida, caminho_saida = tempfile.mkstemp(suffix=".jpg")
    os.close(fd_saida)

    try:
        file_obj.save(caminho_entrada)

        capturado_em = None
        convertida = False

        if extensao_original in EXTENSOES_HEIC:
            _registrar_heif()

        try:
            from PIL import Image, ImageOps
            with Image.open(caminho_entrada) as original:
                capturado_em = _data_da_foto(original) or data_aparelho
                imagem = ImageOps.exif_transpose(original).convert("RGB")
                imagem.thumbnail((LADO_MAXIMO_FOTO, LADO_MAXIMO_FOTO))
                if capturado_em:
                    texto = capturado_em.strftime("%d/%m/%Y %H:%M")
                    if legenda:
                        texto = f"{texto} · {legenda}"
                    _carimbar(imagem, texto)
                imagem.save(caminho_saida, "JPEG", quality=QUALIDADE_FOTO_REDUZIDA, optimize=True)
            convertida = os.path.getsize(caminho_saida) > 0
        except Exception:
            convertida = False
            capturado_em = capturado_em or data_aparelho

        if convertida:
            caminho_final = caminho_saida
            nome_final = os.path.splitext(nome_original)[0] + ".jpg"
            content_type = "image/jpeg"
        else:
            caminho_final = caminho_entrada
            nome_final = nome_original
            content_type = None

        yield caminho_final, nome_final, content_type, os.path.getsize(caminho_final), capturado_em
    finally:
        for caminho in (caminho_entrada, caminho_saida):
            try:
                os.remove(caminho)
            except OSError:
                pass
