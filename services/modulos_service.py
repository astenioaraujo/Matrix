"""
Módulos do menu principal (Escolha o sistema) — lista única.

É lida pela tela de escolha do sistema e pelos atalhos de módulo da barra de
navegação (`templates/components/nav.html`), para que o ícone de um módulo só
apareça para quem pode entrar nele e as duas telas nunca divirjam.
"""
from flask import request, session, url_for

# chave, sistema da permissão (opção MENU), nome, ícone, cor, endpoint,
# blueprints/endpoints que "moram" no módulo (para marcar o módulo atual)
MODULOS_SISTEMA = [
    {"chave": "pdv", "sigla": "PDV", "sistema": "PDV", "nome": "PDV Matrix", "icone": "🛒",
     "cor": "#0f9d58", "endpoint": "pdv.menu_pdv", "blueprints": ["pdv"]},
    {"chave": "operacoes", "sigla": "OPE", "sistema": "OPERACOES", "nome": "Operações", "icone": "⛽",
     "cor": "#e07a2f", "endpoint": "operacoes.menu_operacoes", "blueprints": ["operacoes"]},
    {"chave": "financeiro", "sigla": "FIN", "sistema": "FINANCEIRO", "nome": "Financeiro", "icone": "💰",
     "cor": "#2f66e0", "endpoint": "financeiro.menu_empresa",
     "blueprints": ["financeiro", "importacoes", "relatorios"]},
    {"chave": "vendas", "sigla": "VEN", "sistema": "VENDAS", "nome": "Vendas", "icone": "📊",
     "cor": "#2a8f4a", "endpoint": "sistema.menu_vendas",
     "blueprints": ["vendas"], "endpoints": ["sistema.menu_vendas"]},
    {"chave": "logistica", "sigla": "LOG", "sistema": "LOGISTICA", "nome": "Logística", "icone": "🚛",
     "cor": "#0e7490", "endpoint": "logistica.menu_logistica", "blueprints": ["logistica"]},
    {"chave": "compliance", "sigla": "COM", "sistema": "COMPLIANCE", "nome": "Compliance", "icone": "🛡️",
     "cor": "#4b5563", "endpoint": "compliance.menu_compliance", "blueprints": ["compliance"]},
    {"chave": "vistorias", "sigla": "VIS", "sistema": "VISTORIAS", "nome": "Vistorias", "icone": "📝",
     "cor": "#0f766e", "endpoint": "vistorias.menu_vistorias", "blueprints": ["vistorias"]},
    {"chave": "rh", "sigla": "RH", "sistema": "RH", "nome": "RH", "icone": "👔",
     "cor": "#7c3aed", "endpoint": "rh.menu_rh", "blueprints": ["rh"]},
    {"chave": "performances", "sigla": "PER", "sistema": "PERFORMANCES", "nome": "Performances", "icone": "🚀",
     "cor": "#c2410c", "endpoint": "performances.menu_performances", "blueprints": ["performances"]},
    {"chave": "treinamentos", "sigla": "TRE", "sistema": "TREINAMENTOS", "nome": "Treinamentos", "icone": "🎓",
     "cor": "#2563eb", "endpoint": "treinamentos.menu_treinamentos", "blueprints": ["treinamentos"]},
    {"chave": "canivete", "sigla": "CAN", "sistema": "CANIVETE", "nome": "Canivete Suíço", "icone": "🛠️",
     "cor": "#475569", "endpoint": "canivete.menu_canivete", "blueprints": ["canivete"]},
    {"chave": "projetos", "sigla": "PRO", "sistema": "PROJETOS", "nome": "Projetos", "icone": "📋",
     "cor": "#b45309", "endpoint": "projetos.menu_projetos", "blueprints": ["projetos"]},
    {"chave": "mercado", "sigla": "MER", "sistema": "MERCADO", "nome": "Mercado", "icone": "🏪",
     "cor": "#0369a1", "endpoint": "mercado.menu_mercado", "blueprints": ["mercado"]},
    {"chave": "configuracoes", "sigla": "CON", "sistema": "CONFIGURACOES", "nome": "Configurações", "icone": "⚙️",
     "cor": "#8e6bbf", "endpoint": "sistema.entrar_configuracoes",
     "blueprints": ["configuracoes"], "endpoints": ["sistema.entrar_configuracoes"]},
    {"chave": "usuarios", "sigla": "USU", "sistema": "USUARIOS", "nome": "Usuários", "icone": "👥",
     "cor": "#b02a37", "endpoint": "usuarios.menu_usuarios", "blueprints": ["usuarios"]},
]


def chaves_modulos_permitidos(id_usuario, cod_empresa, tipo_global):
    """Chaves dos módulos que o usuário pode abrir — UMA consulta só."""
    from services.pdv_service import empresa_opera_pdv

    cod_empresa = str(cod_empresa).strip()
    # a trava do PDV vale inclusive para o superusuário
    opera_pdv = empresa_opera_pdv(cod_empresa)

    if str(tipo_global or "").strip().lower() == "superusuario":
        return [m["chave"] for m in MODULOS_SISTEMA
                if m["chave"] != "pdv" or opera_pdv]

    from db import get_connection
    cur = get_connection().cursor()
    try:
        cur.execute("""
            SELECT DISTINCT sistema
            FROM usuarios_permissoes
            WHERE id_usuario = %s
              AND cod_empresa = %s
              AND ativo = TRUE
              AND opcao = 'MENU'
              AND sistema = ANY(%s)
        """, (id_usuario, cod_empresa, [m["sistema"] for m in MODULOS_SISTEMA]))
        liberados = {r[0] for r in cur.fetchall()}
    finally:
        cur.close()

    return [m["chave"] for m in MODULOS_SISTEMA
            if m["sistema"] in liberados and (m["chave"] != "pdv" or opera_pdv)]


def atualizar_cache_modulos():
    """Refaz a lista na sessão (chamada pela tela de escolha do sistema)."""
    id_usuario = session.get("id_usuario")
    cod_empresa = str(session.get("cod_empresa") or "").strip()
    chaves = chaves_modulos_permitidos(id_usuario, cod_empresa, session.get("tipo_global"))
    session["_modulos_topo"] = [id_usuario, cod_empresa, chaves]
    return chaves


def atalhos_modulos():
    """
    Ícones de módulo para a barra de navegação. Só é chamada dentro de
    nav.html, então página sem a barra não paga nada. Guardada na sessão,
    amarrada a (usuário, empresa); a tela de escolha do sistema renova. O
    acesso em si continua sendo checado em cada rota.
    """
    id_usuario = session.get("id_usuario")
    cod_empresa = str(session.get("cod_empresa") or "").strip()
    if not id_usuario or not cod_empresa:
        return []

    cache = session.get("_modulos_topo")
    if (isinstance(cache, list) and len(cache) == 3
            and cache[0] == id_usuario and cache[1] == cod_empresa):
        chaves = cache[2]
    else:
        try:
            chaves = atualizar_cache_modulos()
        except Exception:
            return []

    blueprint = request.blueprint
    endpoint = request.endpoint
    atalhos = []
    for m in MODULOS_SISTEMA:
        if m["chave"] not in chaves:
            continue
        atual = (blueprint in m["blueprints"]
                 or endpoint in m.get("endpoints", []))
        atalhos.append({
            "nome": m["nome"],
            "sigla": m["sigla"],
            "cor": m["cor"],
            "url": url_for(m["endpoint"]),
            "atual": atual,
        })
    return atalhos
