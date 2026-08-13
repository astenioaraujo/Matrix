-- Permissão de acesso ao módulo Mercado.
-- Sem esta linha o botão só aparece para superusuário (que faz bypass).
-- Ordem 1500 (CANIVETE é 1300; 1400 fica reservado para PROJETOS, que ainda
-- não está no catálogo).

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo) VALUES
    ('MERCADO', 'MENU', 'Menu de Mercado', 1500, true)
ON CONFLICT DO NOTHING;
