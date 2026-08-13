-- Separa "Programar Vistorias" (criar a vistoria) de "Executar Vistorias"
-- (responder o questionário). Quem executa passa a enxergar apenas as
-- vistorias das filiais em usuarios_filiais; quem programa enxerga a empresa
-- inteira.
--
-- Não concede a permissão a ninguém: só o superusuário passa por bypass.
-- Liberar em Usuários → Permissões para quem for programar.

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo) VALUES
    ('VISTORIAS', 'PROGRAMAR_VISTORIAS', 'Programar Vistorias', 940, true)
ON CONFLICT DO NOTHING;
