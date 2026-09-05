-- Consultar Vendas Sintéticas (VENDAS/CONSULTAR_VENDAS_SINTETICAS, 800)
--
-- A tela nasceu reusando CONSULTAR_PAINEL enquanto servia de comparação com o
-- painel importado. Agora que ela é a tela verdadeira e o Painel saiu do menu,
-- ela ganha permissão própria — senão o dia em que CONSULTAR_PAINEL for
-- desativado levaria junto o acesso à tela que ficou.
--
-- `usuarios_permissoes` guarda sistema/opcao como TEXTO, então as concessões
-- são copiadas aqui: sem isso quem via o painel perderia a tela nova.

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
VALUES ('VENDAS', 'CONSULTAR_VENDAS_SINTETICAS',
        'Consultar Vendas Sintéticas', 800, TRUE)
ON CONFLICT DO NOTHING;

INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT id_usuario, cod_empresa, 'VENDAS', 'CONSULTAR_VENDAS_SINTETICAS', ativo
FROM usuarios_permissoes
WHERE sistema = 'VENDAS'
  AND opcao = 'CONSULTAR_PAINEL'
  AND NOT EXISTS (
        SELECT 1
        FROM usuarios_permissoes p
        WHERE p.id_usuario = usuarios_permissoes.id_usuario
          AND p.cod_empresa = usuarios_permissoes.cod_empresa
          AND p.sistema = 'VENDAS'
          AND p.opcao = 'CONSULTAR_VENDAS_SINTETICAS'
  );
