-- Descarregos ganha seu próprio botão de menu em Operações (26/11/2026).
-- Até aqui Informar Descarregos, Consultar Descarregos e Consultar Resumo
-- de Descarregos eram três botões soltos no menu de Operações; agora ficam
-- atrás de um nível "Descarregos", mesmo princípio de
-- OPERACOES/MENU_AFERICOES e OPERACOES/MENU_DRENAGENS
-- (migrations/permissao_menu_afericoes.sql, criar_drenagens_tanques.sql):
-- o submenu abre com a opção de menu OU com qualquer uma das de dentro,
-- então ninguém perde acesso.

BEGIN;

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT 'OPERACOES', 'MENU_DESCARREGOS', 'Menu de Descarregos', 409, true
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo
    WHERE sistema = 'OPERACOES' AND opcao = 'MENU_DESCARREGOS'
);

-- Quem já informa ou consulta descarregos recebe o menu marcado, para a
-- tela de acessos refletir o que a pessoa de fato enxerga.
INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT DISTINCT up.id_usuario, up.cod_empresa, 'OPERACOES', 'MENU_DESCARREGOS', true
  FROM usuarios_permissoes up
 WHERE up.sistema = 'OPERACOES'
   AND up.opcao IN ('INFORMAR_DESCARREGOS', 'CONSULTAR_DESCARREGOS', 'CONSULTAR_RESUMO_DESCARREGOS')
   AND up.ativo
   AND NOT EXISTS (
       SELECT 1 FROM usuarios_permissoes x
        WHERE x.id_usuario = up.id_usuario
          AND x.cod_empresa = up.cod_empresa
          AND x.sistema = 'OPERACOES'
          AND x.opcao = 'MENU_DESCARREGOS'
   );

COMMIT;
