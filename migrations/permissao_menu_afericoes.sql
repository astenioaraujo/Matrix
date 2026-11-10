-- Aferições ganha a sua própria opção de menu no catálogo (10/11/2026).
-- Até aqui o botão Aferições de Operações abria com Informar OU Consultar
-- Aferições, e na tela de acessos não havia "Aferições" para marcar.
-- Mesmo princípio de PERFORMANCES/MENU_ABASTECIMENTOS: o submenu abre com a
-- opção de menu OU com qualquer uma das de dentro, então ninguém perde acesso.

BEGIN;

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT 'OPERACOES', 'MENU_AFERICOES', 'Menu de Aferições', 495, true
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo
    WHERE sistema = 'OPERACOES' AND opcao = 'MENU_AFERICOES'
);

-- Quem já usa Aferições recebe o menu marcado, para a tela de acessos
-- refletir o que a pessoa de fato enxerga.
INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT DISTINCT up.id_usuario, up.cod_empresa, 'OPERACOES', 'MENU_AFERICOES', true
  FROM usuarios_permissoes up
 WHERE up.sistema = 'OPERACOES'
   AND up.opcao IN ('INFORMAR_AFERICOES', 'CONSULTAR_AFERICOES')
   AND up.ativo
   AND NOT EXISTS (
       SELECT 1 FROM usuarios_permissoes x
        WHERE x.id_usuario = up.id_usuario
          AND x.cod_empresa = up.cod_empresa
          AND x.sistema = 'OPERACOES'
          AND x.opcao = 'MENU_AFERICOES'
   );

COMMIT;
