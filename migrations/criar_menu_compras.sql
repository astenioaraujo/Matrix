-- Compras ganha seu próprio botão de menu em Operações (26/11/2026).
-- Até aqui Informar Preço de Compra, Informar Compras, Consultar Compras,
-- Consultar Resumo de Compras e Compras em Trânsito eram cinco botões
-- soltos no menu de Operações; agora ficam atrás de um nível "Compras",
-- mesmo princípio de OPERACOES/MENU_AFERICOES, MENU_DRENAGENS,
-- MENU_EMPRESTIMOS e MENU_DESCARREGOS: o submenu abre com a opção de menu
-- OU com qualquer uma das de dentro, então ninguém perde acesso.

BEGIN;

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT 'OPERACOES', 'MENU_COMPRAS', 'Menu de Compras', 359, true
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo
    WHERE sistema = 'OPERACOES' AND opcao = 'MENU_COMPRAS'
);

-- Quem já informa preço de compra, informa compras, consulta compras,
-- consulta resumo de compras ou consulta compras em trânsito recebe o
-- menu marcado, para a tela de acessos refletir o que a pessoa de fato
-- enxerga.
INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT DISTINCT up.id_usuario, up.cod_empresa, 'OPERACOES', 'MENU_COMPRAS', true
  FROM usuarios_permissoes up
 WHERE up.sistema = 'OPERACOES'
   AND up.opcao IN ('INFORMAR_PRECO_COMPRA', 'INFORMAR_COMPRAS', 'CONSULTAR_COMPRAS', 'CONSULTAR_RESUMO_COMPRAS', 'CONSULTAR_COMPRAS_TRANSITO')
   AND up.ativo
   AND NOT EXISTS (
       SELECT 1 FROM usuarios_permissoes x
        WHERE x.id_usuario = up.id_usuario
          AND x.cod_empresa = up.cod_empresa
          AND x.sistema = 'OPERACOES'
          AND x.opcao = 'MENU_COMPRAS'
   );

COMMIT;
