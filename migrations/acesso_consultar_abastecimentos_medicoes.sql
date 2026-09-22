-- Libera Performances -> Performance em Abastecimentos -> Consultar
-- Abastecimentos na EMP010 (Lucena) para os usuários de medições
-- (medicoes01..medicoesNN), no mesmo molde de
-- acesso_performance_gerentes_medicoes.sql.
--
-- São três opções porque o caminho tem três portas: MENU (o módulo),
-- MENU_ABASTECIMENTOS (o submenu) e CONSULTAR_ABASTECIMENTOS (a tela).
-- IMPORTAR_ABASTECIMENTOS fica de fora de propósito — eles só consultam.

INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT u.id_usuario, 'EMP010', 'PERFORMANCES', o.opcao, true
FROM usuarios u
CROSS JOIN (VALUES ('MENU'), ('MENU_ABASTECIMENTOS'), ('CONSULTAR_ABASTECIMENTOS')) AS o(opcao)
WHERE lower(u.email) LIKE 'medicoes%@lucena.com.br'
  AND u.ativo = true
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = true;
