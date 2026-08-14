-- Libera Performance de Gerentes na EMP010 (Lucena) para os usuários de
-- medições (medicoes01..medicoes23).
--
-- Concede só o caminho de gerentes: MENU (porta do módulo), PERFORMANCE_GERENTES
-- (o submenu) e GERENTES_VENDAS (a tela de vendas, único item do submenu hoje).
-- As avaliações de funcionários ficam de fora de propósito.

INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT u.id_usuario, 'EMP010', 'PERFORMANCES', o.opcao, true
FROM usuarios u
CROSS JOIN (VALUES ('MENU'), ('PERFORMANCE_GERENTES'), ('GERENTES_VENDAS')) AS o(opcao)
WHERE lower(u.email) LIKE 'medicoes%@lucena.com.br'
  AND u.ativo = true
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = true;
