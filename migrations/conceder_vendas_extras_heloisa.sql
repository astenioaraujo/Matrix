-- Vendas → Informar Vendas Extras para a Heloísa (id 32), EMP010 — 20/11/2026,
-- a pedido do cliente. O menu de Vendas (VENDAS/MENU) ela já tinha.
INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
VALUES (32, 'EMP010', 'VENDAS', 'MENU', TRUE),
       (32, 'EMP010', 'VENDAS', 'INFORMAR_VENDAS_EXTRAS', TRUE)
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao) DO UPDATE SET ativo = TRUE;
