-- Performance de Gerentes → Acompanhamento de Metas: ranking dos postos pelo
-- percentual da meta atingido no mês. Nenhuma tabela nova — lê vendas_metas e
-- vendas_diarias. Não é concedida a ninguém: só o superusuário passa por bypass.
INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT v.sistema, v.opcao, v.descricao, v.ordem, TRUE
FROM (VALUES
    ('PERFORMANCES', 'ACOMPANHAMENTO_METAS', 'Performance de Gerentes - Acompanhamento de Metas', 1192)
) AS v(sistema, opcao, descricao, ordem)
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo p
    WHERE p.sistema = v.sistema AND p.opcao = v.opcao
);
