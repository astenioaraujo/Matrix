-- Vistorias → Acompanhamento de Notas: ranking dos postos pela nota da
-- vistoria do mês (ou o ano de um posto, de janeiro a dezembro). Nenhuma
-- tabela nova — lê vistorias_execucoes. Não é concedida a ninguém: só o
-- superusuário passa por bypass.
INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT v.sistema, v.opcao, v.descricao, v.ordem, TRUE
FROM (VALUES
    ('VISTORIAS', 'ACOMPANHAMENTO_NOTAS', 'Acompanhamento de Notas', 950)
) AS v(sistema, opcao, descricao, ordem)
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo p
    WHERE p.sistema = v.sistema AND p.opcao = v.opcao
);
