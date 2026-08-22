-- Resultado MB Anual por Posto (Financeiro → Fluxo de Caixa).
--
-- Mesmo Resultado por Margem Bruta, virado de lado: em vez de uma coluna por
-- posto num mês, uma coluna por mês de um ano — de um posto ou de todos
-- somados. Nenhuma tabela nova: lê `vendas_mb_sintetico` e `lancamentos`,
-- como a tela mensal.

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT 'FINANCEIRO', 'RESULTADO_MB_ANUAL', 'Resultado MB Anual por Posto', 525, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo
    WHERE sistema = 'FINANCEIRO' AND opcao = 'RESULTADO_MB_ANUAL'
);
