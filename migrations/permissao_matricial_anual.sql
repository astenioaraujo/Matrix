-- Matricial Anual por Posto (Financeiro → Fluxo de Caixa).
--
-- Mesma matriz da Consulta Matricial, virada de lado: em vez de uma coluna por
-- posto, uma coluna por mês de um ano — de um posto ou de todos somados.
-- Nenhuma tabela nova: a tela lê `lancamentos`, como as demais matriciais.

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT 'FINANCEIRO', 'MATRICIAL_ANUAL', 'Matricial Anual por Posto', 565, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo
    WHERE sistema = 'FINANCEIRO' AND opcao = 'MATRICIAL_ANUAL'
);
