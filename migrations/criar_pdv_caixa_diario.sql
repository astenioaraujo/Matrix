-- Caixa (diário) — PDV Matrix.
--
-- **Nenhuma tabela nova**, de propósito.
--
-- A tela de Caixa é uma forma de olhar e lançar sobre a estrutura que já
-- existe: `pdv_lancamentos_financeiros` (o extrato) e `pdv_contas_financeiras`
-- (as contas: caixa da loja, Banco do Brasil, Caixa Econômica...).
--
-- Uma tabela `lancamentos_caixa` separada partiria a verdade do caixa em duas:
-- a venda em dinheiro, a baixa de nota a prazo e o pagamento de título já
-- gravam em `pdv_lancamentos_financeiros`. Com duas tabelas, o caixa do dia
-- nunca fecharia com o Caixa Geral nem com o extrato da conta — e é
-- exatamente esse tipo de duplicidade que o documento da Inovai alerta a
-- evitar ("não existem tabelas multiuso... cada tabela deve possuir uma
-- finalidade claramente definida" — e, no sentido inverso, um mesmo fato não
-- pode morar em duas tabelas).

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT 'PDV', 'CAIXA', 'Caixa do dia', 1920, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo
    WHERE sistema = 'PDV' AND opcao = 'CAIXA'
);
