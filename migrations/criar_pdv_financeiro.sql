-- PDV Matrix — fase 4: Financeiro / Fluxo de Caixa.
--
-- A tabela de lançamentos (pdv_lancamentos_financeiros) já existe desde a
-- fase 1 e é a estrutura central: o extrato da empresa. Esta migration
-- acrescenta o que faltava para operá-la — as datas de baixa dos títulos, e
-- as permissões das telas.
--
-- Princípio que rege tudo aqui: **Fluxo de Caixa é dinheiro**. Título a
-- receber não é dinheiro; título a pagar não é saída. O lançamento nasce no
-- momento em que o valor efetivamente se movimenta.
--
--     Nota a Prazo → Títulos a Receber : NÃO gera lançamento (não é dinheiro)
--     Nota ou Título → Baixa           : GERA lançamento (entrou dinheiro)
--     Título a Pagar → Pagamento       : GERA lançamento (saiu dinheiro)
--
-- A transferência entre contas não usa tabela nova: são dois lançamentos
-- (−X numa conta, +X na outra) amarrados pelo mesmo id_transferencia, que é o
-- id do primeiro deles. Para a empresa não houve entrada nem saída — o Caixa
-- Geral precisa disso para não ler transferência interna como receita.

ALTER TABLE public.pdv_notas_prazo
    ADD COLUMN IF NOT EXISTS data_baixa date;

ALTER TABLE public.pdv_titulos_receber
    ADD COLUMN IF NOT EXISTS data_baixa date;

ALTER TABLE public.pdv_titulos_pagar
    ADD COLUMN IF NOT EXISTS data_baixa date;

-- Quem pagou/recebeu: a conta financeira que movimentou o dinheiro.
ALTER TABLE public.pdv_notas_prazo
    ADD COLUMN IF NOT EXISTS id_pdv_conta_financeira integer
        REFERENCES public.pdv_contas_financeiras (id_pdv_conta_financeira);

ALTER TABLE public.pdv_titulos_receber
    ADD COLUMN IF NOT EXISTS id_pdv_conta_financeira integer
        REFERENCES public.pdv_contas_financeiras (id_pdv_conta_financeira);

ALTER TABLE public.pdv_titulos_pagar
    ADD COLUMN IF NOT EXISTS id_pdv_conta_financeira integer
        REFERENCES public.pdv_contas_financeiras (id_pdv_conta_financeira);


-- ─── PERMISSÕES ──────────────────────────────────────────────────────────────

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT 'PDV', v.opcao, v.descricao, v.ordem, TRUE
FROM (VALUES
    ('FINANCEIRO_MENU', 'Menu do Financeiro',              1775),
    ('EXTRATO',         'Extrato por Conta Financeira',    1780),
    ('CAIXA_GERAL',     'Caixa Geral Consolidado',         1790),
    ('TRANSFERENCIAS',  'Transferências entre Contas',     1800),
    ('CONCILIACAO',     'Conciliação Bancária',            1810),
    ('TITULOS_RECEBER', 'Contas a Receber (títulos)',      1820),
    ('BAIXAR_PAGAR',    'Baixar Contas a Pagar',           1830)
) AS v(opcao, descricao, ordem)
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo p
    WHERE p.sistema = 'PDV' AND p.opcao = v.opcao
);
