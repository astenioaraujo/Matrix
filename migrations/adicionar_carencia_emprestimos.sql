-- Carência em Empréstimos e Financiamentos (23/09/2026).
--
-- meses_carencia = quantas das primeiras linhas geradas nascem com
-- valor_principal = 0. O valor contratado é dividido só entre as
-- (quantidade_parcelas - meses_carencia) linhas restantes — quantidade_parcelas
-- continua sendo o total de linhas (carência incluída), como já era.
--
-- Não existe carência de juros separada: valor_juros nunca é calculado
-- automaticamente neste modelo (nasce zerado em toda linha, é digitado à
-- mão, célula por célula), então um segundo campo não teria efeito nenhum
-- na geração — decisão do cliente de não criar esse campo.

ALTER TABLE public.financeiro_emprestimos
    ADD COLUMN IF NOT EXISTS meses_carencia integer NOT NULL DEFAULT 0;
