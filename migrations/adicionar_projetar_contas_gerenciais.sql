-- =====================================================================
-- Fluxo de Caixa Projetado — marcar quais contas entram na projeção
-- Contas com movimento atípico (ex.: empresas interligadas, aplicações)
-- não devem virar média para os meses futuros. Desmarcadas aqui, os meses
-- projetados saem em branco e não somam nos totais.
-- Padrão: todas marcadas (comportamento atual preservado).
-- Vale para os dois tipos de análise (Fluxo de Caixa e Margem Bruta).
-- =====================================================================

ALTER TABLE contas_gerenciais
    ADD COLUMN IF NOT EXISTS projetar boolean NOT NULL DEFAULT true;
