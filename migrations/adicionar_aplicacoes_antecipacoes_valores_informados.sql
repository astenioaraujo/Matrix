-- Nova linha "Aplicações e Antecipações (+)" no bloco Valores Informados da tela de Saldos.
-- Fica logo acima de Extras e soma na Variação Final.
ALTER TABLE valores_informados
    ADD COLUMN IF NOT EXISTS aplicacoes_antecipacoes numeric NOT NULL DEFAULT 0;
