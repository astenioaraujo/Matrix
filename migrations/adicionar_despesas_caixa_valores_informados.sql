-- Nova linha "Despesas do Caixa" no bloco Valores Informados da tela de Saldos.
-- Entra na Variação Final com o mesmo sinal de despesas (subtrai).
ALTER TABLE valores_informados
    ADD COLUMN IF NOT EXISTS despesas_caixa numeric NOT NULL DEFAULT 0;
