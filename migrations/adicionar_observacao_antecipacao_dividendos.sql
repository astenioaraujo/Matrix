-- Observação na linha de antecipação de dividendos.
--
-- Passa a ser parte da identidade da linha: a mesma data pode repetir com
-- observações diferentes (ex.: "Bloqueio judicial - Brasil" e
-- "... - Itaú" no mesmo dia). Antes só cabia uma linha por filial/data.

ALTER TABLE antecipacao_dividendos
    ADD COLUMN IF NOT EXISTS observacao character varying NOT NULL DEFAULT '';

ALTER TABLE antecipacao_dividendos
    DROP CONSTRAINT IF EXISTS antecipacao_dividendos_cod_empresa_cod_filial_data_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_antecipacao_dividendos
    ON antecipacao_dividendos (cod_empresa, cod_filial, data, observacao);
