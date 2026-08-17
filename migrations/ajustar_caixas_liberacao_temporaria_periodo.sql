-- =====================================================================
-- Módulo Caixas — liberação temporária por PERÍODO e por FILIAL
--
-- O desenho original guardava só `data_liberada_desde` e a regra lia
-- apenas a linha mais recente (ORDER BY ativado_em DESC LIMIT 1). Duas
-- consequências ruins: liberar uma segunda data cancelava a primeira, e
-- não havia como liberar dias diferentes para filiais diferentes.
--
-- Agora cada linha é um período fechado (desde/até) para uma filial ou
-- para todas (cod_filial NULL), e TODAS as linhas ainda dentro do prazo
-- valem ao mesmo tempo — as liberações se somam.
-- =====================================================================

ALTER TABLE caixas_liberacao_temporaria
    ADD COLUMN IF NOT EXISTS data_liberada_ate date,
    ADD COLUMN IF NOT EXISTS cod_filial        integer,
    ADD COLUMN IF NOT EXISTS revogado_em       timestamp without time zone;

-- Linhas antigas: valiam "desde X até hoje". Como todas já expiraram
-- (a liberação dura poucas horas), fechar no dia da ativação basta.
UPDATE caixas_liberacao_temporaria
   SET data_liberada_ate = GREATEST(data_liberada_desde, ativado_em::date)
 WHERE data_liberada_ate IS NULL;

ALTER TABLE caixas_liberacao_temporaria
    ALTER COLUMN data_liberada_ate SET NOT NULL;

ALTER TABLE caixas_liberacao_temporaria
    DROP CONSTRAINT IF EXISTS caixas_liberacao_temporaria_periodo_check;
ALTER TABLE caixas_liberacao_temporaria
    ADD CONSTRAINT caixas_liberacao_temporaria_periodo_check
    CHECK (data_liberada_ate >= data_liberada_desde);

COMMENT ON COLUMN caixas_liberacao_temporaria.cod_filial IS
    'Filial liberada; NULL = todas as filiais da empresa.';
COMMENT ON COLUMN caixas_liberacao_temporaria.revogado_em IS
    'Preenchido quando a liberação é encerrada na mão antes de expirar.';
