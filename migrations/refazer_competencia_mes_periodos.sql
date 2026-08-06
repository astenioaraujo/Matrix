-- Competência vira "Período" do módulo Saldos: uma linha por mês/ano da EMPRESA
-- (não mais por área), com data de início e data de fim do período.
-- A tabela antiga não estava em uso de fato (2 linhas de teste, jun/26).

-- 1) Uma linha por (empresa, mês/ano): descarta as duplicatas por área.
DELETE FROM competencia_mes c
 USING competencia_mes menor
 WHERE c.cod_empresa = menor.cod_empresa
   AND c.mes_ano = menor.mes_ano
   AND c.id_competencia > menor.id_competencia;

ALTER TABLE competencia_mes DROP CONSTRAINT IF EXISTS competencia_mes_cod_empresa_id_area_mes_ano_key;
ALTER TABLE competencia_mes DROP COLUMN IF EXISTS id_area;

ALTER TABLE competencia_mes RENAME COLUMN data_corte_inicio TO data_inicio;
ALTER TABLE competencia_mes ADD COLUMN IF NOT EXISTS data_fim date;

-- Período existente sem fim: assume o último dia do próprio mês.
UPDATE competencia_mes
   SET data_fim = (date_trunc('month', mes_ano) + interval '1 month - 1 day')::date
 WHERE data_fim IS NULL;

ALTER TABLE competencia_mes ALTER COLUMN data_fim SET NOT NULL;

ALTER TABLE competencia_mes
  ADD CONSTRAINT competencia_mes_empresa_mes_key UNIQUE (cod_empresa, mes_ano);

ALTER TABLE competencia_mes
  ADD CONSTRAINT competencia_mes_datas_check CHECK (data_fim > data_inicio);
