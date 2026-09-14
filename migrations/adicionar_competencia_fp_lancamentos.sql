-- Finanças Pessoais: mês de competência do lançamento, separado da data do pagamento.
-- O boleto de agosto pago em setembro tem data em setembro e competência em agosto,
-- e é pela competência que Lançar e Consultar escolhem o mês.
-- Guardada sempre como o dia 1 do mês.

ALTER TABLE public.fp_lancamentos ADD COLUMN IF NOT EXISTS competencia date;

-- até aqui a competência era a própria data
UPDATE public.fp_lancamentos
   SET competencia = date_trunc('month', data)::date
 WHERE competencia IS NULL;

ALTER TABLE public.fp_lancamentos ALTER COLUMN competencia SET NOT NULL;

CREATE INDEX IF NOT EXISTS ix_fp_lancamentos_usuario_competencia
    ON public.fp_lancamentos (id_usuario, competencia);
