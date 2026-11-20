-- Descarrego para Venda Extra (Operações → Informar Descarregos), 20/11/2026
--
-- A venda extra (Vendas → Informar Vendas Extras, vendas_diarias com
-- venda_extra = true) é combustível vendido direto da carga: ele sai de uma
-- compra, mas não entra no tanque. A única forma de baixar o saldo da compra
-- era um descarrego para coligada — que vira empréstimo, indevidamente.
--
-- A venda extra passa a ser a TERCEIRA ponta possível do descarrego, ao lado
-- da filial nossa e da coligada. O vínculo mora no descarrego
-- (id_venda_extra → vendas_diarias.id), e não o contrário:
--   * é o mesmo desenho de id_coligada — o descarrego diz para onde foi;
--   * vendas_diarias é a tabela grande das importações, e uma coluna de
--     Operações nela não teria sentido para 99,99% das linhas;
--   * a FK (sem CASCADE) impede apagar a venda extra que já baixou uma compra.
--
-- Uma venda extra só se liga a UM descarrego (índice único parcial). Que a
-- linha seja mesmo uma venda extra, que o produto e a quantidade batam e que
-- a data seja a mesma é checado no servidor (informar_descarregos_combustiveis).

ALTER TABLE public.descarregos_combustiveis
    ADD COLUMN IF NOT EXISTS id_venda_extra integer
        REFERENCES public.vendas_diarias (id);

-- exatamente uma das três pontas
ALTER TABLE public.descarregos_combustiveis
    DROP CONSTRAINT IF EXISTS ck_descarregos_parte;

ALTER TABLE public.descarregos_combustiveis
    ADD CONSTRAINT ck_descarregos_parte CHECK (
        num_nonnulls(cod_filial_descarga, id_coligada, id_venda_extra) = 1
    );

CREATE UNIQUE INDEX IF NOT EXISTS uq_descarregos_venda_extra
    ON public.descarregos_combustiveis (id_venda_extra)
    WHERE id_venda_extra IS NOT NULL;

-- a tabela já tinha RLS; repetido aqui por convenção (idempotente)
ALTER TABLE public.descarregos_combustiveis ENABLE ROW LEVEL SECURITY;
