-- Reclassificação de lançamento EMP012: IZABEL M DA SILVA ME dentro da
-- 2.03.03.007 - MANUTENCAO PREDIAL vai para INVESTIMENTOS (5/1).
-- No WebPostos o nome vem com Z ("IZABEL"). O de-para de INVESTIMENTOS já
-- existe (criar_reclassificacoes_lancamentos.sql).
INSERT INTO public.reclassificacoes_lancamentos (cod_empresa, texto, codigo_conta, destino)
SELECT 'EMP012', 'IZABEL M DA SILVA ME', '2.03.03.007', 'INVESTIMENTOS'
WHERE NOT EXISTS (
    SELECT 1 FROM public.reclassificacoes_lancamentos
    WHERE cod_empresa = 'EMP012' AND texto = 'IZABEL M DA SILVA ME'
      AND codigo_conta = '2.03.03.007'
);
