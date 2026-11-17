-- Reclassificação de lançamento EMP012: SERVE BOMBAS dentro da
-- 2.03.03.007 - MANUTENCAO PREDIAL vai para INVESTIMENTOS (5/1).
-- A Serve Bombas aparece também na 2.03.03.020 e na 2.03.02.005, que
-- entram logo abaixo (uma regra por conta).
INSERT INTO public.reclassificacoes_lancamentos (cod_empresa, texto, codigo_conta, destino)
SELECT 'EMP012', 'SERVE BOMBAS', '2.03.03.007', 'INVESTIMENTOS'
WHERE NOT EXISTS (
    SELECT 1 FROM public.reclassificacoes_lancamentos
    WHERE cod_empresa = 'EMP012' AND texto = 'SERVE BOMBAS'
      AND codigo_conta = '2.03.03.007'
);

-- Incluídas depois, a pedido do cliente: a Serve Bombas da 2.03.03.020
-- (parcelas da NF 8) e da 2.03.02.005 (adiantamento) também é investimento.
INSERT INTO public.reclassificacoes_lancamentos (cod_empresa, texto, codigo_conta, destino)
SELECT 'EMP012', 'SERVE BOMBAS', c.codigo, 'INVESTIMENTOS'
FROM (VALUES ('2.03.03.020'), ('2.03.02.005')) AS c(codigo)
WHERE NOT EXISTS (
    SELECT 1 FROM public.reclassificacoes_lancamentos r
    WHERE r.cod_empresa = 'EMP012' AND r.texto = 'SERVE BOMBAS'
      AND r.codigo_conta = c.codigo
);
