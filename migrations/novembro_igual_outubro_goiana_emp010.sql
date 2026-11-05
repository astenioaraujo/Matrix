-- O cliente desistiu do ajuste de migrations/ajustar_metas_novembro_emp010.sql:
-- novembro/2026 da EMP010 volta a ser igual a outubro/2026, posto a posto.
-- A Goiana (24), posto novo sem meta em outubro, fica com 100.000 L.
INSERT INTO vendas_metas (cod_empresa, cod_filial, ano, mes, meta_quantidade)
SELECT cod_empresa, cod_filial, 2026, 11, meta_quantidade
FROM vendas_metas
WHERE cod_empresa = 'EMP010' AND ano = 2026 AND mes = 10
ON CONFLICT (cod_empresa, cod_filial, ano, mes)
DO UPDATE SET meta_quantidade = EXCLUDED.meta_quantidade, atualizado_em = now();

INSERT INTO vendas_metas (cod_empresa, cod_filial, ano, mes, meta_quantidade)
VALUES ('EMP010', 24, 2026, 11, 100000)
ON CONFLICT (cod_empresa, cod_filial, ano, mes)
DO UPDATE SET meta_quantidade = EXCLUDED.meta_quantidade, atualizado_em = now();
