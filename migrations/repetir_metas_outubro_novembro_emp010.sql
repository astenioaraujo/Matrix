-- Repete em novembro/2026 as metas de outubro/2026 da EMP010 (Lucena), a pedido
-- do cliente. Posto que já tiver meta em novembro fica como está.
INSERT INTO vendas_metas (cod_empresa, cod_filial, ano, mes, meta_quantidade)
SELECT cod_empresa, cod_filial, 2026, 11, meta_quantidade
FROM vendas_metas
WHERE cod_empresa = 'EMP010' AND ano = 2026 AND mes = 10
ON CONFLICT (cod_empresa, cod_filial, ano, mes) DO NOTHING;
