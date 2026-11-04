-- Ajuste de metas de novembro/2026 da EMP010 (Lucena), a pedido do cliente.
-- Goiana (24) não tinha meta e entra agora; os demais substituem a repetida de outubro.
INSERT INTO vendas_metas (cod_empresa, cod_filial, ano, mes, meta_quantidade)
VALUES
    ('EMP010', 13, 2026, 11,  65000),  -- Pocinhos II
    ('EMP010',  6, 2026, 11,  60000),  -- Conceição II
    ('EMP010', 11, 2026, 11,  60000),  -- Olivedos
    ('EMP010',  8, 2026, 11,  80000),  -- Coremas II
    ('EMP010',  4, 2026, 11,  50000),  -- Serra Grande
    ('EMP010', 21, 2026, 11, 220000),  -- Patos
    ('EMP010', 16, 2026, 11, 220000),  -- SJ Egito
    ('EMP010', 24, 2026, 11, 100000),  -- Goiana
    ('EMP010', 10, 2026, 11, 200000),  -- SJ Bonfim
    ('EMP010', 22, 2026, 11, 350000)   -- Ipaumirim
ON CONFLICT (cod_empresa, cod_filial, ano, mes)
DO UPDATE SET meta_quantidade = EXCLUDED.meta_quantidade, atualizado_em = now();
