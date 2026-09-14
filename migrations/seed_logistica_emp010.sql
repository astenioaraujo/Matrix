-- Carga inicial da Logística na EMP010 (Lucena), tirada de 6 planilhas de
-- carregamento em Excel de 10/09/2026. Só cadastros — os pedidos não entram.
-- Idempotente: pode rodar de novo sem duplicar.
--
-- As planilhas não trazem a capacidade dos tanques: as carretas entram só com
-- a placa e os tanques precisam ser preenchidos em Configurações → Carretas.

-- CNPJ dos 18 postos que aparecem nas planilhas (faltam Bonito II,
-- Conceição II, Coremas I e II, Santana)
UPDATE filiais f SET cnpj = v.cnpj
FROM (VALUES
    (1,  '08.290.538/0001-90'),  -- Bonito I
    (3,  '09.332.743/0001-33'),  -- Itaporanga
    (4,  '09.332.743/0002-14'),  -- Serra Grande
    (5,  '05.988.476/0001-04'),  -- Conceição I
    (9,  '09.225.919/0001-58'),  -- SJ Piranhas
    (10, '54.036.808/0001-58'),  -- SJ Bonfim
    (11, '04.688.196/0001-00'),  -- Olivedos
    (12, '40.947.145/0001-19'),  -- Pocinhos I
    (13, '40.947.145/0002-08'),  -- Pocinhos II
    (14, '46.815.462/0001-68'),  -- Afogados I
    (15, '46.811.898/0001-89'),  -- Afogados II
    (16, '37.205.930/0001-91'),  -- SJ Egito
    (17, '50.602.146/0001-85'),  -- Milagres
    (18, '36.969.787/0001-41'),  -- Mauriti
    (19, '05.475.312/0001-75'),  -- Ibiara
    (21, '27.604.386/0001-05'),  -- Patos
    (22, '53.613.541/0001-51'),  -- Ipaumirim
    (23, '21.769.396/0001-06')   -- Catingueira
) AS v(cod_filial, cnpj)
WHERE f.cod_empresa = 'EMP010' AND f.cod_filial = v.cod_filial;

INSERT INTO logistica_carretas (cod_empresa, placa)
SELECT 'EMP010', p FROM (VALUES
    ('NQH5707'), ('TPB7E03'), ('QSF4I80'), ('SLC8G36'), ('OFG5987'), ('SLE3D07')
) AS v(p)
ON CONFLICT (cod_empresa, placa) DO NOTHING;

INSERT INTO logistica_cavalos (cod_empresa, placa)
SELECT 'EMP010', p FROM (VALUES
    ('OFA2I65'), ('UHS6C99'), ('QSA7F84'), ('SLA1H57'), ('QFR9963'), ('RLU8H15')
) AS v(p)
ON CONFLICT (cod_empresa, placa) DO NOTHING;

-- Motorista não tem índice único: a checagem é pelo CPF
INSERT INTO logistica_motoristas (cod_empresa, nome, cpf)
SELECT 'EMP010', v.nome, v.cpf FROM (VALUES
    ('LUCIVALDO',        '806.643.504-53'),
    ('RUBENS',           '080.376.664-50'),
    ('FRANCISCO VALDIR', '674.114.444-72'),
    ('KLEBER',           '057.180.364-46'),
    ('JOAO PAULO',       '045.419.974-01'),
    ('ADONIS BRUNO',     '096.773.944-60')
) AS v(nome, cpf)
WHERE NOT EXISTS (
    SELECT 1 FROM logistica_motoristas m
    WHERE m.cod_empresa = 'EMP010' AND m.cpf = v.cpf
);
