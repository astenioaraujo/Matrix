-- =====================================================================
-- Saldos — importar Fiado e Cartões de Crédito do CR (Contas a Receber)
--
-- Mesma ideia de `origem_estoque`: a marca diz QUAL indicador recebe o
-- saldo importado, a tela não adivinha por nome. Valores: FIADO, CARTOES.
--
-- O saldo do dia D é conferido com a importação do CR do dia D+1 (o
-- arquivo é gerado na manhã seguinte e reflete o fechamento do dia D) —
-- o mesmo deslocamento da importação dos valores de estoque.
--
-- O controle de "esta linha veio de importação" continua em
-- saldos_importacoes_estoque: a chave lá é (empresa, data, área,
-- indicador), que serve igual para estas duas origens.
-- =====================================================================

ALTER TABLE indicadores_recebiveis
    ADD COLUMN IF NOT EXISTS origem_cr character varying;

UPDATE indicadores_recebiveis SET origem_cr = 'FIADO'
 WHERE cod_empresa = 'EMP010' AND nome = 'Fiado';

UPDATE indicadores_recebiveis SET origem_cr = 'CARTOES'
 WHERE cod_empresa = 'EMP010' AND nome = 'Cartões de Crédito';
