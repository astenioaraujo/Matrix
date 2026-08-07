-- EMP013 (O Closet): a conta 4/6 não é usada (a empresa não tem equipamentos
-- de posto). Passa a ser "Despesas Comerciais".
--
-- Atenção: contas_gerenciais tem triggers que bloqueiam INSERT e DELETE e que
-- impedem alterar cod_empresa/cod_grupo/cod_conta. Só a descrição pode mudar.

UPDATE contas_gerenciais
   SET descricao = 'Despesas Comerciais'
 WHERE cod_empresa = 'EMP013'
   AND cod_grupo = 4
   AND cod_conta = 6;

-- A 3/1 não é usada (a empresa não vende combustível). Passa a ser
-- "Fornecedores".
UPDATE contas_gerenciais
   SET descricao = 'Fornecedores'
 WHERE cod_empresa = 'EMP013'
   AND cod_grupo = 3
   AND cod_conta = 1;

-- Toda compra de fornecedor foi concentrada na 3/1, então a 3/3 fica sem uso.
-- A conta não pode ser excluída (trigger), só a descrição é limpa.
UPDATE contas_gerenciais
   SET descricao = NULL
 WHERE cod_empresa = 'EMP013'
   AND cod_grupo = 3
   AND cod_conta = 3;

-- Frete usa a 3/2, que já é conta de frete. A 4/3 continua "Impostos Sobre
-- Lucro" (chegou a ser renomeada para Fretes e foi revertida).
UPDATE contas_gerenciais
   SET descricao = 'Impostos Sobre Lucro'
 WHERE cod_empresa = 'EMP013'
   AND cod_grupo = 4
   AND cod_conta = 3;
