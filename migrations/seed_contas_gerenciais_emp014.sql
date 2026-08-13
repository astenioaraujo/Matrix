-- Plano de contas gerenciais da EMP014 (30 Setembro AL).
-- Cópia exata do plano da EMP012 (30 Set CZ) — decisão do cliente, para que as
-- duas empresas do grupo comparem entre si nos relatórios.
--
-- contas_gerenciais tem trigger que bloqueia INSERT (só a descrição pode ser
-- alterada depois). O seed desliga o trigger de bloqueio apenas durante a carga
-- e religa em seguida, dentro da mesma transação.

BEGIN;

ALTER TABLE contas_gerenciais DISABLE TRIGGER trg_bloquear_insert_delete_contas_gerenciais;

INSERT INTO contas_gerenciais (cod_empresa, cod_grupo, cod_conta, descricao, projetar)
SELECT 'EMP014', cod_grupo, cod_conta, descricao, projetar
  FROM contas_gerenciais
 WHERE cod_empresa = 'EMP012';

ALTER TABLE contas_gerenciais ENABLE TRIGGER trg_bloquear_insert_delete_contas_gerenciais;

COMMIT;
