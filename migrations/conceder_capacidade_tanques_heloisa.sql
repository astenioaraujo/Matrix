-- Acesso a Capacidade de Tanques (Operações → Configurações) para a Heloísa
-- (id 32) na EMP010 (Lucena), 25/11/2026. Ela já tinha CONFIGURACOES.
-- Re-executável.

BEGIN;

INSERT INTO public.usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo, criado_em)
SELECT 32, 'EMP010', 'OPERACOES', v.opcao, true, now()
  FROM (VALUES ('CONFIGURACOES'), ('CAPACIDADE_TANQUES')) AS v(opcao)
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = true;

COMMIT;
