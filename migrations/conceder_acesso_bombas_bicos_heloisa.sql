-- Acesso a Bombas e Bicos (e confirma Configurações de Operações) para a
-- Heloísa na EMP010 (Lucena), 24/11/2026. Re-executável.

BEGIN;

INSERT INTO public.usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo, criado_em)
SELECT 32, 'EMP010', 'OPERACOES', v.opcao, true, now()
  FROM (VALUES ('CONFIGURACOES'), ('BOMBAS_BICOS')) AS v(opcao)
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = true;

COMMIT;
