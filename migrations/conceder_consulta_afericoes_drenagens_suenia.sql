-- Suênia (redelucenabsf2022@gmail.com, EMP010) passa a consultar Aferições e
-- Drenagens (10/11/2026): menu + consulta de cada um, sem Informar.
-- Ela já tem os 24 postos em usuarios_filiais — é daí que sai o que enxerga.

INSERT INTO public.usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT u.id_usuario, 'EMP010', 'OPERACOES', p.opcao, TRUE
  FROM public.usuarios u
 CROSS JOIN (VALUES
     ('MENU_AFERICOES'), ('CONSULTAR_AFERICOES'),
     ('MENU_DRENAGENS'), ('CONSULTAR_DRENAGENS')
 ) AS p(opcao)
 WHERE u.email = 'redelucenabsf2022@gmail.com'
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = TRUE;
