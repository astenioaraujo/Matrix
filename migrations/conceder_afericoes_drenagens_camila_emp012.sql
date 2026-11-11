-- Camila (camila@30setembro.com.br, EMP012 - 30 Set CZ) passa a ter Aferições e
-- Drenagem de Tanques completas (11/11/2026): menu + informar + consultar de cada um.
-- Ela já tem os 11 postos da EMP012 em usuarios_filiais — é daí que sai o que enxerga.
-- Re-executável (ON CONFLICT reativa o que estiver desativado).

INSERT INTO public.usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT u.id_usuario, 'EMP012', 'OPERACOES', p.opcao, TRUE
  FROM public.usuarios u
 CROSS JOIN (VALUES
     ('MENU_AFERICOES'), ('INFORMAR_AFERICOES'), ('CONSULTAR_AFERICOES'),
     ('MENU_DRENAGENS'), ('INFORMAR_DRENAGENS'), ('CONSULTAR_DRENAGENS')
 ) AS p(opcao)
 WHERE u.email = 'camila@30setembro.com.br'
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = TRUE;
