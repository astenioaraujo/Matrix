-- Drenagem de Tanques — acesso na EMP010 (Lucena), 10/11/2026
--
-- Pedido do cliente: mesma lógica das Aferições. Recebem Menu + Informar +
-- Consultar Drenagens:
--   - os chefes de pista, e-mail medicoesNN@lucena.com.br (medicoes01 = posto 1,
--     medicoes02 = posto 2…), cada um já restrito à sua única filial ativa em
--     usuarios_filiais — é dela que sai o posto, não da permissão;
--   - a gerente Heloísa (redelucenalcm@gmail.com) e Jefferson Rangel
--     (jefferson.rangel@lucena.com.br), que enxergam todos os postos.
--
-- Re-executável: rodar de novo depois de criar um medicoesNN novo propaga
-- sozinho (ON CONFLICT reativa o que estiver desativado).

INSERT INTO public.usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT u.id_usuario, 'EMP010', 'OPERACOES', p.opcao, TRUE
  FROM public.usuarios u
  JOIN public.usuarios_empresas ue
    ON ue.id_usuario = u.id_usuario AND ue.cod_empresa = 'EMP010'
 CROSS JOIN (VALUES ('MENU_DRENAGENS'), ('INFORMAR_DRENAGENS'), ('CONSULTAR_DRENAGENS')) AS p(opcao)
 WHERE u.ativo
   AND (
        u.email ILIKE 'medicoes%@lucena.com.br'
     OR u.email IN ('redelucenalcm@gmail.com', 'jefferson.rangel@lucena.com.br')
   )
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = TRUE;
