-- Aferições de Bicos — mesmo acesso de quem já tem Medições (EMP010 / Lucena)
--
-- Pedido do cliente (26/10/2026): a gerente Heloísa e os chefes de pista (um
-- por posto, e-mail medicoesNN@lucena.com.br — cada um já restrito à sua
-- única filial ativa em usuarios_filiais) passam a ter Informar/Consultar
-- Aferições do mesmo jeito que já têm Informar/Consultar Medições. A
-- restrição por filial não muda: continua vindo de usuarios_filiais, não da
-- permissão em si.

INSERT INTO public.usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT up.id_usuario, up.cod_empresa, 'OPERACOES', novo.opcao, TRUE
  FROM public.usuarios_permissoes up
  JOIN public.usuarios u ON u.id_usuario = up.id_usuario
  JOIN (VALUES
          ('INFORMAR_MEDICOES',  'INFORMAR_AFERICOES'),
          ('CONSULTAR_MEDICOES', 'CONSULTAR_AFERICOES')
       ) AS novo(antiga, opcao) ON novo.antiga = up.opcao
 WHERE up.sistema = 'OPERACOES'
   AND up.cod_empresa = 'EMP010'
   AND up.ativo = TRUE
   AND (u.email ILIKE 'medicoes%@lucena.com.br' OR u.email = 'redelucenalcm@gmail.com')
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = TRUE;
