-- Separa o acesso por área de SALDOS do acesso por área de CAIXAS.
--
-- Até aqui os dois liam caixas_acessos: quem consultava a área no caixa
-- consultava em saldos. Isso não se sustenta — a mesma pessoa mexe com saldos
-- em uma empresa e com caixa em outra. A partir daqui saldos volta a ler
-- usuarios_areas_saldos, que ganha os mesmos dois flags de caixas_acessos.

ALTER TABLE public.usuarios_areas_saldos
    ADD COLUMN IF NOT EXISTS pode_consultar boolean NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS pode_alterar   boolean NOT NULL DEFAULT TRUE;

-- 1) Quem já estava em usuarios_areas_saldos E em caixas_acessos herda os
--    flags de caixas — é o que vale hoje na tela de Saldos.
UPDATE public.usuarios_areas_saldos uas
   SET pode_consultar = ca.pode_consultar,
       pode_alterar   = ca.pode_alterar,
       atualizado_em  = NOW()
  FROM public.caixas_acessos ca
 WHERE ca.cod_empresa = uas.cod_empresa
   AND ca.id_usuario  = uas.id_usuario
   AND ca.id_area     = uas.id_area;

-- 2) Quem tinha acesso só por caixas_acessos passa a ter a linha própria de
--    saldos, com os mesmos flags. Sem isso essas pessoas perderiam o acesso
--    que hoje enxergam. A linha de resumo (id_area NULL) não entra: saldos
--    não tem aba de todas as áreas.
INSERT INTO public.usuarios_areas_saldos
       (cod_empresa, id_usuario, id_area, ativo, pode_consultar, pode_alterar)
SELECT ca.cod_empresa, ca.id_usuario, ca.id_area, TRUE, ca.pode_consultar, ca.pode_alterar
  FROM public.caixas_acessos ca
 WHERE ca.id_area IS NOT NULL
ON CONFLICT (cod_empresa, id_usuario, id_area)
DO UPDATE SET ativo          = TRUE,
              pode_consultar = EXCLUDED.pode_consultar,
              pode_alterar   = EXCLUDED.pode_alterar,
              atualizado_em  = NOW();
