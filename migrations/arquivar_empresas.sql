-- Empresas arquivadas (11/11/2026).
--
-- Empresa arquivada sai do seletor de empresas, dos atalhos do topo e não
-- pode mais ser acessada — mas NADA dela é apagado: filiais, lançamentos,
-- usuários vinculados, tudo continua no banco. Arquivar e desarquivar é feito
-- direto no banco (UPDATE abaixo), não há tela para isso. O superusuário vê a
-- lista das arquivadas em "Escolher empresa → Empresas arquivadas".
--
-- Código de empresa arquivada não pode ser reaproveitado: a linha continua
-- em `empresas`, então `uq_empresas_cod_empresa` já barra o repetido. O índice
-- abaixo fecha também as variações de grafia ("emp011", " EMP011 "), que o
-- UNIQUE simples deixaria passar e o sistema (que faz strip) leria como a
-- mesma empresa.

ALTER TABLE public.empresas
    ADD COLUMN IF NOT EXISTS arquivada boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS arquivada_em timestamp without time zone;

CREATE UNIQUE INDEX IF NOT EXISTS uq_empresas_cod_empresa_normalizado
    ON public.empresas (upper(btrim(cod_empresa)));

-- Primeira leva: Vilela (as três), O Closet e 30 Setembro AL.
UPDATE public.empresas
   SET arquivada = true,
       arquivada_em = COALESCE(arquivada_em, now())
 WHERE cod_empresa IN ('EMP011', 'EMP011ADM', 'EMP011CONV', 'EMP013', 'EMP014')
   AND NOT arquivada;

-- Para desarquivar:
-- UPDATE public.empresas SET arquivada = false, arquivada_em = NULL WHERE cod_empresa = 'EMP0XX';
