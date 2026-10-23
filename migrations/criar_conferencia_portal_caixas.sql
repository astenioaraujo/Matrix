-- Conferir Caixas → Conferência com portal (ex.: Prosegur)
--
-- Formas de recebimento marcadas com eh_conferencia (checkbox "Conferência"
-- em Conferir Caixas → Configurações, ao lado de "Fiado" e "Cartão") ganham
-- um ícone no cabeçalho da coluna do grid que abre uma janela comparando,
-- dia a dia, o valor lançado no caixa com o valor do portal — digitado à
-- mão pela conferidora. A diferença é calculada na consulta, nunca
-- persistida (como em todo o resto do sistema; ver convenção do schema).

ALTER TABLE public.caixas_formas_recebimento
    ADD COLUMN IF NOT EXISTS eh_conferencia boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS public.caixas_conferencia_portal (
    id serial PRIMARY KEY,
    cod_empresa varchar NOT NULL,
    cod_filial integer NOT NULL,
    data date NOT NULL,
    id_forma integer NOT NULL REFERENCES public.caixas_formas_recebimento(id) ON DELETE CASCADE,
    valor_portal numeric NOT NULL DEFAULT 0,
    atualizado_em timestamp without time zone DEFAULT now(),
    UNIQUE (cod_empresa, cod_filial, data, id_forma)
);

ALTER TABLE public.caixas_conferencia_portal ENABLE ROW LEVEL SECURITY;

-- Carga inicial pedida pelo cliente: habilita a conferência do PROSEGUR
-- na EMP012 (30 Set CZ).
UPDATE public.caixas_formas_recebimento
   SET eh_conferencia = true
 WHERE cod_empresa = 'EMP012' AND nome = 'PROSEGUR';
