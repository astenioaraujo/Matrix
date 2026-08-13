-- A base da ANP é pública e nacional: os mesmos 45 mil postos valem para todas
-- as empresas. Guardá-la por `cod_empresa` só multiplicaria linhas idênticas e
-- obrigaria cada empresa a importar o mesmo arquivo. Quem enxerga e quem
-- importa passa a ser decidido só pela permissão (MERCADO/CONSULTAR_ANP e
-- MERCADO/IMPORTAR_CSV_ANP).
--
-- Roda sobre as tabelas criadas por criar_tabelas_mercado_anp.sql na versão
-- anterior (que tinha cod_empresa). Instalação nova já nasce sem a coluna e
-- não precisa deste arquivo.
--
-- Não há perda de dados: só existia a carga de uma empresa, então largar a
-- coluna transforma essas linhas na base global. O índice único
-- (cod_empresa, cnpj) cai junto com a coluna e volta como (cnpj) — o arquivo
-- da ANP não repete CNPJ.

ALTER TABLE public.mercado_anp_postos DROP COLUMN IF EXISTS cod_empresa;

-- Em importações a empresa vira auditoria (de onde o usuário estava logado),
-- não particionamento.
ALTER TABLE public.mercado_anp_importacoes
    RENAME COLUMN cod_empresa TO cod_empresa_origem;

ALTER TABLE public.mercado_anp_importacoes
    ALTER COLUMN cod_empresa_origem DROP NOT NULL;

DROP INDEX IF EXISTS ix_mercado_anp_postos_empresa_uf;
DROP INDEX IF EXISTS ix_mercado_anp_postos_empresa_municipio;
DROP INDEX IF EXISTS ix_mercado_anp_postos_empresa_bandeira;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mercado_anp_postos_cnpj
    ON public.mercado_anp_postos (cnpj);

CREATE INDEX IF NOT EXISTS ix_mercado_anp_postos_uf
    ON public.mercado_anp_postos (uf);

CREATE INDEX IF NOT EXISTS ix_mercado_anp_postos_municipio
    ON public.mercado_anp_postos (uf, municipio);

CREATE INDEX IF NOT EXISTS ix_mercado_anp_postos_bandeira
    ON public.mercado_anp_postos (bandeira);
