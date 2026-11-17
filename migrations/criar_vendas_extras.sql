-- Vendas Extras (Vendas → Informar Vendas Extras), 17/11/2026
--
-- Venda real que não veio no relatório do WebPostos (ex.: carga vendida a um
-- cliente direto do tanque). Ela é gravada na PRÓPRIA vendas_diarias, marcada
-- com venda_extra = true, e não numa tabela à parte: assim ela entra sozinha
-- em tudo que lê as vendas (Consultar Vendas, Consulta de Estoques, Perdas e
-- Sobras, Resumo, Margem Unitária...) sem cada consulta precisar de um UNION.
--
-- O que ela NÃO pode fazer:
--   * ser apagada pela reimportação das vendas diárias — o DELETE por período
--     de limpar_importacao_diaria_periodo passou a pular venda_extra;
--   * contar para o gerente — Performance de Gerentes (Vendas, Acompanhamento
--     de Metas, prêmio e pontos) lê com sem_extras=True.
--
-- ADD COLUMN com default constante não reescreve a tabela (PG 11+).

ALTER TABLE public.vendas_diarias
    ADD COLUMN IF NOT EXISTS venda_extra boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS observacao character varying,
    ADD COLUMN IF NOT EXISTS atualizado_em timestamp without time zone;

CREATE INDEX IF NOT EXISTS idx_vendas_diarias_extras
    ON public.vendas_diarias (cod_empresa, data)
    WHERE venda_extra;

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo) VALUES
    ('VENDAS', 'INFORMAR_VENDAS_EXTRAS', 'Informar Vendas Extras', 728, true)
ON CONFLICT DO NOTHING;
