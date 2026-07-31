-- =====================================================================
-- Conferir Caixas — abrir/fechar coluna passa a valer também para os
-- controles adicionais, não só para as formas de recebimento.
--
-- A tabela nasceu presa a id_forma. Agora guarda (tipo, id_item), no mesmo
-- padrão de _tabela_detalhe(): 'forma' ou 'controle'. As linhas que já
-- existiam são todas de forma de recebimento, então o DEFAULT resolve.
--
-- Continua de fora a coluna DATA repetida do bloco de controles, e as
-- colunas fixas do sistema (TOTAL, TOTAL CX, FALTAS / SOBRAS).
-- =====================================================================

ALTER TABLE caixas_colunas_visiveis
    ADD COLUMN tipo character varying NOT NULL DEFAULT 'forma';

ALTER TABLE caixas_colunas_visiveis
    RENAME COLUMN id_forma TO id_item;

ALTER TABLE caixas_colunas_visiveis
    DROP CONSTRAINT caixas_colunas_visiveis_unica;

ALTER TABLE caixas_colunas_visiveis
    ADD CONSTRAINT caixas_colunas_visiveis_unica
    UNIQUE (cod_empresa, cod_filial, tipo, id_item);

ALTER TABLE caixas_colunas_visiveis
    ADD CONSTRAINT caixas_colunas_visiveis_tipo_valido
    CHECK (tipo IN ('forma', 'controle'));
