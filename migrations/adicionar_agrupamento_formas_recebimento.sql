-- =====================================================================
-- Formas de recebimento — agrupamento livre para o resumo por origem
--
-- Campo de texto opcional. A maioria das formas fica em branco (aparece
-- sozinha no resumo, com o próprio nome). Quem tiver o mesmo agrupamento
-- (ex.: "PIX" em PIX REDE, PIX CNPJ, PIX BB) entra somado numa única linha
-- do resumo por origem do dinheiro, nas abas de totais do Conferir Caixas.
-- =====================================================================

ALTER TABLE caixas_formas_recebimento
    ADD COLUMN agrupamento character varying;
