-- CR → Fiado → Movimento do Fiado por Dia
--
-- Entradas (fiado novo lançado nos caixas) × saídas (recebimentos por dia,
-- importados do relatório de Notas/Duplicatas a Receber) e o saldo do dia.
-- Não há tabela nova: tudo é calculado na consulta sobre caixas_lancamentos
-- e cr_recebimentos_dia.
--
-- Qual forma de recebimento do caixa é "fiado" varia de empresa para empresa
-- (NOTA / VALE na EMP010; CLIENTES / VALES nas outras, a confirmar), então é
-- uma marca no cadastro das formas, e não um nome fixo no código. Ela é
-- editada em Conferir Caixas → Configurações.

ALTER TABLE public.caixas_formas_recebimento
    ADD COLUMN IF NOT EXISTS eh_fiado boolean NOT NULL DEFAULT false;

UPDATE public.caixas_formas_recebimento
   SET eh_fiado = true
 WHERE cod_empresa = 'EMP010' AND nome = 'NOTA / VALE';

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo) VALUES
    ('FINANCEIRO', 'FIADO_MOVIMENTO_DIA', 'CR — Movimento do Fiado por Dia', 2120, true)
ON CONFLICT DO NOTHING;
