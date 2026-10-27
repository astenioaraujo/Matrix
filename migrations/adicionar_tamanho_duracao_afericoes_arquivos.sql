-- Aferições: tamanho e duração de cada arquivo (27/10/2026)
--
-- A tela de Consultar Aferições passou a mostrar o tamanho (MB) e, pra
-- vídeo, a duração (segundos) de cada arquivo — pedido do cliente pra
-- conferir visualmente se a compressão no upload está funcionando.
--
-- Guardado no momento do upload (metadado do próprio arquivo, não dá pra
-- recalcular depois sem baixar o arquivo de novo do Storage) em vez de
-- recalculado a cada consulta.

ALTER TABLE public.afericoes_arquivos
    ADD COLUMN IF NOT EXISTS tamanho_bytes bigint,
    ADD COLUMN IF NOT EXISTS duracao_segundos numeric;
