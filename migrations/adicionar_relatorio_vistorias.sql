-- Vistorias → Executar: texto livre ao final do checklist (o relatório do
-- vistoriador — o que não cabe na observação de um item). Uma coluna na própria
-- execução: é um texto por vistoria. Gravado pelo botão "Salvar relatório",
-- não por tecla.
ALTER TABLE public.vistorias_execucoes
    ADD COLUMN IF NOT EXISTS relatorio text;
