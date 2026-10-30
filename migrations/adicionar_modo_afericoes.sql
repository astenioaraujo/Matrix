-- Aferições: campo "modo" do teste (29/10/2026)
--
-- Como os 20 litros foram aplicados no teste: 'alta' (vazão alta), 'baixa'
-- (vazão baixa) ou 'digitado' (não foi escoado de verdade — valor digitado,
-- velocidade normal). Mesma convenção de afericoes_arquivos.tipo (texto
-- livre em minúsculo, validado na aplicação, não em CHECK — poucas opções,
-- fixas no próprio <select> do formulário).

ALTER TABLE public.afericoes
    ADD COLUMN IF NOT EXISTS modo character varying;

UPDATE public.afericoes SET modo = 'digitado' WHERE modo IS NULL;

ALTER TABLE public.afericoes ALTER COLUMN modo SET NOT NULL;
