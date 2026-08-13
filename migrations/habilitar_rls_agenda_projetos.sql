-- Habilita RLS nas tabelas da Agenda/Projetos que ficaram de fora.
--
-- Alerta do Supabase (rls_disabled_in_public, 09/08/2026): sem RLS, qualquer
-- um com a URL do projeto e a chave anônima lê/edita essas tabelas pela API
-- REST. Mesmo padrão das demais tabelas do sistema: RLS ligado e nenhuma
-- policy — a aplicação acessa com a service role key, que faz bypass.

ALTER TABLE public.agenda_projetos            ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.agenda_proj_tarefas        ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.agenda_proj_recorrencias   ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.agenda_proj_rec_execucoes  ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.agenda_proj_journal        ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.agenda_dia_journal         ENABLE ROW LEVEL SECURITY;
