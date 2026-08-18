-- Hashtag do projeto na Programação do Dia.
--
-- Uma tarefa do dia escrita como "livro Eduardo #PS" pertence ao projeto cuja
-- hashtag é PS. Passado o dia dela (sem D, DU ou A para segurá-la), ela não
-- se perde: vira tarefa eventual desse projeto, na primeira posição e em
-- destaque — foi programada para um dia e não foi feita.

ALTER TABLE public.agenda_projetos
  ADD COLUMN IF NOT EXISTS hashtag character varying;

-- a hashtag é o endereço do projeto: não pode haver duas iguais por usuário
CREATE UNIQUE INDEX IF NOT EXISTS uq_agenda_projetos_hashtag
  ON public.agenda_projetos (id_usuario, upper(hashtag))
  WHERE hashtag IS NOT NULL AND ativo;

-- tarefa que veio da programação do dia entra marcada (cor de destaque)
ALTER TABLE public.agenda_proj_tarefas
  ADD COLUMN IF NOT EXISTS destaque boolean NOT NULL DEFAULT false;

-- marca de que a tarefa do dia já foi despejada no projeto (idempotência:
-- a varredura roda a cada abertura da agenda)
ALTER TABLE public.agenda_dia_tarefas
  ADD COLUMN IF NOT EXISTS migrada_em timestamp without time zone;
