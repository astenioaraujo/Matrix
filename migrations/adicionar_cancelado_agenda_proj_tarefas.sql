-- Cancelamento de metas e tarefas eventuais dos Projetos da Agenda.
-- Mesmo comportamento do ✖ das recorrências mensais: a linha sai da lista
-- mas não é apagada — volta a aparecer, riscada, pelo botão 👁.

ALTER TABLE agenda_proj_tarefas
    ADD COLUMN IF NOT EXISTS cancelado    boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS cancelado_em timestamp without time zone;
