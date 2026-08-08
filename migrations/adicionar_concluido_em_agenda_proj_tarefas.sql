-- Data da conclusão das metas / tarefas eventuais dos Projetos da Agenda.
-- A tela esconde o que está concluído; o botão 👁 mostra de volta, filtrando
-- por período — e para isso é preciso saber QUANDO cada uma foi concluída.
-- (As recorrências mensais não precisam: a competência já diz o mês.)

ALTER TABLE agenda_proj_tarefas
    ADD COLUMN IF NOT EXISTS concluido_em timestamp without time zone;

-- o que já estava concluído antes da coluna existir fica com a data de criação
UPDATE agenda_proj_tarefas
   SET concluido_em = COALESCE(criado_em, now())
 WHERE concluido AND concluido_em IS NULL;
