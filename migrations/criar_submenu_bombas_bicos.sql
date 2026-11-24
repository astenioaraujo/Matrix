-- Bombas e Bicos vira submenu (Cadastrar / Consultar), 24/11/2026.
--
-- A tela única de edição virou duas: Cadastrar (a mesma de antes, atrás de
-- CADASTRAR_BOMBAS_BICOS) e Consultar, só leitura (CONSULTAR_BOMBAS_BICOS).
-- MENU_BOMBAS_BICOS abre o submenu para quem só precisasse dele sem as
-- duas de dentro (mesmo princípio de MENU_AFERICOES/MENU_DRENAGENS).
--
-- A permissão antiga BOMBAS_BICOS (ordem 530) fica no catálogo sem uso —
-- igual a outras permissões substituídas no projeto, nunca é removida.

BEGIN;

INSERT INTO public.permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT v.sistema, v.opcao, v.descricao, v.ordem, true
  FROM (VALUES
      ('OPERACOES', 'MENU_BOMBAS_BICOS',      'Menu de Bombas e Bicos',   531),
      ('OPERACOES', 'CADASTRAR_BOMBAS_BICOS', 'Cadastrar Bombas e Bicos', 532),
      ('OPERACOES', 'CONSULTAR_BOMBAS_BICOS', 'Consultar Bombas e Bicos', 533)
  ) AS v(sistema, opcao, descricao, ordem)
 WHERE NOT EXISTS (
     SELECT 1 FROM public.permissoes_catalogo p
      WHERE p.sistema = v.sistema AND p.opcao = v.opcao
 );

-- Heloísa (32, EMP010): cadastrar e consultar.
INSERT INTO public.usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo, criado_em)
SELECT 32, 'EMP010', 'OPERACOES', v.opcao, true, now()
  FROM (VALUES ('CADASTRAR_BOMBAS_BICOS'), ('CONSULTAR_BOMBAS_BICOS')) AS v(opcao)
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = true;

-- Jefferson Logística (57) e Jefferson Rangel (79), EMP010: só consultar —
-- e a permissão de CONFIGURACOES que abre o menu até chegar lá (nenhum dos
-- dois tinha, ao contrário da Heloísa).
INSERT INTO public.usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo, criado_em)
SELECT u.id_usuario, 'EMP010', 'OPERACOES', v.opcao, true, now()
  FROM (VALUES (57), (79)) AS u(id_usuario)
 CROSS JOIN (VALUES ('CONFIGURACOES'), ('CONSULTAR_BOMBAS_BICOS')) AS v(opcao)
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = true;

COMMIT;
