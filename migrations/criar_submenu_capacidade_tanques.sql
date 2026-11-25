-- Capacidade de Tanques vira submenu (Cadastrar / Consultar), 25/11/2026.
--
-- Cadastrar é a tela de edição que já existia (antes atrás só de
-- CONFIGURACOES); Consultar é a mesma, só leitura. MENU_ abre o submenu
-- sozinho (mesmo princípio de Bombas e Bicos).
--
-- A permissão antiga CAPACIDADE_TANQUES continua mostrando Fornecedores de
-- Combustíveis e Coligadas no menu de configurações — não mostra mais o botão
-- de Capacidade. Foi dada à Heloísa em 25/11/2026 só para chegar à tela de
-- tanques; aqui ela é desativada, para não abrir Fornecedores e Coligadas.

BEGIN;

INSERT INTO public.permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT v.sistema, v.opcao, v.descricao, v.ordem, true
  FROM (VALUES
      ('OPERACOES', 'MENU_CAPACIDADE_TANQUES',      'Menu de Capacidade de Tanques',   341),
      ('OPERACOES', 'CADASTRAR_CAPACIDADE_TANQUES', 'Cadastrar Capacidade de Tanques', 342),
      ('OPERACOES', 'CONSULTAR_CAPACIDADE_TANQUES', 'Consultar Capacidade de Tanques', 343)
  ) AS v(sistema, opcao, descricao, ordem)
 WHERE NOT EXISTS (
     SELECT 1 FROM public.permissoes_catalogo p
      WHERE p.sistema = v.sistema AND p.opcao = v.opcao
 );

-- Heloísa (32, EMP010): cadastrar e consultar.
INSERT INTO public.usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo, criado_em)
SELECT 32, 'EMP010', 'OPERACOES', v.opcao, true, now()
  FROM (VALUES ('CADASTRAR_CAPACIDADE_TANQUES'), ('CONSULTAR_CAPACIDADE_TANQUES')) AS v(opcao)
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = true;

UPDATE public.usuarios_permissoes
   SET ativo = false
 WHERE id_usuario = 32 AND cod_empresa = 'EMP010'
   AND sistema = 'OPERACOES' AND opcao = 'CAPACIDADE_TANQUES';

-- Jefferson Logística (57) e Jefferson Rangel (79), EMP010: só consultar
-- (CONFIGURACOES, que abre o menu, já têm desde Bombas e Bicos).
INSERT INTO public.usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo, criado_em)
SELECT u.id_usuario, 'EMP010', 'OPERACOES', v.opcao, true, now()
  FROM (VALUES (57), (79)) AS u(id_usuario)
 CROSS JOIN (VALUES ('CONFIGURACOES'), ('CONSULTAR_CAPACIDADE_TANQUES')) AS v(opcao)
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = true;

COMMIT;
