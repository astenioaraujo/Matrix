-- Faltas de Caixa vira submenu (Importar / Consultar), 26/11/2026.
--
-- A tela única (upload + grade) separa como pedido do cliente: Importar é só
-- o formulário de upload; Consultar é a grade mensal, só leitura. MENU_ abre
-- o submenu sozinho (mesmo princípio de Capacidade de Tanques,
-- migrations/criar_submenu_capacidade_tanques.sql).
--
-- FALTAS_CAIXAS (antiga, 504) deixa de abrir qualquer tela — Clécia (74,
-- EMP012), única concessão existente, ganha as duas novas e perde a antiga.

BEGIN;

INSERT INTO public.permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT v.sistema, v.opcao, v.descricao, v.ordem, true
  FROM (VALUES
      ('FINANCEIRO', 'MENU_FALTAS_CAIXAS',      'Menu de Faltas de Caixa',      515),
      ('FINANCEIRO', 'IMPORTAR_FALTAS_CAIXAS',  'Importar Faltas de Caixa',     516),
      ('FINANCEIRO', 'CONSULTAR_FALTAS_CAIXAS', 'Consultar Faltas de Caixa',    517)
  ) AS v(sistema, opcao, descricao, ordem)
 WHERE NOT EXISTS (
     SELECT 1 FROM public.permissoes_catalogo p
      WHERE p.sistema = v.sistema AND p.opcao = v.opcao
 );

INSERT INTO public.usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo, criado_em)
SELECT 74, 'EMP012', 'FINANCEIRO', v.opcao, true, now()
  FROM (VALUES ('IMPORTAR_FALTAS_CAIXAS'), ('CONSULTAR_FALTAS_CAIXAS')) AS v(opcao)
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao)
DO UPDATE SET ativo = true;

UPDATE public.usuarios_permissoes
   SET ativo = false
 WHERE id_usuario = 74 AND cod_empresa = 'EMP012'
   AND sistema = 'FINANCEIRO' AND opcao = 'FALTAS_CAIXAS';

COMMIT;
