-- Empréstimos ganha seu próprio botão de menu em Operações (26/11/2026).
-- Até aqui Consultar Empréstimos e Consultar Saldo de Empréstimos eram dois
-- botões soltos no menu de Operações; agora ficam atrás de um nível
-- "Empréstimos", mesmo princípio de OPERACOES/MENU_AFERICOES e
-- OPERACOES/MENU_DRENAGENS (migrations/permissao_menu_afericoes.sql,
-- criar_drenagens_tanques.sql): o submenu abre com a opção de menu OU com
-- qualquer uma das de dentro, então ninguém perde acesso.

BEGIN;

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT 'OPERACOES', 'MENU_EMPRESTIMOS', 'Menu de Empréstimos', 459, true
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo
    WHERE sistema = 'OPERACOES' AND opcao = 'MENU_EMPRESTIMOS'
);

-- Quem já consulta empréstimos ou saldo de empréstimos recebe o menu
-- marcado, para a tela de acessos refletir o que a pessoa de fato enxerga.
INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT DISTINCT up.id_usuario, up.cod_empresa, 'OPERACOES', 'MENU_EMPRESTIMOS', true
  FROM usuarios_permissoes up
 WHERE up.sistema = 'OPERACOES'
   AND up.opcao IN ('CONSULTAR_EMPRESTIMOS', 'CONSULTAR_SALDO_EMPRESTIMOS')
   AND up.ativo
   AND NOT EXISTS (
       SELECT 1 FROM usuarios_permissoes x
        WHERE x.id_usuario = up.id_usuario
          AND x.cod_empresa = up.cod_empresa
          AND x.sistema = 'OPERACOES'
          AND x.opcao = 'MENU_EMPRESTIMOS'
   );

COMMIT;
