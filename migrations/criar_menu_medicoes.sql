-- Medições ganha seu próprio botão de menu em Operações (26/11/2026).
-- Até aqui Informar Medições e Consultar Medições eram dois botões soltos
-- no menu de Operações; agora ficam atrás de um nível "Medições", mesmo
-- princípio de MENU_AFERICOES, MENU_DRENAGENS, MENU_EMPRESTIMOS,
-- MENU_DESCARREGOS e MENU_COMPRAS: o submenu abre com a opção de menu OU
-- com qualquer uma das de dentro, então ninguém perde acesso.
--
-- Atenção especial aqui: INFORMAR_MEDICOES é a permissão dos gerentes de
-- posto (usuários "Medições 01", "Medições 02"...) que lançam a medição
-- todo dia — o grant abaixo tem que cobrir TODOS eles, não só quem usa o
-- menu administrativo.

BEGIN;

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT 'OPERACOES', 'MENU_MEDICOES', 'Menu de Medições', 309, true
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo
    WHERE sistema = 'OPERACOES' AND opcao = 'MENU_MEDICOES'
);

-- Quem já informa ou consulta medições recebe o menu marcado, para a tela
-- de acessos refletir o que a pessoa de fato enxerga e para ninguém ficar
-- sem conseguir abrir o botão "Medições".
INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT DISTINCT up.id_usuario, up.cod_empresa, 'OPERACOES', 'MENU_MEDICOES', true
  FROM usuarios_permissoes up
 WHERE up.sistema = 'OPERACOES'
   AND up.opcao IN ('INFORMAR_MEDICOES', 'CONSULTAR_MEDICOES')
   AND up.ativo
   AND NOT EXISTS (
       SELECT 1 FROM usuarios_permissoes x
        WHERE x.id_usuario = up.id_usuario
          AND x.cod_empresa = up.cod_empresa
          AND x.sistema = 'OPERACOES'
          AND x.opcao = 'MENU_MEDICOES'
   );

COMMIT;
