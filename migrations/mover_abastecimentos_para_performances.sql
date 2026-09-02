-- Abastecimentos saiu do menu de RH e virou o submenu
-- Performances -> Performance em Abastecimentos (13/08/2026).
-- O catalogo de permissoes ficou para tras: as duas opcoes continuavam
-- listadas em RH, onde ninguem mais as procura. Aqui elas mudam de sistema,
-- junto com as concessoes ja feitas, para que ninguem perca acesso.

BEGIN;

-- 1) O submenu passa a ter a sua propria opcao no catalogo.
INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT 'PERFORMANCES', 'MENU_ABASTECIMENTOS', 'Performance em Abastecimentos', 1160, true
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo
    WHERE sistema = 'PERFORMANCES' AND opcao = 'MENU_ABASTECIMENTOS'
);

-- 2) As duas opcoes trocam de sistema (o id nao muda).
UPDATE permissoes_catalogo
   SET sistema = 'PERFORMANCES',
       ordem = 1170
 WHERE sistema = 'RH'
   AND opcao = 'IMPORTAR_ABASTECIMENTOS';

UPDATE permissoes_catalogo
   SET sistema = 'PERFORMANCES',
       ordem = 1180
 WHERE sistema = 'RH'
   AND opcao = 'CONSULTAR_ABASTECIMENTOS';

-- 3) As concessoes acompanham: usuarios_permissoes guarda sistema/opcao
--    como texto, entao sem isto os acessos ja dados ficariam orfaos.
UPDATE usuarios_permissoes
   SET sistema = 'PERFORMANCES'
 WHERE sistema = 'RH'
   AND opcao IN ('IMPORTAR_ABASTECIMENTOS', 'CONSULTAR_ABASTECIMENTOS');

COMMIT;
