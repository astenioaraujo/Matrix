-- Menu de Saldos: a tela de lançamento passou a ser o item "Informar Saldos",
-- com permissão própria. Rodar depois de criar_tabelas_saldos.sql.

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo) VALUES
    ('FINANCEIRO', 'INFORMAR_SALDOS', 'Informar Saldos', 509, true);
