-- Cada item do menu de Saldos com permissão própria no catálogo.
-- MENU_SALDOS (505) abre o menu; as opções abaixo liberam cada rotina.
--   509 INFORMAR_SALDOS            (já criada)
--   511 ANTECIPACAO_DIVIDENDOS     (já criada)
--   512 CONSULTAR_VARIACOES_SALDOS
--   513 CONSULTAR_VARIACOES_FILIAL
--   514 CONFIGURAR_SALDOS

INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo) VALUES
    ('FINANCEIRO', 'CONSULTAR_VARIACOES_SALDOS', 'Consultar Variações', 512, true),
    ('FINANCEIRO', 'CONSULTAR_VARIACOES_FILIAL', 'Consultar Variações por Filial', 513, true),
    ('FINANCEIRO', 'CONFIGURAR_SALDOS', 'Configurar Saldos', 514, true)
ON CONFLICT DO NOTHING;

-- Quem já consultava saldos continua consultando as variações.
INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT up.id_usuario, up.cod_empresa, 'FINANCEIRO', nova.opcao, TRUE
  FROM usuarios_permissoes up
 CROSS JOIN (VALUES ('CONSULTAR_VARIACOES_SALDOS'), ('CONSULTAR_VARIACOES_FILIAL')) AS nova(opcao)
 WHERE up.sistema = 'FINANCEIRO' AND up.opcao = 'CONSULTA_SALDOS' AND up.ativo
   AND NOT EXISTS (SELECT 1 FROM usuarios_permissoes x
                    WHERE x.id_usuario = up.id_usuario AND x.cod_empresa = up.cod_empresa
                      AND x.sistema = 'FINANCEIRO' AND x.opcao = nova.opcao);

-- Quem cadastrava contas bancárias continua configurando saldos.
INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT up.id_usuario, up.cod_empresa, 'FINANCEIRO', 'CONFIGURAR_SALDOS', TRUE
  FROM usuarios_permissoes up
 WHERE up.sistema = 'FINANCEIRO' AND up.opcao = 'CADASTRO_CONTAS_BANCARIAS' AND up.ativo
   AND NOT EXISTS (SELECT 1 FROM usuarios_permissoes x
                    WHERE x.id_usuario = up.id_usuario AND x.cod_empresa = up.cod_empresa
                      AND x.sistema = 'FINANCEIRO' AND x.opcao = 'CONFIGURAR_SALDOS');
