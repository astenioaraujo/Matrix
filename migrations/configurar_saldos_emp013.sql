-- Configuração do módulo Saldos para a EMP013 (O Closet) — 31/08/2026
-- As contas bancárias (Banco do Brasil, Itaú, Sicredi, Mercado Pago, Rede, Stone)
-- já foram inseridas; este arquivo cobre o que falta para a tela abrir.

-- 1) Parâmetros de visualização.
--    Só o bloco de bancos + variações: os cartões (Rede, Stone, Mercado Pago)
--    entraram como conta bancária, então Estoques/Recebíveis não se aplica aqui.
INSERT INTO saldos_configuracoes
    (cod_empresa, mostrar_recebiveis, mostrar_variacoes, mostrar_valores_informados)
VALUES ('EMP013', false, true, false)
ON CONFLICT (cod_empresa) DO NOTHING;

-- 2) Acesso por área. Saldos lê caixas_acessos (a mesma tabela de Caixas),
--    não usuarios_areas_saldos. Área 5 = "Área I", ligada à filial 1 (Loja 1).
INSERT INTO caixas_acessos (cod_empresa, id_usuario, id_area, pode_consultar, pode_alterar)
VALUES ('EMP013', 77, 5, true, true);   -- 77 = Eduarda (eduarda@ocloset.com.br)

-- 3) Permissões de Financeiro/Saldos para a Eduarda.
--    Remova as linhas que não quiser conceder.
INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
VALUES
    (77, 'EMP013', 'FINANCEIRO', 'MENU',                      true),
    (77, 'EMP013', 'FINANCEIRO', 'MENU_SALDOS',               true),
    (77, 'EMP013', 'FINANCEIRO', 'CONSULTA_SALDOS',           true),
    (77, 'EMP013', 'FINANCEIRO', 'LANCAMENTO_SALDOS',         true),
    (77, 'EMP013', 'FINANCEIRO', 'CADASTRO_CONTAS_BANCARIAS', true),
    (77, 'EMP013', 'FINANCEIRO', 'CONFIGURAR_SALDOS',         true);
