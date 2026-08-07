-- Classificações automáticas da EMP013 (O Closet)
-- Textos = categorias do CSV de Contas a Pagar do O Closet.
-- O importador grava o histórico como "<Categoria> | <Descrição>", então o
-- casamento por texto contido no histórico encontra a categoria.
--
-- Grupos/contas conforme contas_gerenciais da EMP013. As linhas marcadas
-- "-- REVISAR" foram um palpite e precisam de confirmação do cliente.

INSERT INTO classificacoes_automaticas (cod_empresa, texto, cod_grupo, cod_conta, complemento)
SELECT * FROM (VALUES
    ('EMP013', 'Taxas de pagamento',                 4, 11, NULL::text),
    ('EMP013', 'Tarifa bancária',                    4, 11, NULL),
    ('EMP013', 'IOF',                                4,  1, NULL),
    ('EMP013', 'DAS / Simples Nacional',             4,  1, NULL),
    ('EMP013', 'ICMS sobre Vendas',                  4,  1, NULL),
    ('EMP013', 'Impostos sobre vendas',              4,  1, NULL),
    ('EMP013', 'Juros pagos',                        4, 15, NULL),

    ('EMP013', 'Salários',                           4,  2, NULL),
    ('EMP013', 'Rescisões trabalhistas',             4,  2, NULL),
    ('EMP013', 'Encargos da folha',                  4,  2, NULL),
    ('EMP013', 'FGTS',                               4,  2, NULL),
    ('EMP013', 'Comissões de vendedoras',            4,  2, NULL),
    ('EMP013', 'Comissões',                          4,  2, NULL),
    ('EMP013', 'Pró-labore',                         4,  7, NULL),

    ('EMP013', 'Despesas administrativas diversas',  4,  4, NULL),
    ('EMP013', 'Despesas administrativas',           4,  4, NULL),
    ('EMP013', 'Despesas comerciais',                4,  6, NULL),  -- conta 4/6 renomeada para "Despesas Comerciais"
    ('EMP013', 'Honorários contábeis',               4,  4, NULL),
    ('EMP013', 'Software e assinaturas',             4,  4, NULL),
    ('EMP013', 'MARCAS E PATENTES',                  4,  4, NULL),
    ('EMP013', 'Internet',                           4,  8, NULL),
    ('EMP013', 'Aluguel da loja',                    4,  9, NULL),
    ('EMP013', 'Marketing e divulgação',             4, 13, NULL),
    ('EMP013', 'Manutenção de veículos',             4, 14, NULL),

    ('EMP013', 'Compras de fornecedores',            3,  1, NULL),
    ('EMP013', 'Compra de insumos e matéria prima',  3,  1, NULL),
    ('EMP013', 'Custo de aquisição (compra)',        3,  1, NULL),
    ('EMP013', 'Frete pago (Melhor Envio)',          3,  2, NULL),

    ('EMP013', 'Transferência entre contas',         7,  1, NULL),
    ('EMP013', 'Transferências de saída',            7,  1, NULL),
    ('EMP013', 'Devolução de aporte',                2,  1, NULL),

    -- Redutora de receita: classificada como venda. O valor é negativo como o
    -- de todo o arquivo (contas a pagar).
    ('EMP013', 'Devoluções de vendas',               1,  1, NULL),

    -- Textos sem categoria no CSV, casados pela descrição do lançamento
    ('EMP013', 'Emprestimo SICRED',                  5,  7, NULL),
    ('EMP013', 'Hevilyn Gabriele de Oliveira',       4,  2, NULL),  -- funcionária: comissão
    ('EMP013', 'VINDI PAGAMENTOS ONLINE',            3,  2, NULL),  -- gateway de frete
    ('EMP013', 'LIVE ROUPAS ESPORTIVAS',             3,  1, NULL),  -- conta 3/1 renomeada para "Fornecedores"
    ('EMP013', 'ICMS LIVE',                          4,  1, NULL)
) AS v(cod_empresa, texto, cod_grupo, cod_conta, complemento)
WHERE NOT EXISTS (
    SELECT 1
    FROM classificacoes_automaticas ca
    WHERE ca.cod_empresa = v.cod_empresa
      AND LOWER(TRIM(ca.texto)) = LOWER(TRIM(v.texto))
);
