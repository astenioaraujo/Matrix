-- Descrição própria no título a pagar — PDV Matrix.
--
-- Fornecedor e descrição estavam no mesmo campo: a carga do Contas a Pagar
-- d'O Closet jogou o "Contato" do arquivo em `nome_fornecedor`, e ali há de
-- tudo — fornecedor de verdade (LIVE, RECCO), mas também nome de funcionária
-- ("MARIA CLARA"), de sócio e rótulo genérico ("CARTÃO", "EMPRESA"). Nenhum
-- desses é fornecedor, e tratá-los como tal enche o cadastro de gente que
-- nunca vendeu nada para a loja.
--
-- Agora são dois campos: `descricao` é sempre o que se está pagando, e o
-- fornecedor (`id_pdv_fornecedor` / `nome_fornecedor`) só é preenchido quando
-- existe fornecedor mesmo — nos demais casos fica em branco, e o texto do
-- contato entra na descrição para não se perder.

ALTER TABLE public.pdv_titulos_pagar
    ADD COLUMN IF NOT EXISTS descricao character varying;

-- Os títulos que já existiam guardavam a descrição em `observacao`.
UPDATE public.pdv_titulos_pagar
   SET descricao = observacao
 WHERE descricao IS NULL AND observacao IS NOT NULL;

-- Permissão da tela nova de fluxo de caixa das obrigações.
INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT 'PDV', 'FLUXO_CAIXA_PAGAR', 'Fluxo de caixa de contas a pagar', 1960, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo
    WHERE sistema = 'PDV' AND opcao = 'FLUXO_CAIXA_PAGAR'
);
