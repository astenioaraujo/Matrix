-- Classificação gerencial dos títulos manuais — PDV Matrix.
--
-- O tipo de despesa passa a apontar para a classificação do Fluxo de Caixa do
-- Matrix (`grupos_gerenciais` + `contas_gerenciais`, por empresa).
--
-- **A classificação fica no TIPO, não no título.** Cada título já aponta para
-- o tipo; repetir grupo/conta na linha do título seria a mesma verdade em dois
-- lugares, que diverge na primeira correção. Reclassificar "Cartão de Crédito"
-- de 5.11 para outra conta conserta, de uma vez, todos os títulos daquele tipo
-- — inclusive os já pagos.
--
-- Não há FK composta para `contas_gerenciais` porque a tabela é alimentada por
-- importação e tem triggers que bloqueiam INSERT/DELETE; a consistência é
-- garantida pela tela, que só oferece contas existentes na empresa.

ALTER TABLE public.pdv_despesas_tipos
    ADD COLUMN IF NOT EXISTS cod_grupo integer;

ALTER TABLE public.pdv_despesas_tipos
    ADD COLUMN IF NOT EXISTS cod_conta integer;

-- `chave_origem` é a identidade da linha no arquivo importado (vencimento +
-- contato + descrição + valor). É ela que faz a carga ser repetível: rodar de
-- novo o mesmo arquivo não duplica título nenhum.
ALTER TABLE public.pdv_titulos_pagar
    ADD COLUMN IF NOT EXISTS chave_origem character varying;

CREATE UNIQUE INDEX IF NOT EXISTS uq_pdv_titulos_pagar_chave_origem
    ON public.pdv_titulos_pagar (cod_empresa, chave_origem)
    WHERE chave_origem IS NOT NULL;
