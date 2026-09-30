-- CR → Cartões → Vendas por Dia por Bandeira
--
-- Quanto se vendeu em cada cartão, dia a dia, a partir do que os caixas
-- lançaram em Conferir Caixas (caixas_lancamentos). Não há tabela nova: tudo
-- é calculado na consulta.
--
-- Qual forma de recebimento é cartão varia de empresa para empresa — o
-- `agrupamento` não serve (na EMP010 o CHEQUE está em "CARTÃO FROTA"; na EMP012
-- nada tem agrupamento). Então é uma marca no cadastro, como `eh_fiado`,
-- editada em Conferir Caixas → Configurações.

ALTER TABLE public.caixas_formas_recebimento
    ADD COLUMN IF NOT EXISTS eh_cartao boolean NOT NULL DEFAULT false;

-- Carga inicial: débito, crédito e cartões frota, menos o cheque.
UPDATE public.caixas_formas_recebimento
   SET eh_cartao = true
 WHERE agrupamento IN ('DÉBITO', 'CRÉDITO', 'CARTÃO FROTA', 'FROTA')
   AND nome <> 'CHEQUE';

-- Sem agrupamento cadastrado: pelo nome.
UPDATE public.caixas_formas_recebimento
   SET eh_cartao = true
 WHERE (cod_empresa = 'EMP012' AND nome IN ('CRD GET', 'DB GET', 'CARTÃO PRÉ'))
    OR (cod_empresa = 'EMP013' AND nome IN ('DÉBITO', 'CRÉDITO'));

INSERT INTO public.permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT 'FINANCEIRO', 'CARTOES_VENDAS_DIA', 'CR Cartões — Vendas por Dia por Bandeira', 1495, TRUE
 WHERE NOT EXISTS (SELECT 1 FROM public.permissoes_catalogo p
                    WHERE p.sistema = 'FINANCEIRO' AND p.opcao = 'CARTOES_VENDAS_DIA');
