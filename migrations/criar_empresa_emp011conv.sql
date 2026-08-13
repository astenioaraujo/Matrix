-- Cria a empresa EMP011CONV (Vilela Conveniência), do Grupo Vilela.
-- Idempotente: pode rodar mais de uma vez.

INSERT INTO public.empresas (nome_fantasia, cod_empresa)
SELECT 'Vilela Conveniência', 'EMP011CONV'
WHERE NOT EXISTS (SELECT 1 FROM public.empresas WHERE cod_empresa = 'EMP011CONV');

INSERT INTO public.filiais (cod_empresa, cod_filial, nome_filial, ativo)
SELECT 'EMP011CONV', 1, 'Conveniência 1', true
WHERE NOT EXISTS (
    SELECT 1 FROM public.filiais WHERE cod_empresa = 'EMP011CONV' AND cod_filial = 1
);

-- Origem da importação do painel de vendas (padrão do grupo)
INSERT INTO public.vendas_parametros (cod_empresa, sistema_origem_painel)
VALUES ('EMP011CONV', 'WEBPOSTOS')
ON CONFLICT (cod_empresa) DO NOTHING;

-- Parâmetros de visualização de Saldos: tudo desligado (padrão fora da EMP010)
INSERT INTO public.saldos_configuracoes (cod_empresa, mostrar_recebiveis, mostrar_variacoes, mostrar_valores_informados)
VALUES ('EMP011CONV', false, false, false)
ON CONFLICT (cod_empresa) DO NOTHING;
