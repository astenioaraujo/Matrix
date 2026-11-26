-- Descarrego lançado a partir de Informar Medições: motorista e hora (26/11/2026).
--
-- Até aqui "Quantidade Descarregada" em Informar Medições era um número
-- digitado direto (AJAX de célula), sem ligação com compra nem motorista.
-- Passa a existir um lápis ao lado do campo que abre uma janela: escolhe a
-- compra pendente (trânsito), informa quantidade real, motorista e hora, e
-- o sistema grava em descarregos_combustiveis (mesma tabela de sempre) e
-- soma o total do dia em medicoes.quantidade_descarregada.
--
-- Os dados antigos, digitados direto no campo, ficam como estão — não têm
-- (e não vão ganhar) motorista nem compra ligada; é por isso que id_motorista
-- e hora_descarrego nascem opcionais. O relatório de Perdas e Sobras por
-- Motorista reconhece esse histórico pela ausência de linhas em
-- descarregos_combustiveis para o dia/filial/produto, e aproxima a
-- diferença (não por motorista algum).

ALTER TABLE public.descarregos_combustiveis
    ADD COLUMN IF NOT EXISTS id_motorista integer REFERENCES logistica_motoristas(id_motorista),
    ADD COLUMN IF NOT EXISTS hora_descarrego time without time zone;

-- Botão "Perdas e Sobras por Motorista" em Operações → Medições.
INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
SELECT 'OPERACOES', 'CONSULTAR_PERDAS_SOBRAS_MOTORISTA', 'Consultar Perdas e Sobras por Motorista', 534, true
WHERE NOT EXISTS (
    SELECT 1 FROM permissoes_catalogo
    WHERE sistema = 'OPERACOES' AND opcao = 'CONSULTAR_PERDAS_SOBRAS_MOTORISTA'
);

-- Quem já informa medições recebe o acesso de cara (mesmo público da tela
-- que vai gerar os dados desse relatório).
INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT DISTINCT up.id_usuario, up.cod_empresa, 'OPERACOES', 'CONSULTAR_PERDAS_SOBRAS_MOTORISTA', true
  FROM usuarios_permissoes up
 WHERE up.sistema = 'OPERACOES'
   AND up.opcao = 'INFORMAR_MEDICOES'
   AND up.ativo
   AND NOT EXISTS (
       SELECT 1 FROM usuarios_permissoes x
        WHERE x.id_usuario = up.id_usuario
          AND x.cod_empresa = up.cod_empresa
          AND x.sistema = 'OPERACOES'
          AND x.opcao = 'CONSULTAR_PERDAS_SOBRAS_MOTORISTA'
   );
