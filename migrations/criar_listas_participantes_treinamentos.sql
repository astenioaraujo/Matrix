-- Listas de participantes para Treinamentos: substitui o vínculo direto com
-- o cadastro de funcionários por listas nomeadas pelo usuário (ex.: "Frentistas
-- do Vilela", "Treinamentos avulsos"), com membros próprios — nome/cargo/filial
-- em texto livre, para caber gente de fora da empresa. Desativar uma lista
-- manda ela para o arquivo morto (fica fora do seletor de "Vincular
-- Participantes", mas não é excluída).
--
-- Migra os 161 vínculos já existentes (treinamentos_participantes.id_funcionario)
-- para uma lista arquivada "Funcionários (migrados)" por empresa, copiando
-- nome/cargo/filial de cada funcionário na data da migração — preserva o
-- histórico de certificados já emitidos. Depois disso remove id_funcionario:
-- corta o vínculo com o cadastro de funcionários.

BEGIN;

CREATE TABLE listas_participantes (
    id              SERIAL PRIMARY KEY,
    cod_empresa     VARCHAR NOT NULL,
    nome_lista      VARCHAR NOT NULL,
    ativo           BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em       TIMESTAMP NOT NULL DEFAULT NOW(),
    atualizado_em   TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE listas_participantes ENABLE ROW LEVEL SECURITY;

CREATE TABLE listas_participantes_membros (
    id              SERIAL PRIMARY KEY,
    cod_empresa     VARCHAR NOT NULL,
    id_lista        INTEGER NOT NULL REFERENCES listas_participantes(id) ON DELETE CASCADE,
    nome            VARCHAR NOT NULL,
    cargo           VARCHAR,
    filial          VARCHAR,
    ativo           BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em       TIMESTAMP NOT NULL DEFAULT NOW(),
    atualizado_em   TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE listas_participantes_membros ENABLE ROW LEVEL SECURITY;

CREATE INDEX idx_listas_participantes_membros_lista
    ON listas_participantes_membros (id_lista);

-- coluna temporária só para a migração: amarra o membro criado ao funcionário
-- de origem, para montar o UPDATE abaixo sem depender do nome (que pode se
-- repetir entre funcionários). Removida no fim desta mesma migração.
ALTER TABLE listas_participantes_membros
    ADD COLUMN _id_funcionario_origem INTEGER;

ALTER TABLE treinamentos_participantes
    ADD COLUMN id_participante INTEGER;

-- 1) uma lista arquivada "Funcionários (migrados)" por empresa que tem
--    vínculos hoje
INSERT INTO listas_participantes (cod_empresa, nome_lista, ativo)
SELECT DISTINCT cod_empresa, 'Funcionários (migrados)', FALSE
FROM treinamentos_participantes;

-- 2) um membro por funcionário hoje vinculado, copiando nome/cargo/filial
INSERT INTO listas_participantes_membros
    (cod_empresa, id_lista, nome, cargo, filial, _id_funcionario_origem)
SELECT
    fv.cod_empresa,
    nl.id,
    f.nome,
    c.descricao,
    fi.nome_filial,
    fv.id_funcionario
FROM (SELECT DISTINCT cod_empresa, id_funcionario FROM treinamentos_participantes) fv
JOIN listas_participantes nl
    ON nl.cod_empresa = fv.cod_empresa
   AND nl.nome_lista = 'Funcionários (migrados)'
JOIN funcionarios f
    ON f.cod_empresa = fv.cod_empresa
   AND f.id = fv.id_funcionario
LEFT JOIN cargos c
    ON c.cod_empresa = f.cod_empresa
   AND c.id = f.id_cargo
LEFT JOIN filiais fi
    ON fi.cod_empresa = f.cod_empresa
   AND fi.cod_filial = f.cod_filial;

-- 3) aponta cada vínculo existente para o novo membro
UPDATE treinamentos_participantes tp
SET id_participante = m.id
FROM listas_participantes_membros m
WHERE m.cod_empresa = tp.cod_empresa
  AND m._id_funcionario_origem = tp.id_funcionario;

ALTER TABLE listas_participantes_membros
    DROP COLUMN _id_funcionario_origem;

-- 4) só corta o vínculo se tudo migrou
DO $$
DECLARE
    pendentes INTEGER;
BEGIN
    SELECT COUNT(*) INTO pendentes
    FROM treinamentos_participantes
    WHERE id_participante IS NULL;

    IF pendentes > 0 THEN
        RAISE EXCEPTION 'Existem % vínculos sem id_participante migrado', pendentes;
    END IF;
END $$;

ALTER TABLE treinamentos_participantes
    ALTER COLUMN id_participante SET NOT NULL;

ALTER TABLE treinamentos_participantes
    ADD CONSTRAINT treinamentos_participantes_id_participante_fkey
    FOREIGN KEY (id_participante) REFERENCES listas_participantes_membros(id);

ALTER TABLE treinamentos_participantes
    DROP CONSTRAINT treinamentos_participantes_cod_empresa_id_treinamento_id_fu_key;

ALTER TABLE treinamentos_participantes
    ADD CONSTRAINT treinamentos_participantes_cod_emp_id_trein_id_part_key
    UNIQUE (cod_empresa, id_treinamento, id_participante);

ALTER TABLE treinamentos_participantes
    DROP COLUMN id_funcionario;

-- 5) nova opção de permissão para a tela de Listas de Participantes, e
--    concede a quem já tinha Vincular Participantes
INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
VALUES ('TREINAMENTOS', 'LISTAS_PARTICIPANTES', 'Listas de Participantes', 1225, TRUE)
ON CONFLICT (sistema, opcao) DO NOTHING;

INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
SELECT id_usuario, cod_empresa, 'TREINAMENTOS', 'LISTAS_PARTICIPANTES', ativo
FROM usuarios_permissoes
WHERE sistema = 'TREINAMENTOS'
  AND opcao = 'VINCULAR_PARTICIPANTES'
ON CONFLICT (id_usuario, cod_empresa, sistema, opcao) DO NOTHING;

COMMIT;
