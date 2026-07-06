# Módulo Financeiro — Matrix
## Especificação Funcional: Controle de Saldos

Baseado na análise das planilhas "SALDOS DIÁRIOS" (Área 1 - Carol e Área 2 - Thalyta) e na descrição do fluxo de negócio.

---

## 1. Ajuste no menu do Módulo Financeiro

Ordem proposta:

1. **Saldos** *(novo)*
2. Fluxo de Caixa
3. Fluxo de Caixa Projetado
4. Empréstimos e Financiamentos
5. Cadastros

---

## 2. Cadastros necessários

### 2.1 Áreas (já existe)
Continua sendo usada para:
- Agrupar as filiais/postos (Área 1, Área 2, ...)
- Definir a **sequência de exibição** das colunas na tela de Saldos (confirmado: nas planilhas a ordem dos postos segue exatamente o cadastro de área — ex. Área 1: Bonito I, Bonito II, Conceição I, Conceição II, Afogados I, Afogados II, Ibiara, Pocinhos II, Coremas I, Coremas II).

### 2.2 Contas Bancárias *(novo)*
Cadastro simples, **por empresa** (não por área — confirmado que todas as áreas de uma mesma empresa compartilham o mesmo conjunto de bancos; se uma filial não usa um banco, o lançamento fica zerado, e não o cadastro excluído):
| Campo | Observação |
|---|---|
| Banco | Ex.: Banco do Brasil, Itaú, Sicoob, Bradesco |
| Nome/Apelido da conta | Ex.: "Saldo Conta/WEB" também é tratado como uma "conta" na planilha |
| Ordem de exibição | Replica a ordem das linhas na planilha |
| Ativo | Sim/Não |

### 2.3 Vínculo Conta Bancária × Filial
Nem toda filial tem todas as contas (ex.: Afogados I não tem saldo em Banco do Brasil na planilha). Sugiro cadastro de vínculo (conta × filial) ou simplesmente permitir célula vazia/zero na tela de lançamento — o segundo é mais simples e reflete melhor o comportamento observado.

### 2.4 Indicadores de Estoques/Recebíveis *(novo — corrigido)*
**Correção importante**: assim como as contas bancárias, os itens do segundo bloco (Cartões de Crédito, Fiado, Cheque, Compra Combustível, Em Trânsito, Estoque) **não podem ser fixos no código** — a planilha de junho é só uma carga histórica pontual; a partir de agora o preenchimento é manual e diário, então os rótulos que aparecem do lado esquerdo da tela (e para os quais a variação é calculada) precisam vir de um cadastro configurável por empresa, no mesmo padrão do cadastro de contas bancárias:

| Campo | Observação |
|---|---|
| Nome do indicador | Ex.: "Cartões de Crédito", "Fiado", "Cheque", "Compra Combustível", "Em Trânsito", "Estoque" |
| Ordem de exibição | Replica a ordem das linhas na tela |
| Ativo | Sim/Não |

Isso dá à empresa uma **tela de configuração** (dentro de Cadastros) para dizer quais contas bancárias e quais indicadores de recebíveis aparecem na tela de Saldos — os nomes configurados ali é que populam automaticamente as linhas do lado esquerdo, tanto do bloco de contas bancárias quanto do bloco de estoques/recebíveis, e a variação dia a dia é calculada para qualquer indicador cadastrado, sem precisar alterar código.

---

## 3. Tela "Saldos" (tela principal do novo item de menu)

### 3.1 Seleção de período
Comportamento sugerido:
- Padrão ao abrir: **data de ontem** e **data de anteontem**, com a variação entre elas já calculada.
- Alternativa 1: selecionar **do dia 1 (ou data de início de competência) até uma data escolhida** — mostra a evolução acumulada do mês.
- Alternativa 2: **intervalo livre** (data inicial / data final), qualquer que seja.
- A tela **não precisa (e não deve, por performance)** carregar o mês inteiro por padrão — carrega sob demanda conforme o filtro.

### 3.2 Estrutura de cada "bloco de dia"
Cada dia é renderizado como um bloco com Filiais nas colunas e as linhas abaixo:

**Cabeçalho:** Data | Filial 1 | Filial 2 | ... | Filial N | SALDOS (total da linha)
Repetido duas vezes, lado a lado: bloco esquerdo = **BANCOS** (saldo real no banco), bloco direito = **WEB** (saldo no sistema) + coluna extra **DIFERENÇA** = Saldo Banco − Saldo Sistema, por linha/conta.

Essa diferença é exatamente a conciliação: quando bate, fica 0.

**Bloco 1 — Contas Bancárias**
- Uma linha por conta bancária cadastrada (Banco do Brasil, Itaú, Sicoob, Bradesco, Saldo Conta/WEB, ...)
- SUB-TOTAL

**Bloco 2 — Estoques e Recebíveis**
- Cartões de Crédito
- Fiado
- Cheque
- Compra Combustível
- Em Trânsito
- Estoque R$ (preço de custo)
- SUB-TOTAL

**TOTAL** = Sub-total Bloco 1 + Sub-total Bloco 2 (calculado tanto no lado Bancos quanto no lado WEB, com diferença também no total)

### 3.3 Bloco de Variações (dia atual vs. dia anterior)
Por filial, para cada linha relevante:
- Contas (variação do saldo bancário total)
- Cartões de Crédito
- Fiado
- Cheque
- Estoques + Compras + Trânsito (somado)
- **Total** (soma das variações acima)

Fórmula: `variação[linha][filial] = valor(dia atual)[linha][filial] − valor(dia anterior)[linha][filial]`

### 3.4 Bloco "Valores Informados"
Lançamentos manuais/complementares por filial e por dia:
- Perdas (+) / Sobras (−)
- Extras (aportes/aplicações)
- Empréstimos (+) / Devoluções (−)
- Despesas
- **Variação Final** = Total (do bloco Variações) + Perdas/Sobras + Extras + Empréstimos/Devoluções − Despesas *(confirmar sinal exato de cada componente com base na planilha — nos exemplos a Variação Final ficou próxima da soma direta desses itens)*

---

## 4. Regra de Competência / Fechamento de Mês

Ponto central do seu relato — modelado assim:

- Existe um **cadastro de "Data de Corte de Competência"** por mês (ex.: pode ser dia 26, 27, 28 do mês anterior, dependendo de cair ou não em fim de semana bancário).
- Essa data marcada é o **último saldo do mês anterior** — nas planilhas aparece destacada na aba EDITOR com a nota "Último dia válido do mês passado".
- O saldo dessa data se torna automaticamente o **saldo inicial (dia 1) da competência seguinte** — não é uma nova apuração, é o mesmo valor carregado.
- Na tela de seleção de datas, essa data de corte deve aparecer sinalizada (ex.: tag/ícone "Início do mês") para o usuário saber que ali começa a competência.
- Sugestão de cadastro: tabela `competencia_mes` com campos `área`, `mês/ano`, `data_corte_inicio`. Isso permite configurar mês a mês (e área a área, se necessário) sem depender de regra fixa de calendário.

---

## 5. Modelo de dados sugerido (independente de stack)

```
areas
 ├─ id, nome, ordem

filiais (postos)
 ├─ id, area_id, nome, ordem

contas_bancarias
 ├─ id, area_id, banco, apelido, ordem, ativo

competencia_mes
 ├─ id, area_id, mes_ano, data_corte_inicio

saldos_diarios          -- Bloco Contas Bancárias, banco x sistema
 ├─ id, data, area_id, filial_id, conta_bancaria_id
 ├─ saldo_banco, saldo_sistema   -- diferença é calculada (saldo_banco - saldo_sistema)

saldos_recebiveis        -- Bloco Estoques/Recebíveis
 ├─ id, data, area_id, filial_id, tipo (cartao|fiado|cheque|compra_combustivel|em_transito|estoque)
 ├─ valor_banco (quando aplicável), valor_sistema

valores_informados        -- Bloco Valores Informados (lançamento manual)
 ├─ id, data, area_id, filial_id
 ├─ perdas_sobras, extras, emprestimos_devolucoes, despesas
```

Subtotais, totais, diferenças e variações **não precisam ser persistidos** — são calculados na consulta/tela a partir dessas tabelas, o que evita inconsistência quando um lançamento é corrigido depois.

---

## 6. Pontos confirmados

1. **Todas as áreas usam o mesmo conjunto de bancos** — não existe banco exclusivo de uma área. O cadastro de contas bancárias é por empresa, não por área; quando uma filial não usa um banco, o lançamento fica **zerado** em vez de a conta ser omitida. Ajustado no cadastro (seção 2.2).
2. **Fórmula da Variação Final** confirmada:
   `Variação Final = Total (bloco Variações) + Perdas(+)/Sobras(−) + Empréstimos(+)/Devoluções(−) − Despesas`
3. **Estoque**: é a mesma grandeza nos dois blocos (Bancos mostra "Estoque R$ (preço de custo)", WEB mostra só "Estoque") — mesmo valor, duas fontes/rótulos.
4. **Edição**: todos os campos da tela de Saldos são de **lançamento manual** (conciliação diária). Não há integração automática (OFX/API/ERP) nesta fase.
5. **Wireframe**: mockup da tela produzido, com:
   - Seletor de Área + chips de data rápida (Anteontem / Ontem / Desde início do mês)
   - Bloco "Contas bancárias" com **três modos de visualização selecionáveis**: Banco, Sistema, ou Lado a lado
     - Banco / Sistema: uma coluna por conta, editável, mais rápido para lançar
     - Lado a lado: cada filial exibe as duas colunas (banco / sistema) lado a lado, valor divergente destacado em vermelho — facilita a comparação visual célula a célula, mas nesse modo os campos ficam em modo leitura (a edição acontece nos modos Banco/Sistema)
   - Bloco "Estoques e recebíveis" com subtotal (mesmo padrão de alternância pode ser aplicado aqui depois, se fizer falta)
   - Linha de Total geral com Δ (diferença banco × sistema) — mantida sempre visível nos três modos; se com o uso ela se mostrar redundante com o modo "Lado a lado", dá para remover depois
   - Bloco "Variação (dia a dia)"
   - Bloco "Valores informados", 100% editável, com botão de salvar

## 7. Próximos passos sugeridos

1. ~~Desenhar as tabelas finais (DDL) e os endpoints da API~~ — feito nas seções 8 e 9 abaixo.
2. Definir o cadastro de `competencia_mes` (tela para marcar a data de corte de cada mês, por área) — DDL incluso.
3. Validar com o uso real se a linha de Total geral com Δ continua útil junto do modo "Lado a lado", ou se pode ser simplificada.

> **Assunção assumida nesta seção**: DDL em sintaxe PostgreSQL e API em estilo REST/JSON, por serem os padrões mais comuns. Se o Matrix já usa outro banco (MySQL/SQL Server) ou outro padrão de API (GraphQL, RPC), me avise e eu adapto.

---

## 8. DDL — Estrutura de tabelas (PostgreSQL / Supabase, encaixado no schema existente)

Confirmado com o cliente: **nenhuma tabela existente pode ser reaproveitada**, exceto as de consulta (`areas`, `filiais`, `areas_filiais`, `usuarios`, `empresas`). A tabela `lancamentos` é importada de outro sistema (fluxo de caixa) e não deve ser tocada.

Convenções seguidas (extraídas do schema real do Matrix):
- PK `id_<entidade>` inteiro com `GENERATED BY DEFAULT AS IDENTITY` (mesmo padrão de `financeiro_emprestimos`, `empresas`, `usuarios`)
- Multi-empresa: toda tabela carrega `cod_empresa character varying`
- Filial é referenciada por `(cod_empresa, cod_filial)` — é assim que `areas_filiais` já faz, **não** existe FK direta para um `filiais.id` interno
- Área é referenciada por `id_area integer` (FK direta para `areas.id_area`)
- Campos de status/tipo são `character varying` livre (não enum do Postgres), igual a `financeiro_emprestimos_parcelas.situacao`
- Auditoria com `criado_em` / `atualizado_em` (`timestamp without time zone`, default `now()`), e usuário responsável referenciando `usuarios.id_usuario`
- `ordem` como `integer default 10`, igual a `areas_filiais.ordem`

```sql
-- ============================================================
-- Garantia de integridade (idempotente — só cria se não existir)
-- Necessário para poder referenciar filiais por (cod_empresa, cod_filial)
-- ============================================================
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'filiais_cod_empresa_cod_filial_key'
  ) THEN
    ALTER TABLE filiais
      ADD CONSTRAINT filiais_cod_empresa_cod_filial_key UNIQUE (cod_empresa, cod_filial);
  END IF;
END $$;

-- ============================================================
-- CADASTROS NOVOS
-- ============================================================

-- Contas bancárias, por EMPRESA (não por área — todas as áreas compartilham o mesmo conjunto de bancos)
CREATE TABLE contas_bancarias (
    id_conta_bancaria  integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    cod_empresa        character varying NOT NULL,
    banco              character varying NOT NULL,   -- 'Banco do Brasil', 'Itaú', 'Bradesco', 'Saldo Conta/WEB', ...
    apelido            character varying,             -- opcional, se houver 2 contas do mesmo banco
    ordem              integer DEFAULT 10,
    ativo              boolean NOT NULL DEFAULT true,
    criado_em          timestamp without time zone DEFAULT now(),
    atualizado_em      timestamp without time zone DEFAULT now(),
    UNIQUE (cod_empresa, banco, apelido)
);

-- Data de corte de competência (fechamento de mês), por área
CREATE TABLE competencia_mes (
    id_competencia      integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    cod_empresa          character varying NOT NULL,
    id_area              integer NOT NULL REFERENCES areas (id_area),
    mes_ano               date NOT NULL,          -- normalizado para o dia 1 do mês, ex. 2026-06-01
    data_corte_inicio     date NOT NULL,          -- ex. 2026-05-29 (último dia válido do mês anterior)
    criado_em            timestamp without time zone DEFAULT now(),
    atualizado_em        timestamp without time zone DEFAULT now(),
    UNIQUE (cod_empresa, id_area, mes_ano)
);

-- ============================================================
-- LANÇAMENTOS DIÁRIOS (editáveis na tela de Saldos)
-- ============================================================

-- Bloco "Contas bancárias": saldo banco x saldo sistema, por conta e filial
CREATE TABLE saldos_bancarios (
    id_saldo_bancario   integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    cod_empresa         character varying NOT NULL,
    cod_filial          integer NOT NULL,
    data                date NOT NULL,
    id_conta_bancaria   integer NOT NULL REFERENCES contas_bancarias (id_conta_bancaria),
    saldo_banco         numeric NOT NULL DEFAULT 0,
    saldo_sistema       numeric NOT NULL DEFAULT 0,
    usuario_lancamento  integer REFERENCES usuarios (id_usuario),
    criado_em           timestamp without time zone DEFAULT now(),
    atualizado_em       timestamp without time zone DEFAULT now(),
    UNIQUE (cod_empresa, cod_filial, data, id_conta_bancaria),
    FOREIGN KEY (cod_empresa, cod_filial) REFERENCES filiais (cod_empresa, cod_filial)
);

-- Cadastro de indicadores do bloco "Estoques e recebíveis" (por EMPRESA, igual contas_bancarias)
CREATE TABLE indicadores_recebiveis (
    id_indicador_recebivel  integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    cod_empresa             character varying NOT NULL,
    nome                    character varying NOT NULL,   -- 'Cartões de Crédito', 'Fiado', 'Cheque', 'Compra Combustível', 'Em Trânsito', 'Estoque', ...
    ordem                   integer DEFAULT 10,
    ativo                   boolean NOT NULL DEFAULT true,
    criado_em               timestamp without time zone DEFAULT now(),
    atualizado_em           timestamp without time zone DEFAULT now(),
    UNIQUE (cod_empresa, nome)
);

-- Bloco "Estoques e recebíveis" — referencia o cadastro acima, não um valor fixo
CREATE TABLE saldos_recebiveis (
    id_saldo_recebivel      integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    cod_empresa              character varying NOT NULL,
    cod_filial                integer NOT NULL,
    data                       date NOT NULL,
    id_indicador_recebivel    integer NOT NULL REFERENCES indicadores_recebiveis (id_indicador_recebivel),
    valor_banco               numeric NOT NULL DEFAULT 0,    -- lado "Bancos" da planilha (ex.: estoque a preço de custo)
    valor_sistema             numeric NOT NULL DEFAULT 0,    -- lado "WEB" da planilha
    usuario_lancamento        integer REFERENCES usuarios (id_usuario),
    criado_em                 timestamp without time zone DEFAULT now(),
    atualizado_em             timestamp without time zone DEFAULT now(),
    UNIQUE (cod_empresa, cod_filial, data, id_indicador_recebivel),
    FOREIGN KEY (cod_empresa, cod_filial) REFERENCES filiais (cod_empresa, cod_filial)
);

-- Bloco "Valores informados"
CREATE TABLE valores_informados (
    id_valor_informado       integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    cod_empresa              character varying NOT NULL,
    cod_filial               integer NOT NULL,
    data                     date NOT NULL,
    perdas_sobras            numeric NOT NULL DEFAULT 0,  -- perdas(+) / sobras(-)
    extras                   numeric NOT NULL DEFAULT 0,  -- aportes/aplicações
    emprestimos_devolucoes   numeric NOT NULL DEFAULT 0,  -- empréstimos(+) / devoluções(-)
    despesas                 numeric NOT NULL DEFAULT 0,
    usuario_lancamento       integer REFERENCES usuarios (id_usuario),
    criado_em                timestamp without time zone DEFAULT now(),
    atualizado_em            timestamp without time zone DEFAULT now(),
    UNIQUE (cod_empresa, cod_filial, data),
    FOREIGN KEY (cod_empresa, cod_filial) REFERENCES filiais (cod_empresa, cod_filial)
);

-- Índices de apoio (consulta por período é a query mais comum da tela)
CREATE INDEX idx_saldos_bancarios_data ON saldos_bancarios (cod_empresa, data);
CREATE INDEX idx_saldos_recebiveis_data ON saldos_recebiveis (cod_empresa, data);
CREATE INDEX idx_indicadores_recebiveis_empresa ON indicadores_recebiveis (cod_empresa);
CREATE INDEX idx_valores_informados_data ON valores_informados (cod_empresa, data);
```

**Por que subtotal/total/diferença/variação não viram coluna nem tabela**: são sempre derivados de `saldos_bancarios`, `saldos_recebiveis` e `valores_informados` na hora da consulta. Se um lançamento for corrigido depois, o cálculo já reflete a correção automaticamente.

**RLS (Row Level Security)**: confirmado que o Matrix **não usa Supabase Auth** (`usuarios` tem `senha_hash` próprio) — o controle de acesso é 100% custom, via `usuarios_empresas` (empresa), `usuarios_filiais` (filial) e `usuarios_permissoes` (ação/tela), checados pela API. Não existe `auth.uid()` pra basear policy de linha.

Padrão adotado (consistente com o resto do banco, que também não tem RLS por linha): habilitar RLS nas 5 tabelas novas só para **bloquear acesso direto** via chave anônima/pública do Supabase — toda a filtragem por empresa/área/filial/permissão continua sendo feita no código da API, que usa a **service role key**.

```sql
ALTER TABLE contas_bancarias      ENABLE ROW LEVEL SECURITY;
ALTER TABLE indicadores_recebiveis ENABLE ROW LEVEL SECURITY;
ALTER TABLE competencia_mes       ENABLE ROW LEVEL SECURITY;
ALTER TABLE saldos_bancarios      ENABLE ROW LEVEL SECURITY;
ALTER TABLE saldos_recebiveis     ENABLE ROW LEVEL SECURITY;
ALTER TABLE valores_informados    ENABLE ROW LEVEL SECURITY;

-- Mesma policy nas 6: só a service role (usada pela API) acessa; anon/authenticated ficam de fora.
CREATE POLICY service_role_only ON contas_bancarias      FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_only ON indicadores_recebiveis FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_only ON competencia_mes       FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_only ON saldos_bancarios      FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_only ON saldos_recebiveis     FOR ALL TO service_role USING (true) WITH CHECK (true);
CREATE POLICY service_role_only ON valores_informados    FOR ALL TO service_role USING (true) WITH CHECK (true);
```

> Se a API **não** usa a service role key hoje (ex.: usa a chave `anon`/`authenticated` com um JWT customizado), me avisa — nesse caso as policies precisam ler claims do JWT em vez de travar tudo pra `service_role`.

---

## 9. API — Endpoints sugeridos (REST/JSON)

### Cadastros (CRUD simples)
```
GET    /api/areas
GET    /api/filiais?id_area=
GET    /api/contas-bancarias?cod_empresa=          -- comum a todas as áreas da empresa
POST   /api/contas-bancarias
PUT    /api/contas-bancarias/{id_conta_bancaria}
DELETE /api/contas-bancarias/{id_conta_bancaria}     -- soft delete (ativo=false)

GET    /api/indicadores-recebiveis?cod_empresa=    -- idem, configurável por empresa
POST   /api/indicadores-recebiveis
PUT    /api/indicadores-recebiveis/{id_indicador_recebivel}
DELETE /api/indicadores-recebiveis/{id_indicador_recebivel}   -- soft delete (ativo=false)

GET    /api/competencias?id_area=&ano=
POST   /api/competencias                              -- cadastra a data de corte de um mês
```

### Consulta consolidada (o que a tela de Saldos consome)
```
GET /api/saldos?id_area=1&data_inicio=2026-06-29&data_fim=2026-07-01
```
Retorna já pronto para montar os blocos da tela — subtotal, total, diferença e variação calculados no backend:

```json
{
  "id_area": 1,
  "periodo": { "data_inicio": "2026-06-29", "data_fim": "2026-07-01" },
  "filiais": [
    { "cod_filial": 1, "nome_filial": "Bonito I" },
    { "cod_filial": 2, "nome_filial": "Bonito II" }
  ],
  "dias": [
    {
      "data": "2026-06-29",
      "inicio_competencia": false,
      "contas_bancarias": [
        {
          "id_conta_bancaria": 1,
          "banco": "Banco do Brasil",
          "saldo_banco": { "1": 104403.55, "2": 7109.83 },
          "saldo_sistema": { "1": 104403.55, "2": 7109.83 },
          "diferenca_total": 0
        }
      ],
      "subtotal_contas": { "banco": 1682271.08, "sistema": 1682271.08 },
      "recebiveis": [
        { "id_indicador_recebivel": 1, "nome": "Cartões de Crédito", "valor_banco": { "1": 156179.36 }, "valor_sistema": { "1": 156179.36 } }
      ],
      "subtotal_recebiveis": { "banco": 4204481.29, "sistema": 4204481.29 },
      "total": { "banco": 5886752.37, "sistema": 5886752.37, "diferenca": 0 },
      "variacao": {
        "contas": { "1": 6996.82 },
        "total": { "1": -940.92 }
      },
      "valores_informados": {
        "1": { "perdas_sobras": 296.07, "extras": 0, "emprestimos_devolucoes": 0, "despesas": 2770.28, "variacao_final": 2125.43 }
      }
    }
  ]
}
```

### Lançamento (edição manual — usado pelos botões "Salvar" da tela)
Upsert em lote, por dia/filial, para minimizar chamadas:

```
PUT /api/saldos/bancarios
Body: {
  "data": "2026-07-01",
  "cod_empresa": "001",
  "lancamentos": [
    { "cod_filial": 3, "id_conta_bancaria": 1, "saldo_banco": 47583.88, "saldo_sistema": 47583.88 }
  ]
}

PUT /api/saldos/recebiveis
Body: {
  "data": "2026-07-01",
  "cod_empresa": "001",
  "lancamentos": [
    { "cod_filial": 3, "id_indicador_recebivel": 2, "valor_banco": 205199.56, "valor_sistema": 205199.56 }
  ]
}

PUT /api/saldos/valores-informados
Body: {
  "data": "2026-07-01",
  "cod_empresa": "001",
  "lancamentos": [
    { "cod_filial": 3, "perdas_sobras": 1856.59, "extras": 0, "emprestimos_devolucoes": 0, "despesas": 6512.44 }
  ]
}
```

Todos os três endpoints fazem `INSERT ... ON CONFLICT (cod_empresa, cod_filial, data, ...) DO UPDATE`, aproveitando as constraints `UNIQUE` da seção 8 — não precisa de lógica separada para criar vs. atualizar.

---

## 10. Regra de cálculo da consulta consolidada (pseudo-SQL)

```sql
-- Diferença por conta/dia
saldo_banco - saldo_sistema AS diferenca

-- Subtotal contas bancárias, por dia/filial
SELECT cod_empresa, cod_filial, data,
       SUM(saldo_banco) AS subtotal_banco, SUM(saldo_sistema) AS subtotal_sistema
FROM saldos_bancarios
GROUP BY cod_empresa, cod_filial, data;

-- Variação dia a dia (LAG por filial/conta, ordenado por data) -- bloco Contas Bancárias
SELECT cod_empresa, cod_filial, data, id_conta_bancaria,
       saldo_banco - LAG(saldo_banco) OVER (
           PARTITION BY cod_empresa, cod_filial, id_conta_bancaria ORDER BY data
       ) AS variacao
FROM saldos_bancarios;

-- Variação dia a dia -- bloco Estoques/Recebíveis (mesmo princípio, qualquer indicador cadastrado)
SELECT cod_empresa, cod_filial, data, id_indicador_recebivel,
       valor_banco - LAG(valor_banco) OVER (
           PARTITION BY cod_empresa, cod_filial, id_indicador_recebivel ORDER BY data
       ) AS variacao
FROM saldos_recebiveis;

-- Variação final
variacao_final = variacao_total + perdas_sobras + emprestimos_devolucoes - despesas
```

> A variação nunca depende de uma lista fixa de linhas — ela é calculada automaticamente para **qualquer** conta bancária ou indicador de recebível que esteja cadastrado (`contas_bancarias` / `indicadores_recebiveis`), então cadastrar um novo banco ou indicador não exige alterar código, só o cadastro.

---

## 9b. Telas de cadastro (dentro de "Cadastros", no menu do Financeiro)

Mockup produzido com 3 abas, refletindo o modelo das seções 2.2/2.4/4:

- **Contas bancárias**: lista editável (ordem, banco, apelido, ativo), comum a todas as áreas da empresa
- **Indicadores de recebíveis**: mesma estrutura, define as linhas do bloco "Estoques e recebíveis"
- **Competência**: uma linha por área/mês, com a data de corte editável — é isso que alimenta o "início de competência" sinalizado na tela de Saldos (seção 4)

Todas as 3 usam `POST`/`PUT`/`DELETE (soft delete)` já definidos na seção 9.

---

## 10b. Carga de Junho/26 — status final

Confirmado com o cliente: o valor sem rótulo do Ibiara (29/05–21/06, planilha Carol) é **erro/apagamento na planilha original** — não representa lançamento real e foi **descartado** da carga. Com isso, `seed_saldos_junho26.sql` está fechado: 704 combinações filial×dia validadas contra o TOTAL de cada planilha, 100% batendo (as 2 exceções pontuais de fórmula desatualizada na própria planilha — Itaporanga 08/06 e Pocinhos I 21/06 — foram carregadas com o valor correto, soma dos itens, e não com o total exibido na célula).

---

## 11. Permissões — cadastro no catálogo (`permissoes_catalogo`)

Seguindo o padrão já usado no `FINANCEIRO` (`MENU_FLUXO_CAIXA` em 510, `CADASTRO_EMPRESTIMOS_FINANCIAMENTOS`/`CONSULTA_EMPRESTIMOS_FINANCIAMENTOS` em 610/620), encaixei "Saldos" no vão livre entre `MENU` (500) e `MENU_FLUXO_CAIXA` (510), já que ele deve aparecer **antes** no menu:

```sql
INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo) VALUES
    ('FINANCEIRO', 'MENU_SALDOS',               'Menu de Saldos',              505, true),
    ('FINANCEIRO', 'CONSULTA_SALDOS',            'Consultar Saldos',           506, true),
    ('FINANCEIRO', 'LANCAMENTO_SALDOS',          'Lançar/Editar Saldos',       507, true),
    ('FINANCEIRO', 'CADASTRO_CONTAS_BANCARIAS',  'Cadastrar Contas Bancárias', 508, true);
```

- `MENU_SALDOS`: exibe o item "Saldos" no menu do Financeiro
- `CONSULTA_SALDOS`: acessa a tela em modo leitura (o "pode visualizar" que você mencionou)
- `LANCAMENTO_SALDOS`: libera a edição dos campos (bancos, recebíveis, valores informados) — o "pode selecionar/editar"
- `CADASTRO_CONTAS_BANCARIAS`: libera o cadastro de contas bancárias (separado, porque é uma ação mais sensível que só lançar saldo do dia)

A API, ao receber uma requisição da tela de Saldos, checa `usuarios_permissoes` (existe linha ativa pra `sistema='FINANCEIRO'` e `opcao` correspondente) — mesmo modelo que deve estar checando hoje pra `CADASTRO_EMPRESTIMOS_FINANCIAMENTOS`.

---

## 11. Pendências para a próxima etapa

1. Rodar a query de constraints (seção anterior) e confirmar se `filiais(cod_empresa, cod_filial)` já é única — o bloco `DO $$ ... $$` da seção 8 cobre os dois cenários, mas é bom confirmar visualmente.
2. Levantar `usuarios_empresas` e `usuarios_filiais` para desenhar as policies de RLS das 5 tabelas novas.
3. Definir o valor de `cod_empresa` que a Área 1 (Carol) e a Área 2 (Thalyta) usam hoje — isso entra direto nos `INSERT` de carga inicial das planilhas.
