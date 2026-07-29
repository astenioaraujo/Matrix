"""
Compliance — tabela + permissões no catálogo.

Rode assim que o banco estiver acessível:
    cd /Users/astenioaraujo/sistemas/matrx && python3 migrations/2026-07-28_compliance.py

É idempotente: pode rodar quantas vezes precisar.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection

PERMISSOES = [
    ("MENU",                  "Menu de Compliance",   100),
    ("COMPLIANCES",           "Cadastrar Compliance", 110),
    ("CONSULTAR_COMPLIANCES", "Consultar Compliance", 120),
]


def main():
    conn = get_connection()
    cur = conn.cursor()

    # ---- tabela (mesmo formato de rh_funcoes) ----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS compliances (
          id            SERIAL PRIMARY KEY,
          cod_empresa   VARCHAR NOT NULL,
          titulo        VARCHAR NOT NULL,
          descricao     TEXT    NOT NULL,
          ativo         BOOLEAN NOT NULL DEFAULT TRUE,
          criado_em     TIMESTAMP DEFAULT NOW(),
          atualizado_em TIMESTAMP DEFAULT NOW()
        )
    """)
    # RLS junto do CREATE TABLE: sem isso a tabela fica pública via chave anon
    cur.execute("ALTER TABLE public.compliances ENABLE ROW LEVEL SECURITY")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_compliances_empresa
        ON compliances (cod_empresa, ativo)
    """)
    print("tabela compliances: ok (RLS habilitado)")

    # ---- permissões ----
    for opcao, descricao, ordem in PERMISSOES:
        cur.execute("""
            INSERT INTO permissoes_catalogo (sistema, opcao, descricao, ordem, ativo)
            VALUES ('COMPLIANCE', %s, %s, %s, TRUE)
            ON CONFLICT (sistema, opcao)
            DO UPDATE SET descricao = EXCLUDED.descricao,
                          ordem     = EXCLUDED.ordem,
                          ativo     = TRUE
        """, (opcao, descricao, ordem))
        print(f"permissao: COMPLIANCE/{opcao:22} {descricao}")

    conn.commit()

    # ---- conferência ----
    cur.execute("SELECT relrowsecurity FROM pg_class WHERE relname='compliances'")
    print("\nRLS em compliances:", cur.fetchone()[0])
    cur.execute("""SELECT opcao, descricao, ordem FROM permissoes_catalogo
                   WHERE sistema='COMPLIANCE' ORDER BY ordem""")
    print("Catálogo COMPLIANCE:")
    for r in cur.fetchall():
        print(f"  {r[2]:>4}  {r[1]:24} ({r[0]})")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
