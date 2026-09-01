"""Remove as linhas-fantasma de antecipacao_dividendos.

Cada prefixo digitado na observacao virava uma linha nova no banco
("Baixa", "Baixa Fiado", "Baixa Fiado ore"...), todas com o mesmo valor.
O bug foi corrigido em 01/09/2026; isto limpa o que ele deixou.

Roda com --aplicar; sem a flag apenas mostra o que seria removido.
"""
import sys

from db import get_connection

ALVOS = [
    ('2026-08-31', ['', 'Baixa', 'Baixa Fiado', 'Baixa Fiado ore']),
    ('2026-07-31', ['', 'Baixa', 'Baixa ep', 'Baixa esp', 'Baixa fiado']),
]

aplicar = '--aplicar' in sys.argv
conn = get_connection()
cur = conn.cursor()

for data, obs in ALVOS:
    cur.execute("""SELECT observacao, count(*), sum(valor)
                     FROM antecipacao_dividendos
                    WHERE cod_empresa = 'EMP010' AND data = %s AND observacao = ANY(%s)
                    GROUP BY 1 ORDER BY 1""", (data, obs))
    for linha in cur.fetchall():
        print(f'{data}  {linha[0]!r:28} {linha[1]:3} linhas  R$ {linha[2]}')

    if aplicar:
        cur.execute("""DELETE FROM antecipacao_dividendos
                        WHERE cod_empresa = 'EMP010' AND data = %s AND observacao = ANY(%s)""",
                    (data, obs))
        print(f'{data}  -> {cur.rowcount} linhas removidas')

if aplicar:
    conn.commit()

print('\nComo ficou:')
cur.execute("""SELECT data, observacao, count(*), sum(valor)
                 FROM antecipacao_dividendos
                WHERE cod_empresa = 'EMP010' AND data IN ('2026-07-31', '2026-08-31')
                GROUP BY 1, 2 ORDER BY 1, 2""")
for linha in cur.fetchall():
    print(f'{linha[0]}  {linha[1]!r:28} {linha[2]:3} linhas  R$ {linha[3]}')
