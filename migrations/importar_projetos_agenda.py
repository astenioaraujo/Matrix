"""
Copia para os Projetos da Agenda (Canivete Suíço) o que já está cadastrado
no módulo Projetos de cada empresa:

  METAS                ← projetos_mc_itens   (Melhorias Contínuas)
  RECORRÊNCIAS MENSAIS ← projetos_rec_modelos (cadastro) e
                          projetos_rec_execucoes do mês (situação do mês)
  TAREFAS EVENTUAIS    ← nada (fica em branco)

Nada é apagado na origem — é cópia. O script é idempotente: compara pelo
texto, então rodar de novo não duplica nada, só traz o que falta.

Uso:
    python3 migrations/importar_projetos_agenda.py            # aplica
    python3 migrations/importar_projetos_agenda.py --simular  # só mostra
"""
import sys
from datetime import date

sys.path.insert(0, ".")

from psycopg2.extras import RealDictCursor          # noqa: E402
from db import get_connection                       # noqa: E402

# projeto da agenda (pelo nome) → empresa de origem
EMPRESAS = {
    "lucena":   "EMP010",
    "vilela":   "EMP011",
    "30 cz":    "EMP012",
    "o closet": "EMP003",
    "inovai":   "EMP001",
}


def _norm(txt):
    return " ".join((txt or "").split()).strip()


def importar(simular=False):
    comp = date.today().replace(day=1)
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT id, id_usuario, nome FROM agenda_projetos WHERE ativo ORDER BY ordem, id")
    projetos = cur.fetchall()

    for p in projetos:
        empresa = EMPRESAS.get(_norm(p["nome"]).lower())
        if not empresa:
            print(f"— {p['nome']}: sem empresa mapeada, pulando")
            continue

        # ── METAS ← melhorias contínuas ──────────────────────────────
        cur.execute("""
            SELECT i.meta, i.status, i.ordem
            FROM projetos_mc_itens i
            JOIN projetos_mc_pastas pa ON pa.id = i.id_pasta AND pa.ativo
            WHERE i.cod_empresa=%s
            ORDER BY pa.ordem, i.ordem, i.id
        """, (empresa,))
        melhorias = cur.fetchall()

        cur.execute("""
            SELECT texto FROM agenda_proj_tarefas
            WHERE id_projeto=%s AND bloco='meta'
        """, (p["id"],))
        ja_tem = {_norm(r["texto"]).lower() for r in cur.fetchall()}

        novas_metas = 0
        for m in melhorias:
            texto = _norm(m["meta"])
            if not texto or texto.lower() in ja_tem:
                continue
            ja_tem.add(texto.lower())
            novas_metas += 1
            if simular:
                continue
            cur.execute("""
                INSERT INTO agenda_proj_tarefas (id_projeto, bloco, texto, concluido, ordem)
                VALUES (%s,'meta',%s,%s,
                        COALESCE((SELECT MAX(ordem)+1 FROM agenda_proj_tarefas
                                  WHERE id_projeto=%s AND bloco='meta'), 10))
            """, (p["id"], texto, (m["status"] or "").upper() == "CONCLUIDO", p["id"]))

        # ── RECORRÊNCIAS ← modelos cadastrados ───────────────────────
        cur.execute("""
            SELECT m.descricao, m.ordem
            FROM projetos_rec_modelos m
            JOIN projetos_rec_pastas pa ON pa.id = m.id_pasta AND pa.ativo
            WHERE m.cod_empresa=%s AND m.ativo
            ORDER BY pa.ordem, m.ordem, m.id
        """, (empresa,))
        modelos = [_norm(r["descricao"]) for r in cur.fetchall() if _norm(r["descricao"])]

        # ── situação do mês ← execuções do mês corrente ──────────────
        cur.execute("""
            SELECT descricao, concluido, ordem
            FROM projetos_rec_execucoes
            WHERE cod_empresa=%s AND ano=%s AND mes=%s
            ORDER BY ordem, id
        """, (empresa, comp.year, comp.month))
        execucoes = [(_norm(r["descricao"]), r["concluido"])
                     for r in cur.fetchall() if _norm(r["descricao"])]

        # o mês manda: o que está na lista do mês entra mesmo que não haja
        # modelo cadastrado; o resto dos modelos entra como pendente
        textos = list(dict.fromkeys([t for t, _ in execucoes] + modelos))
        situacoes = {t: c for t, c in execucoes}
        no_mes = {t for t, _ in execucoes} or set(modelos)

        cur.execute("""
            SELECT id, texto FROM agenda_proj_recorrencias
            WHERE id_projeto=%s AND ativo
        """, (p["id"],))
        existentes = {_norm(r["texto"]).lower(): r["id"] for r in cur.fetchall()}

        novas_recs = novas_execs = 0
        for ordem, texto in enumerate(textos, start=1):
            if texto in no_mes:
                novas_execs += 1

            id_rec = existentes.get(texto.lower())
            if not id_rec:
                novas_recs += 1
                if simular:
                    continue
                cur.execute("""
                    INSERT INTO agenda_proj_recorrencias (id_projeto, texto, ordem)
                    VALUES (%s,%s,%s) RETURNING id
                """, (p["id"], texto, ordem * 10))
                id_rec = cur.fetchone()["id"]
                existentes[texto.lower()] = id_rec

            if texto not in no_mes or simular:
                continue
            cur.execute("""
                INSERT INTO agenda_proj_rec_execucoes (id_recorrencia, competencia, situacao)
                VALUES (%s,%s,%s)
                ON CONFLICT (id_recorrencia, competencia) DO NOTHING
            """, (id_rec, comp, "concluida" if situacoes.get(texto) else "pendente"))

        if not simular:
            cur.execute("UPDATE agenda_projetos SET competencia=%s WHERE id=%s",
                        (comp, p["id"]))

        print(f"— {p['nome']} ({empresa}): {novas_metas} metas, "
              f"{novas_recs} recorrências, {novas_execs} no mês {comp:%m/%Y}")

    if simular:
        conn.rollback()
        print("\n(simulação — nada foi gravado)")
    else:
        conn.commit()
        print("\nimportação concluída")
    cur.close()
    conn.close()


if __name__ == "__main__":
    importar(simular="--simular" in sys.argv)
