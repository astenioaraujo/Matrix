import os
import re
from pathlib import Path

PASTA = Path.home() / "Downloads"

FILIAIS = {
    1: "BONITO I",
    2: "BONITO II",
    3: "ITAPORANGA",
    4: "SERRA GRANDE",
    5: "CONCEICAO I",
    6: "CONCEICAO II",
    7: "COREMAS I",
    8: "COREMAS II",
    9: "S J PIRANHAS",
    10: "S J BONFIM",
    11: "OLIVEDOS",
    12: "POCINHOS I",
    13: "POCINHOS II",
    14: "AFOGADOS I",
    15: "AFOGADOS II",
    16: "S J EGITO",
    17: "MILAGRES",
    18: "MAURITI",
    19: "IBIARA",
    20: "SANTANA",
    21: "PATOS",
    22: "IPAUMIRIM",
}


def normalizar(txt: str) -> str:
    txt = txt.upper().strip()
    txt = txt.replace("_", " ")
    txt = re.sub(r"\s+", " ", txt)
    return txt


def ja_tem_codigo(nome: str) -> bool:
    return re.match(r"^\d+\s*-\s*", nome) is not None


def detectar_codigo(nome_arquivo: str):
    nome_base = Path(nome_arquivo).stem
    nome_norm = normalizar(nome_base)

    # Ordena do maior nome para o menor, para evitar casar "BONITO I" antes de "BONITO II"
    itens = sorted(FILIAIS.items(), key=lambda x: len(x[1]), reverse=True)

    for codigo, nome_filial in itens:
        filial_norm = normalizar(nome_filial)
        if nome_norm.startswith(filial_norm + " -") or nome_norm == filial_norm:
            return codigo, nome_filial

    return None, None


def main():
    if not PASTA.exists():
        print(f"Pasta não encontrada: {PASTA}")
        return

    arquivos = sorted(
        [p for p in PASTA.iterdir() if p.is_file() and p.suffix.lower() in (".xlsx", ".xls")]
    )

    if not arquivos:
        print("Nenhum arquivo Excel encontrado.")
        return

    print(f"Pasta: {PASTA}")
    print(f"Arquivos encontrados: {len(arquivos)}\n")

    a_renomear = []
    nao_identificados = []
    ja_ok = []

    for arq in arquivos:
        nome = arq.name

        if ja_tem_codigo(nome):
            ja_ok.append(nome)
            continue

        codigo, filial = detectar_codigo(nome)
        if codigo is None:
            nao_identificados.append(nome)
            continue

        novo_nome = f"{codigo} - {nome}"
        destino = arq.with_name(novo_nome)
        a_renomear.append((arq, destino, codigo, filial))

    print("PRÉVIA:\n")
    for origem, destino, codigo, filial in a_renomear:
        print(f"[OK] {origem.name}  -->  {destino.name}")

    for nome in nao_identificados:
        print(f"[??] Não identificado: {nome}")

    for nome in ja_ok:
        print(f"[--] Já renomeado: {nome}")

    print("\nResumo:")
    print(f"Prontos para renomear: {len(a_renomear)}")
    print(f"Não identificados: {len(nao_identificados)}")
    print(f"Já renomeados: {len(ja_ok)}")

    resp = input("\nDeseja renomear agora? (S/N): ").strip().upper()
    if resp != "S":
        print("Operação cancelada.")
        return

    conflitos = [destino.name for _, destino, _, _ in a_renomear if destino.exists()]
    if conflitos:
        print("\nConflitos encontrados. Estes nomes já existem:")
        for nome in conflitos:
            print(" -", nome)
        print("\nNada foi alterado.")
        return

    total = 0
    for origem, destino, _, _ in a_renomear:
        origem.rename(destino)
        total += 1

    print(f"\nConcluído. {total} arquivo(s) renomeado(s).")


if __name__ == "__main__":
    main()