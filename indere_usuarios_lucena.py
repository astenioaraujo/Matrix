from db import get_connection
from werkzeug.security import generate_password_hash

conn = get_connection()
cur = conn.cursor()

for i in range(1, 23):
    username = f"medicoes{str(i).zfill(2)}"
    nome = f"Medições {str(i).zfill(2)}"
    email = f"{username}@matrx.com"

    senha_hash = generate_password_hash("1234")

    # 🔹 cria usuário
    cur.execute("""
        INSERT INTO usuarios (nome, email, senha_hash, tipo_global)
        VALUES (%s, %s, %s, 'normal')
        RETURNING id_usuario
    """, (nome, email, senha_hash))

    id_usuario = cur.fetchone()[0]

    # 🔹 empresa
    cur.execute("""
        INSERT INTO usuarios_empresas (id_usuario, cod_empresa, perfil_empresa, ativo)
        VALUES (%s, 'EMP010', 'medicoes', TRUE)
    """, (id_usuario,))

    # 🔹 filial
    cod_filial = str(i).zfill(2)

    cur.execute("""
        INSERT INTO usuarios_filiais (id_usuario, cod_empresa, cod_filial, ativo)
        VALUES (%s, 'EMP010', %s, TRUE)
    """, (id_usuario, cod_filial))

    # 🔹 permissões
    cur.execute("""
        INSERT INTO usuarios_permissoes (id_usuario, cod_empresa, sistema, opcao, ativo)
        VALUES
        (%s, 'EMP010', 'OPERACOES', 'MENU', TRUE),
        (%s, 'EMP010', 'OPERACOES', 'INFORMAR_MEDICOES', TRUE)
    """, (id_usuario, id_usuario))

conn.commit()
cur.close()
conn.close()

print("Usuários criados com sucesso!")