timeout = 120

# Um worker sync atende UMA requisição por vez: qualquer tela pesada
# (Consulta de Estoques, Fluxo de Caixa Projetado, importação) deixava
# todo mundo na fila, e para quem esperava o sistema parecia fora do ar.
# Com gthread são 4 requisições simultâneas. As rotas são I/O de banco,
# que é justamente onde thread ajuda. São 4, e não 8, porque o scrypt do
# login usa ~32 MB por verificação de senha e a instância Starter tem
# 512 MB: 8 logins simultâneos arriscariam estourar a memória.
worker_class = "gthread"
threads = 4

# Continua UM worker de propósito. IMPORT_PROGRESS
# (routes/importacoes_routes.py) guarda o progresso da importação num
# dicionário em memória; com mais de um processo, a consulta do progresso
# cairia no worker que não tem o job e a barra quebraria. Threads dividem
# a mesma memória, processos não.
workers = 1
