#!/bin/bash
# Servidor de desenvolvimento do Matrix, com o log em arquivo em vez de preso
# ao terminal. O dev server do Flask escreve uma linha por requisição; se essa
# escrita bloquear (terminal com a saída pausada, janela fechada), o processo
# trava e o sistema parece "fora do ar" sem ter caído.
#
#   ./run_dev.sh start    sobe o servidor em segundo plano
#   ./run_dev.sh stop     derruba
#   ./run_dev.sh status   diz se está no ar e responde uma requisição de teste
#   ./run_dev.sh log      acompanha o log ao vivo
#   ./run_dev.sh restart  stop + start

set -u

RAIZ="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$RAIZ/logs"
LOG="$LOG_DIR/dev.log"
PIDFILE="$LOG_DIR/dev.pid"
PORTA=5001

pid_vivo() {
  [ -f "$PIDFILE" ] || return 1
  local pid
  pid="$(cat "$PIDFILE")"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

start() {
  if pid_vivo; then
    echo "Já está rodando (pid $(cat "$PIDFILE")). Use ./run_dev.sh restart"
    return 0
  fi
  mkdir -p "$LOG_DIR"
  # setsid não existe no macOS; o & já basta para soltar do terminal, e o
  # redirecionamento garante que nada bloqueie na escrita do stdout.
  cd "$RAIZ" || exit 1
  nohup python3 app.py >>"$LOG" 2>&1 &
  echo $! >"$PIDFILE"
  sleep 2
  if pid_vivo; then
    echo "No ar em http://localhost:$PORTA (pid $(cat "$PIDFILE"))"
    echo "Log: $LOG"
  else
    echo "Não subiu. Últimas linhas do log:"
    tail -20 "$LOG"
    return 1
  fi
}

stop() {
  if ! pid_vivo; then
    echo "Não está rodando."
    rm -f "$PIDFILE"
    # o reloader pode ter deixado processo órfão de uma execução anterior
    pkill -f "python3? app.py" 2>/dev/null && echo "Processos órfãos de app.py encerrados."
    return 0
  fi
  local pid
  pid="$(cat "$PIDFILE")"
  kill "$pid" 2>/dev/null
  sleep 1
  # o modo debug roda um processo pai (reloader) e um filho; o filho pode
  # sobreviver ao pai
  pkill -f "python3? app.py" 2>/dev/null
  rm -f "$PIDFILE"
  echo "Encerrado."
}

status() {
  if pid_vivo; then
    echo "Processo vivo (pid $(cat "$PIDFILE"))."
  else
    echo "Processo NÃO está rodando pelo script."
    pgrep -fl "app.py" || true
  fi
  echo -n "Resposta HTTP: "
  curl -s -o /dev/null -w "%{http_code} em %{time_total}s\n" \
    --max-time 10 "http://localhost:$PORTA/" || echo "sem resposta"
}

case "${1:-start}" in
  start)   start ;;
  stop)    stop ;;
  restart) stop; start ;;
  status)  status ;;
  log)     tail -f "$LOG" ;;
  *)       echo "uso: $0 {start|stop|restart|status|log}"; exit 1 ;;
esac
