-- Compras 100% descarregadas que ficaram com status ABERTA (30/10/2026).
--
-- A exclusão de descarrego reabria a compra sem olhar o saldo: excluir um
-- descarrego repetido deixava "Aberta" uma compra já toda descarregada (caso
-- da 2858, LO, Gasolina Aditivada de 23/10/2026 — o descarrego 2974 de
-- 3.000 L, datado de 26/10, continua lá). O código passou a recalcular o
-- status pelo saldo (recalcular_status_compra, em operacoes_routes.py).
--
-- Só EMP012, por decisão do cliente: 1925, 1975 e 2858. A EMP010 tem 36
-- compras na mesma situação, deixadas como estão.

UPDATE compras_combustiveis cc
SET status = 'FECHADA',
    atualizado_em = NOW()
WHERE cc.cod_empresa = 'EMP012'
  AND cc.status = 'ABERTA'
  AND cc.quantidade_comprada - COALESCE((
        SELECT SUM(d.quantidade_descarregada)
        FROM descarregos_combustiveis d
        WHERE d.cod_empresa = cc.cod_empresa
          AND d.id_compra = cc.id_compra
      ), 0) <= 0;
