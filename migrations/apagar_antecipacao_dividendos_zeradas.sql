-- Registros zerados de antecipação de dividendos (31/10/2026).
--
-- A tela antiga era uma planilha que gravava a linha inteira: toda filial em
-- branco virava um registro com valor zero. Eram 157 dos 188 registros da
-- EMP010. Zero não é antecipação, e a tela nova recusa zero — então não voltam.
-- Apagados a pedido do cliente.

DELETE FROM antecipacao_dividendos WHERE valor = 0;
