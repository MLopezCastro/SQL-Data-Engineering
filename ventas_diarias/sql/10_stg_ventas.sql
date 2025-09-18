CREATE OR ALTER VIEW dbo.stg_ventas AS
SELECT
  v.id                          AS venta_id,
  v.cliente_id,
  CAST(v.fecha_venta AS date)   AS fecha,
  LOWER(LTRIM(RTRIM(v.producto))) AS producto,
  NULLIF(v.cantidad,0)          AS cantidad,
  v.precio_unitario,
  CAST(ISNULL(v.cantidad,0)*ISNULL(v.precio_unitario,0) AS DECIMAL(14,2)) AS monto
FROM dbo.raw_ventas v;
