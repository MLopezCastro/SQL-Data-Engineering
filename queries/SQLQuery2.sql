--¡Vamos con todo, Marce! Te dejo un paso a paso en SQL Server 2022 para armar la capa staging → intermedia → mart del caso 3.7. 
--Incluye tablas crudas, stg_clientes, stg_ventas, el modelo intermedio int_ingresos_mensuales (v1 y v2 como migración), y el mart mart_top_clientes. 
--Todo está listo para pegar en SSMS.

CREATE DATABASE DemoStaging;
GO
USE DemoStaging;
GO

--Tablas crudas:
-- RAW CLIENTES
USE DemoStaging; -- o la DB que estés usando

IF OBJECT_ID('dbo.raw_clientes') IS NULL
CREATE TABLE dbo.raw_clientes(
  id INT PRIMARY KEY,
  nombre NVARCHAR(100),
  fechaalta DATETIME2,
  canalorigen NVARCHAR(50)
);

IF OBJECT_ID('dbo.raw_ventas') IS NULL
CREATE TABLE dbo.raw_ventas(
  id INT PRIMARY KEY,
  cliente_id INT,
  fecha_venta DATETIME2,
  producto NVARCHAR(100),
  cantidad INT,
  precio_unitario DECIMAL(12,2)
);

/* Datos de ejemplo mínimos */
MERGE dbo.raw_clientes AS t USING (VALUES
(1,'ANA','2024-01-10 14:32:00','WEB'),
(2,'juan p.','2024-01-11 11:00:00','Referido')
) AS s(id,nombre,fechaalta,canalorigen)
ON t.id=s.id WHEN NOT MATCHED THEN
INSERT(id,nombre,fechaalta,canalorigen) VALUES(s.id,s.nombre,s.fechaalta,s.canalorigen);

MERGE dbo.raw_ventas AS t USING (VALUES
(1001,1,'2024-02-01 10:05:00','teclado',1,50.00),
(1002,1,'2024-02-15 09:10:00','mouse',  2,30.00),
(1003,2,'2024-02-20 13:00:00','monitor',1,200.00)
) AS s(id,cliente_id,fecha_venta,producto,cantidad,precio_unitario)
ON t.id=s.id WHEN NOT MATCHED THEN
INSERT(id,cliente_id,fecha_venta,producto,cantidad,precio_unitario)
VALUES(s.id,s.cliente_id,s.fecha_venta,s.producto,s.cantidad,s.precio_unitario);


--) stg_ventas (limpieza + tipificación)
SELECT * FROM raw_clientes;
SELECT * FROM raw_ventas;

CREATE OR ALTER VIEW dbo.stg_ventas AS
SELECT
  v.id                      AS venta_id,
  v.cliente_id,
  CAST(v.fecha_venta AS date)                AS fecha,
  LOWER(LTRIM(RTRIM(v.producto)))            AS producto,
  NULLIF(v.cantidad,0)                       AS cantidad,     -- evita 0 “válido”
  v.precio_unitario,
  CAST(ISNULL(v.cantidad,0)*ISNULL(v.precio_unitario,0) AS DECIMAL(14,2)) AS monto
FROM dbo.raw_ventas v;

SELECT * FROM raw_ventas;

--2) int_ingresos_mensuales v1 (total por mes por cliente)
CREATE OR ALTER VIEW dbo.int_ingresos_mensuales_v1 AS
SELECT
  sv.cliente_id,
  DATEFROMPARTS(YEAR(sv.fecha), MONTH(sv.fecha), 1) AS mes,
  SUM(sv.monto) AS total_ingresos
FROM dbo.stg_ventas sv
GROUP BY sv.cliente_id, DATEFROMPARTS(YEAR(sv.fecha), MONTH(sv.fecha), 1);

SELECT * FROM int_ingresos_mensuales_v1;

--3) Migración → int_ingresos_mensuales_v2 (agrego columnas nuevas)
CREATE OR ALTER VIEW dbo.int_ingresos_mensuales_v2 AS
WITH base AS (
  SELECT
    sv.cliente_id,
    DATEFROMPARTS(YEAR(sv.fecha), MONTH(sv.fecha), 1) AS mes,
    SUM(sv.monto)  AS total_ingresos,
    COUNT(*)       AS cantidad_ventas
  FROM dbo.stg_ventas sv
  GROUP BY sv.cliente_id, DATEFROMPARTS(YEAR(sv.fecha), MONTH(sv.fecha), 1)
)
SELECT
  b.cliente_id,
  b.mes,
  b.total_ingresos,
  b.cantidad_ventas,
  CASE WHEN b.cantidad_ventas=0 THEN NULL
       ELSE CAST(b.total_ingresos*1.0/b.cantidad_ventas AS DECIMAL(14,2))
  END AS ticket_promedio,
  SUM(b.total_ingresos) OVER (
    PARTITION BY b.cliente_id
    ORDER BY b.mes
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS acumulado_cliente
FROM base b;

SELECT * FROM int_ingresos_mensuales_v2;

--4) mart_top_clientes (top 10 últimos 90 días)
--Primero armamos stg_clientes rápido (para tener nombre normalizado) y luego el mart.

-- stg_clientes: normaliza nombre, fecha, canal
CREATE OR ALTER VIEW dbo.stg_clientes AS
SELECT
  rc.id,
  -- Title Case sencillo: primera mayúscula, resto minúscula por palabra
  STRING_AGG(CONCAT(UPPER(LEFT(w.value,1)), LOWER(SUBSTRING(w.value,2,LEN(w.value)))), ' ')
    WITHIN GROUP (ORDER BY w.ordinal) AS nombre,
  CAST(rc.fechaalta AS date) AS fecha_alta,
  LOWER(LTRIM(RTRIM(rc.canalorigen))) AS canal
FROM dbo.raw_clientes rc
CROSS APPLY STRING_SPLIT(LTRIM(RTRIM(ISNULL(rc.nombre,''))),' ',1) AS w
GROUP BY rc.id, rc.fechaalta, rc.canalorigen;
GO

-- mart: top 10 por facturación 90 días
CREATE OR ALTER VIEW dbo.mart_top_clientes AS
WITH ref AS (
  SELECT MAX(fecha) AS ref_date FROM dbo.stg_ventas
),
v90 AS (
  SELECT sv.*
  FROM dbo.stg_ventas sv
  CROSS JOIN ref
  WHERE sv.fecha >= DATEADD(DAY, -90, ref.ref_date)
),
tot AS (
  SELECT cliente_id, SUM(monto) AS total_90d
  FROM v90
  GROUP BY cliente_id
)
SELECT TOP (10)
  c.id        AS cliente_id,
  sc.nombre   AS nombre,
  t.total_90d AS total_ult_90_dias,
  DENSE_RANK() OVER (ORDER BY t.total_90d DESC) AS ranking_top
FROM tot t
JOIN dbo.stg_clientes sc ON sc.id = t.cliente_id
JOIN dbo.raw_clientes c  ON c.id  = t.cliente_id
ORDER BY ranking_top, cliente_id;



--Resultados:
SELECT * FROM dbo.stg_ventas;
SELECT * FROM dbo.int_ingresos_mensuales_v1 ORDER BY cliente_id, mes;
SELECT * FROM dbo.int_ingresos_mensuales_v2 ORDER BY cliente_id, mes;
SELECT * FROM dbo.mart_top_clientes;


SELECT COUNT(*) AS dentro_90d
FROM dbo.stg_ventas
WHERE fecha >= DATEADD(DAY, -90, CAST(GETDATE() AS date));
-- Si esto da 0, es exactamente eso.
