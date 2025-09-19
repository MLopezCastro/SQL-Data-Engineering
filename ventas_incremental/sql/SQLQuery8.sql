/* ========================================
   one_click.sql — SQL Server (SSMS)
   Ejecutar UNA sola vez (F5)
   Construye pipeline incremental con watermark.
========================================= */

-- USE TuBase;           -- ← (opcional) descomentá y poné tu DB
SET NOCOUNT ON;

PRINT '=== 0) Crear tablas si no existen ===';

IF OBJECT_ID('dbo.ventas_crudas') IS NULL
BEGIN
  CREATE TABLE dbo.ventas_crudas (
    id          INT           NOT NULL PRIMARY KEY,
    cliente_id  INT           NOT NULL,
    producto_id INT           NOT NULL,
    fecha       DATE          NOT NULL,
    monto       DECIMAL(12,2) NOT NULL
  );
END;

IF OBJECT_ID('dbo.ventas_limpias') IS NULL
BEGIN
  CREATE TABLE dbo.ventas_limpias (
    id          INT           NOT NULL PRIMARY KEY,
    cliente_id  INT           NOT NULL,
    producto_id INT           NOT NULL,
    fecha       DATE          NOT NULL,
    monto       DECIMAL(12,2) NOT NULL
  );
END;

PRINT 'Tablas listas';


/* ===============================
   Helper: función incremental
   - calcula watermark (MAX(fecha) en ventas_limpias, o 1900-01-01 si vacío)
   - inserta SOLO filas nuevas (fecha > watermark)
   - loguea cuántas insertó
================================= */
PRINT '=== 1) Incremental inicial (si hay data previa) ===';
DECLARE @watermark DATE = (SELECT COALESCE(MAX(fecha),'19000101') FROM dbo.ventas_limpias);
PRINT CONVERT(VARCHAR(19),GETDATE(),120) + ' | INFO | Última fecha procesada: ' + CONVERT(VARCHAR(10),@watermark,23);

INSERT INTO dbo.ventas_limpias (id, cliente_id, producto_id, fecha, monto)
SELECT id, cliente_id, producto_id, fecha, monto
FROM dbo.ventas_crudas
WHERE fecha > @watermark;

PRINT CONVERT(VARCHAR(19),GETDATE(),120) + ' | INFO | Se insertaron ' + CAST(@@ROWCOUNT AS VARCHAR(20)) + ' filas nuevas';


/* ===============================
   2) Simular 3 días y correr incremental tras cada día
   - hoy-2, hoy-1, hoy
   - IDs dinámicos para no chocar PK si re-ejecutás
================================= */
DECLARE @hoy DATE = CAST(GETDATE() AS DATE);
DECLARE @base_id INT;

-- Día 1: HOY - 2
PRINT '=== 2) Simulación día 1 (hoy-2) + incremental ===';
SELECT @base_id = ISNULL(MAX(id),0) + 1 FROM dbo.ventas_crudas;
INSERT INTO dbo.ventas_crudas (id, cliente_id, producto_id, fecha, monto)
VALUES (@base_id+0, 1, 101, DATEADD(DAY,-2,@hoy), 120.00),
       (@base_id+1, 2, 103, DATEADD(DAY,-2,@hoy),  75.50),
       (@base_id+2, 1, 102, DATEADD(DAY,-2,@hoy),  43.20);

SET @watermark = (SELECT COALESCE(MAX(fecha),'19000101') FROM dbo.ventas_limpias);
PRINT CONVERT(VARCHAR(19),GETDATE(),120) + ' | INFO | Última fecha procesada: ' + CONVERT(VARCHAR(10),@watermark,23);

INSERT INTO dbo.ventas_limpias (id, cliente_id, producto_id, fecha, monto)
SELECT id, cliente_id, producto_id, fecha, monto
FROM dbo.ventas_crudas
WHERE fecha > @watermark;

PRINT CONVERT(VARCHAR(19),GETDATE(),120) + ' | INFO | Se insertaron ' + CAST(@@ROWCOUNT AS VARCHAR(20)) + ' filas nuevas';

-- Día 2: HOY - 1
PRINT '=== 3) Simulación día 2 (hoy-1) + incremental ===';
SELECT @base_id = ISNULL(MAX(id),0) + 1 FROM dbo.ventas_crudas;
INSERT INTO dbo.ventas_crudas (id, cliente_id, producto_id, fecha, monto)
VALUES (@base_id+0, 3, 101, DATEADD(DAY,-1,@hoy), 210.00),
       (@base_id+1, 2, 104, DATEADD(DAY,-1,@hoy),  30.00),
       (@base_id+2, 4, 102, DATEADD(DAY,-1,@hoy),  99.90);

SET @watermark = (SELECT COALESCE(MAX(fecha),'19000101') FROM dbo.ventas_limpias);
PRINT CONVERT(VARCHAR(19),GETDATE(),120) + ' | INFO | Última fecha procesada: ' + CONVERT(VARCHAR(10),@watermark,23);

INSERT INTO dbo.ventas_limpias (id, cliente_id, producto_id, fecha, monto)
SELECT id, cliente_id, producto_id, fecha, monto
FROM dbo.ventas_crudas
WHERE fecha > @watermark;

PRINT CONVERT(VARCHAR(19),GETDATE(),120) + ' | INFO | Se insertaron ' + CAST(@@ROWCOUNT AS VARCHAR(20)) + ' filas nuevas';

-- Día 3: HOY
PRINT '=== 4) Simulación día 3 (hoy) + incremental ===';
SELECT @base_id = ISNULL(MAX(id),0) + 1 FROM dbo.ventas_crudas;
INSERT INTO dbo.ventas_crudas (id, cliente_id, producto_id, fecha, monto)
VALUES (@base_id+0, 1, 101, @hoy,  15.00),
       (@base_id+1, 5, 105, @hoy,  65.75),
       (@base_id+2, 3, 103, @hoy, 180.00);

SET @watermark = (SELECT COALESCE(MAX(fecha),'19000101') FROM dbo.ventas_limpias);
PRINT CONVERT(VARCHAR(19),GETDATE(),120) + ' | INFO | Última fecha procesada: ' + CONVERT(VARCHAR(10),@watermark,23);

INSERT INTO dbo.ventas_limpias (id, cliente_id, producto_id, fecha, monto)
SELECT id, cliente_id, producto_id, fecha, monto
FROM dbo.ventas_crudas
WHERE fecha > @watermark;

PRINT CONVERT(VARCHAR(19),GETDATE(),120) + ' | INFO | Se insertaron ' + CAST(@@ROWCOUNT AS VARCHAR(20)) + ' filas nuevas';


/* ===============================
   5) Validación final
================================= */
PRINT '=== 5) Validación final ===';
SELECT COUNT(*) AS filas_crudas FROM dbo.ventas_crudas;
SELECT COUNT(*) AS filas_limpias FROM dbo.ventas_limpias;

SELECT MIN(fecha) AS min_fecha_limpias,
       MAX(fecha) AS max_fecha_limpias
FROM dbo.ventas_limpias;

SELECT fecha, COUNT(*) AS filas
FROM dbo.ventas_limpias
GROUP BY fecha
ORDER BY fecha DESC;
