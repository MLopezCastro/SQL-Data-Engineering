-- USE TuBase;
SET NOCOUNT ON;

------------------------------------------------------------
-- 1) Tablas necesarias (crea si no existen)
------------------------------------------------------------
IF OBJECT_ID('dbo.raw_ventas') IS NULL
BEGIN
  CREATE TABLE dbo.raw_ventas(
    id INT PRIMARY KEY,
    cliente_id INT,
    fecha_venta DATETIME2,
    producto NVARCHAR(100),
    cantidad INT,
    precio_unitario DECIMAL(12,2)
  );

  INSERT INTO dbo.raw_ventas(id,cliente_id,fecha_venta,producto,cantidad,precio_unitario) VALUES
  (1001,1,'2024-02-01 10:05:00','teclado',1,50.00),
  (1002,1,'2024-02-15 09:10:00','mouse',  2,30.00),
  (1003,2,'2024-02-20 13:00:00','monitor',1,200.00);
END

IF OBJECT_ID('dbo.ventas_diarias') IS NULL
  CREATE TABLE dbo.ventas_diarias(
    fecha           DATE          NOT NULL PRIMARY KEY,
    total_monto     DECIMAL(18,2) NOT NULL,
    cantidad_ventas INT           NOT NULL,
    last_loaded_at  DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
  );

IF OBJECT_ID('dbo.etl_control') IS NULL
  CREATE TABLE dbo.etl_control(
    pipeline         SYSNAME   NOT NULL PRIMARY KEY,
    last_loaded_date DATE      NULL,
    updated_at       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
  );

------------------------------------------------------------
-- 2) FULL REFRESH (DELETE + INSERT)  ← 1ra carga completa
------------------------------------------------------------
BEGIN TRY
  BEGIN TRAN;

  IF OBJECT_ID('tempdb..#agg_full') IS NOT NULL DROP TABLE #agg_full;
  SELECT
    CAST(fecha_venta AS date) AS fecha,
    SUM(ISNULL(cantidad,0)*ISNULL(precio_unitario,0)) AS total_monto,
    COUNT(*) AS cantidad_ventas
  INTO #agg_full
  FROM dbo.raw_ventas
  GROUP BY CAST(fecha_venta AS date);

  DELETE FROM dbo.ventas_diarias;

  INSERT INTO dbo.ventas_diarias(fecha,total_monto,cantidad_ventas,last_loaded_at)
  SELECT fecha,total_monto,cantidad_ventas,SYSUTCDATETIME()
  FROM #agg_full;

  DECLARE @last_full DATE = (SELECT MAX(fecha) FROM #agg_full);
  IF EXISTS (SELECT 1 FROM dbo.etl_control WHERE pipeline='ventas_diarias')
    UPDATE dbo.etl_control SET last_loaded_date=@last_full, updated_at=SYSUTCDATETIME() WHERE pipeline='ventas_diarias';
  ELSE
    INSERT INTO dbo.etl_control(pipeline,last_loaded_date,updated_at) VALUES('ventas_diarias',@last_full,SYSUTCDATETIME());

  COMMIT TRAN;
END TRY
BEGIN CATCH
  IF @@TRANCOUNT>0 ROLLBACK TRAN;
  THROW;
END CATCH;

------------------------------------------------------------
-- 3) SIMULAR NUEVO DATO (hoy)
------------------------------------------------------------
DECLARE @new_id INT = (SELECT ISNULL(MAX(id),0)+1 FROM dbo.raw_ventas);
INSERT INTO dbo.raw_ventas(id,cliente_id,fecha_venta,producto,cantidad,precio_unitario)
VALUES(@new_id,1,CAST(GETDATE() AS DATETIME2),'teclado',1,55.00);

------------------------------------------------------------
-- 4) INCREMENTAL (watermark + solape 1 día)
------------------------------------------------------------
BEGIN TRY
  BEGIN TRAN;

  DECLARE @wm DATE = (SELECT last_loaded_date FROM dbo.etl_control WHERE pipeline='ventas_diarias');
  IF @wm IS NULL SET @wm = '19000101';
  DECLARE @from DATE = DATEADD(DAY,-1,@wm);   -- solape

  IF OBJECT_ID('tempdb..#agg_inc') IS NOT NULL DROP TABLE #agg_inc;
  SELECT
    CAST(fecha_venta AS date) AS fecha,
    SUM(ISNULL(cantidad,0)*ISNULL(precio_unitario,0)) AS total_monto,
    COUNT(*) AS cantidad_ventas
  INTO #agg_inc
  FROM dbo.raw_ventas
  WHERE CAST(fecha_venta AS date) >= @from
  GROUP BY CAST(fecha_venta AS date);

  MERGE dbo.ventas_diarias AS d
  USING #agg_inc AS s
  ON d.fecha = s.fecha
  WHEN MATCHED THEN
    UPDATE SET d.total_monto=s.total_monto,
               d.cantidad_ventas=s.cantidad_ventas,
               d.last_loaded_at=SYSUTCDATETIME()
  WHEN NOT MATCHED THEN
    INSERT (fecha,total_monto,cantidad_ventas,last_loaded_at)
    VALUES (s.fecha,s.total_monto,s.cantidad_ventas,SYSUTCDATETIME());

  DECLARE @last_inc DATE = (SELECT MAX(fecha) FROM #agg_inc);
  IF @last_inc IS NOT NULL
    UPDATE dbo.etl_control
      SET last_loaded_date=@last_inc, updated_at=SYSUTCDATETIME()
      WHERE pipeline='ventas_diarias';

  COMMIT TRAN;
END TRY
BEGIN CATCH
  IF @@TRANCOUNT>0 ROLLBACK TRAN;
  THROW;
END CATCH;

------------------------------------------------------------
-- 5) RESULTADOS
------------------------------------------------------------
SELECT TOP (20) * FROM dbo.ventas_diarias ORDER BY fecha DESC;       -- debe incluir HOY
SELECT * FROM dbo.etl_control WHERE pipeline='ventas_diarias';
