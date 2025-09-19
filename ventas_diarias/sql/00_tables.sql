-- (Opcional) USE TuBase;
-- GO

-- Tabla destino
IF OBJECT_ID('dbo.ventas_diarias') IS NULL
CREATE TABLE dbo.ventas_diarias (
  fecha            DATE          NOT NULL PRIMARY KEY,
  total_monto      DECIMAL(18,2) NOT NULL,
  cantidad_ventas  INT           NOT NULL,
  last_loaded_at   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

-- Tabla de control (watermark)
IF OBJECT_ID('dbo.etl_control') IS NULL
CREATE TABLE dbo.etl_control (
  pipeline         SYSNAME    NOT NULL PRIMARY KEY,
  last_loaded_date DATE       NULL,
  updated_at       DATETIME2  NOT NULL DEFAULT SYSUTCDATETIME()
);

-- Fuente RAW mínima (si no existe)
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
