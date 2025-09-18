-- CREATE DATABASE DemoStaging; -- si la necesitás
-- USE DemoStaging;
-- GO

IF OBJECT_ID('dbo.ventas_diarias') IS NULL
CREATE TABLE dbo.ventas_diarias (
  fecha            DATE          NOT NULL PRIMARY KEY,
  total_monto      DECIMAL(18,2) NOT NULL,
  cantidad_ventas  INT           NOT NULL,
  last_loaded_at   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('dbo.etl_control') IS NULL
CREATE TABLE dbo.etl_control (
  pipeline         SYSNAME    NOT NULL PRIMARY KEY,
  last_loaded_date DATE       NULL,
  updated_at       DATETIME2  NOT NULL DEFAULT SYSUTCDATETIME()
);
