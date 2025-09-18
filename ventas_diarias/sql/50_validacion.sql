-- A) conteo antes/después
SELECT COUNT(*) AS filas_antes FROM dbo.ventas_diarias;

-- Ejecutá: EXEC dbo.sp_load_ventas_diarias_incremental;

SELECT COUNT(*) AS filas_despues FROM dbo.ventas_diarias;

-- B) solo se agregó la fecha de hoy
SELECT * FROM dbo.ventas_diarias WHERE fecha = CAST(GETDATE() AS DATE);

-- C) watermark
SELECT * FROM dbo.etl_control WHERE pipeline = N'ventas_diarias';
