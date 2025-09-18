-- Este test se corre tras insertar "hoy" y ejecutar el incremental:
SELECT COUNT(*) AS nuevas_fechas
FROM dbo.ventas_diarias
WHERE fecha = CAST(GETDATE() AS DATE);
