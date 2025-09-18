SELECT CASE WHEN EXISTS (
  SELECT 1 FROM dbo.etl_control WHERE pipeline = N'ventas_diarias' AND last_loaded_date IS NOT NULL
) THEN 1 ELSE 0 END AS ok;
