CREATE OR ALTER PROCEDURE dbo.sp_load_ventas_diarias_full
AS
BEGIN
  SET NOCOUNT ON;
  BEGIN TRY
    BEGIN TRAN;

    SELECT sv.fecha, SUM(sv.monto) AS total_monto, COUNT(*) AS cantidad_ventas
    INTO #new
    FROM dbo.stg_ventas AS sv
    GROUP BY sv.fecha;

    TRUNCATE TABLE dbo.ventas_diarias;

    INSERT INTO dbo.ventas_diarias (fecha, total_monto, cantidad_ventas, last_loaded_at)
    SELECT fecha, total_monto, cantidad_ventas, SYSUTCDATETIME()
    FROM #new;

    MERGE dbo.etl_control AS t
    USING (SELECT CAST('ventas_diarias' AS SYSNAME) AS pipeline,
                  MAX(fecha) AS last_loaded_date) AS s
      ON t.pipeline = s.pipeline
    WHEN MATCHED THEN UPDATE SET last_loaded_date = s.last_loaded_date, updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN INSERT (pipeline,last_loaded_date,updated_at) VALUES (s.pipeline,s.last_loaded_date,SYSUTCDATETIME());

    COMMIT TRAN;
  END TRY
  BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRAN;
    THROW;
  END CATCH
END;
GO
