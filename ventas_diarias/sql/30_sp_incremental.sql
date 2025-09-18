CREATE OR ALTER PROCEDURE dbo.sp_load_ventas_diarias_incremental
AS
BEGIN
  SET NOCOUNT ON;
  DECLARE @pipeline SYSNAME = N'ventas_diarias';
  DECLARE @wm DATE = (SELECT last_loaded_date FROM dbo.etl_control WHERE pipeline = @pipeline);
  IF @wm IS NULL SET @wm = '19000101';

  DECLARE @overlap_days INT = 1;
  DECLARE @from DATE = DATEADD(DAY, -@overlap_days, @wm);

  BEGIN TRY
    BEGIN TRAN;

    ;WITH agg AS (
      SELECT sv.fecha, SUM(sv.monto) AS total_monto, COUNT(*) AS cantidad_ventas
      FROM dbo.stg_ventas AS sv
      WHERE sv.fecha >= @from
      GROUP BY sv.fecha
    )
    MERGE dbo.ventas_diarias AS d
    USING agg AS s
      ON d.fecha = s.fecha
    WHEN MATCHED THEN
      UPDATE SET d.total_monto = s.total_monto,
                 d.cantidad_ventas = s.cantidad_ventas,
                 d.last_loaded_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
      INSERT (fecha, total_monto, cantidad_ventas, last_loaded_at)
      VALUES (s.fecha, s.total_monto, s.cantidad_ventas, SYSUTCDATETIME());

    DECLARE @new_wm DATE = (SELECT MAX(fecha) FROM dbo.stg_ventas);
    MERGE dbo.etl_control AS t
    USING (SELECT @pipeline AS pipeline, @new_wm AS last_loaded_date) AS s
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
