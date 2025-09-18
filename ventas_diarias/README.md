# Proyecto: ventas_diarias

Pipeline que construye la tabla **dbo.ventas_diarias** con dos modos:
- **Full refresh** (TRUNCATE + INSERT)
- **Incremental** por `fecha` con watermark y solape de 1 día

## Orden de ejecución (primera vez)
1) `sql/00_tables.sql`
2) `sql/10_stg_ventas.sql` (si ya tenés stg_ventas, salteá)
3) `sql/20_sp_full_refresh.sql` → `EXEC dbo.sp_load_ventas_diarias_full;`
4) `sql/40_simular_nueva_venta.sql` (inserta una venta de HOY)
5) `sql/30_sp_incremental.sql` → `EXEC dbo.sp_load_ventas_diarias_incremental;`
6) `sql/50_validacion.sql`

## Runner (opcional)
`pipelines/run_ventas_diarias.py` permite `python run_ventas_diarias.py full` o `incremental` usando variables de entorno (.env).

