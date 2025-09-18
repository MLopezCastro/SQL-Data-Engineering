INSERT INTO dbo.raw_ventas(id, cliente_id, fecha_venta, producto, cantidad, precio_unitario)
VALUES (999999, 1, CAST(GETDATE() AS DATETIME2), 'teclado', 1, 55.00);
