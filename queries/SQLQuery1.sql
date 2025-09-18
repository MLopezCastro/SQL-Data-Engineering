--Creo DataBase
CREATE DATABASE DemoSQLDE;
GO
USE DemoSQLDE;
GO

--Creo Tablas
-- Clientes
CREATE TABLE clientes (
    cliente_id INT PRIMARY KEY,
    nombre NVARCHAR(100)
);

-- Productos
CREATE TABLE productos (
    producto_id INT PRIMARY KEY,
    nombre NVARCHAR(100),
    precio_unitario DECIMAL(10,2)
);

-- Compras
CREATE TABLE compras (
    compra_id INT PRIMARY KEY,
    cliente_id INT FOREIGN KEY REFERENCES clientes(cliente_id),
    producto_id INT FOREIGN KEY REFERENCES productos(producto_id),
    fecha_compra DATE,
    cantidad INT
);

--Inserto Datos Ejemplo
-- Clientes
INSERT INTO clientes (cliente_id, nombre)
VALUES (1, 'Ana'), (2, 'Bruno'), (3, 'Carla');

-- Productos
INSERT INTO productos (producto_id, nombre, precio_unitario)
VALUES (101, 'Teclado', 50.00),
       (102, 'Mouse', 30.00),
       (103, 'Monitor', 200.00);

-- Compras
INSERT INTO compras (compra_id, cliente_id, producto_id, fecha_compra, cantidad)
VALUES 
(1001, 1, 101, '2025-07-15', 1),
(1002, 1, 102, '2025-08-01', 2),
(1003, 1, 103, '2025-09-05', 1),
(1004, 2, 103, '2025-08-20', 1),
(1005, 2, 101, '2025-09-10', 3),
(1006, 3, 102, '2025-09-12', 1);

--
SELECT * FROM clientes;
SELECT * FROM compras;
SELECT * FROM productos;

--Query del ejercicio:
WITH compras_60 AS (
    SELECT
        c.cliente_id,
        c.compra_id,
        c.fecha_compra,
        c.cantidad,
        p.precio_unitario,
        CAST(c.cantidad * p.precio_unitario AS DECIMAL(18,2)) AS importe_linea,

        SUM(CAST(c.cantidad * p.precio_unitario AS DECIMAL(18,2)))
            OVER (PARTITION BY c.cliente_id) AS total_60,

        MAX(c.fecha_compra) OVER (PARTITION BY c.cliente_id) AS ultima_compra_60d
    FROM compras c
    JOIN productos p ON p.producto_id = c.producto_id
    WHERE c.fecha_compra >= DATEADD(DAY, -60, GETDATE())
),
clientes_tot AS (
    SELECT DISTINCT
        cliente_id,
        total_60,
        ultima_compra_60d
    FROM compras_60
)
SELECT
    cl.cliente_id,
    cl.nombre,
    ct.total_60 AS total_gastado_ult_60_dias,
    ct.ultima_compra_60d AS fecha_ultima_compra,
    RANK() OVER (ORDER BY ct.total_60 DESC) AS ranking_cliente_top
FROM clientes_tot ct
JOIN clientes cl ON cl.cliente_id = ct.cliente_id
ORDER BY ranking_cliente_top;

--CTE compras_60

--WHERE c.fecha_compra >= DATEADD(DAY, -60, GETDATE())
--Filtra solo compras en los últimos 60 días (ventana temporal).

--importe_linea = cantidad × precio_unitario.
--Si tu tabla ya tuviera monto, podés usarla directo.

--Window #1: SUM(...) OVER (PARTITION BY cliente_id)
--Suma todos los importe_linea dentro de esos 60 días por cliente.

--No lleva ORDER BY porque el total no depende del orden.

--Window #2: MAX(fecha_compra) OVER (PARTITION BY cliente_id)
--Devuelve la última fecha de compra del cliente en ese período.

--En este CTE seguís teniendo una fila por compra (no se colapsan filas; las window functions “pintan” el total/última en cada fila del cliente).

--CTE clientes_tot

--Usa SELECT DISTINCT para dejar una sola fila por cliente, ya con sus dos métricas: total_60 y ultima_compra_60d.

--SELECT final

--Se une con clientes para traer el nombre.

--RANK() OVER (ORDER BY total_60 DESC) asigna el ranking (1 = mayor gasto).

--Si hay empates, dos clientes pueden tener el mismo rank (ej., 1 y 1, y el siguiente sería 3).

--Si querés que el siguiente sea 2, usá DENSE_RANK().


