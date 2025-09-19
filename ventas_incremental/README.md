
---

# Proyecto: `ventas_incremental` (SQL puro)

Mini–pipeline incremental en **SQL Server** que construye una tabla `ventas_limpias` a partir de `ventas_crudas`, usando **watermark por fecha** y simulando **tres ejecuciones diarias**.

> Objetivo didáctico: que entiendas el **patrón incremental más simple** (append-only) y cómo controlarlo con SQL.

---

## 📂 Estructura

```
ventas_incremental/
├─ sql/
│  └─ one_click.sql     # UN solo archivo: crea tablas, simula 3 días y ejecuta el incremental
└─ README.md
```

---

## 🚀 Cómo correrlo

1. Abrí **SSMS** y elegí la base de datos donde querés trabajar.
2. Abrí `ventas_incremental/sql/one_click.sql`.
3. (Opcional) Descomentá la línea `-- USE TuBase;` y poné tu DB.
4. Presioná **Execute (F5)** una sola vez.

El script imprime logs y al final muestra resultados.

---

## 🧱 Tablas que usa

* **`ventas_crudas`** *(fuente, sin control)*
  Columnas: `id`, `cliente_id`, `producto_id`, `fecha (DATE)`, `monto (DECIMAL)`.

* **`ventas_limpias`** *(destino del pipeline)*
  Mismas columnas que `ventas_crudas`. Aquí solo insertamos **lo nuevo**.

> Si no existen, el script **las crea** automáticamente antes de correr.

---

## 🧠 Concepto clave: Watermark

Usamos como **watermark** la *última fecha ya procesada* en `ventas_limpias`:

```sql
SELECT COALESCE(MAX(fecha), '19000101') FROM dbo.ventas_limpias;
```

* Si `ventas_limpias` está vacía → `COALESCE` devuelve `1900-01-01`, así la primera corrida **carga todo**.
* En corridas siguientes, traemos **solo filas con fecha mayor a ese valor**.

> Este enfoque es ideal para **append-only** (las filas nuevas siempre tienen fechas posteriores).

---

## 🔁 Flujo del script (qué hace, por bloques)

### 0) Crear tablas (si no existen)

Crea `ventas_crudas` y `ventas_limpias` con sus columnas.
No rompe si ya existen (usa `IF OBJECT_ID(...) IS NULL`).

```sql
IF OBJECT_ID('dbo.ventas_crudas') IS NULL
BEGIN
  CREATE TABLE dbo.ventas_crudas (...);
END;

IF OBJECT_ID('dbo.ventas_limpias') IS NULL
BEGIN
  CREATE TABLE dbo.ventas_limpias (...);
END;
```

---

### 1) Incremental “inicial” (por si ya había datos)

1. Calcula el **watermark**: `MAX(fecha)` en `ventas_limpias`.
2. Inserta **solo** filas de `ventas_crudas` con `fecha > watermark`.
3. Loguea cuántas filas se insertaron.

```sql
DECLARE @watermark DATE = (SELECT COALESCE(MAX(fecha),'19000101') FROM dbo.ventas_limpias);

INSERT INTO dbo.ventas_limpias (id, cliente_id, producto_id, fecha, monto)
SELECT id, cliente_id, producto_id, fecha, monto
FROM dbo.ventas_crudas
WHERE fecha > @watermark;

PRINT ... 'Se insertaron ' + CAST(@@ROWCOUNT AS VARCHAR(20)) + ' filas nuevas';
```

> Si `ventas_limpias` está vacía, **carga todo**; si ya tenía datos, **no duplica**.

---

### 2) Simulación y ejecuciones incrementales

El script **simula tres días de ventas** y, **después de cada día**, vuelve a ejecutar el mismo incremental.

* Día 1: `hoy - 2`
* Día 2: `hoy - 1`
* Día 3: `hoy`

Para evitar choques de PK, calcula `@base_id = MAX(id) + 1` y usa IDs nuevos en cada simulación.

Cada ciclo hace:

1. `INSERT` de 3 filas nuevas en `ventas_crudas` para la fecha simulada.
2. Recalcula `@watermark = MAX(fecha)` en `ventas_limpias`.
3. `INSERT ... SELECT ... FROM ventas_crudas WHERE fecha > @watermark`.

Ejemplo del patrón que se repite:

```sql
-- simular filas en crudas (3 filas)
INSERT INTO dbo.ventas_crudas (id, cliente_id, producto_id, fecha, monto)
VALUES (@base_id, ...), (@base_id+1, ...), (@base_id+2, ...);

-- incremental (trae solo > watermark)
DECLARE @watermark DATE = (SELECT COALESCE(MAX(fecha),'19000101') FROM dbo.ventas_limpias);

INSERT INTO dbo.ventas_limpias (id, cliente_id, producto_id, fecha, monto)
SELECT id, cliente_id, producto_id, fecha, monto
FROM dbo.ventas_crudas
WHERE fecha > @watermark;

PRINT ... 'Se insertaron ' + CAST(@@ROWCOUNT AS VARCHAR(20)) + ' filas nuevas';
```

> Notá la **regla estricta** `fecha > watermark`: cada corrida carga **solo días posteriores** a lo ya procesado (perfecto para el ejercicio *append-only*).

---

### 3) Validación final

Muestra:

* Total de filas en `ventas_crudas` y `ventas_limpias` (deberían quedar **iguales**).
* Rango de fechas en `ventas_limpias` (debería cubrir **los tres días**).
* Cantidad de filas por día (deberías ver **3 por día**).

Consultas del bloque:

```sql
SELECT COUNT(*) AS filas_crudas   FROM dbo.ventas_crudas;
SELECT COUNT(*) AS filas_limpias  FROM dbo.ventas_limpias;

SELECT MIN(fecha) AS min_fecha_limpias,
       MAX(fecha) AS max_fecha_limpias
FROM dbo.ventas_limpias;

SELECT fecha, COUNT(*) AS filas
FROM dbo.ventas_limpias
GROUP BY fecha
ORDER BY fecha DESC;
```

**Resultado esperado** (si ejecutaste una vez el script tal cual):

* `filas_crudas = 9`
* `filas_limpias = 9`
* Rango: de `hoy - 2` a `hoy`
* 3 filas por cada una de esas fechas

---

## 🧩 Qué aprendiste con este ejercicio

* Diferencia entre **carga completa** vs **incremental** (acá usamos incremental puro).
* Cómo **controlar qué procesar** usando un **watermark** guardado en la propia tabla destino.
* Cómo **simular ejecuciones** en días distintos y **validar** que se acumulen bien.
* Qué significa **append-only**: el incremental usa `fecha > última_fecha` (las nuevas filas siempre tienen fechas posteriores).

---

## 📝 Notas y extensiones (para después)

* Si mañana quisieras permitir **nuevas filas del mismo día** (no solo días futuros), la estrategia sería **solape** (`fecha >= watermark`) + una técnica que evite duplicados (p. ej., `MERGE` por `id` o un anti-join por PK).
* Para producción, suele guardarse el watermark en una **tabla de control** (pipeline, última\_fecha, etc.) en vez de deducirlo de `MAX(fecha)`.
* Este SQL se puede envolver en un job (SQL Agent) o llamarlo desde Python/Airflow.

---

