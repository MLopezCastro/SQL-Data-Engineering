
---

# Proyecto: `ventas_diarias`

Pipeline mínimo para construir una tabla agregada **por día** a partir de ventas crudas.
Se ejecuta con **un solo script**: `sql/one_click.sql`.

## 📦 Estructura

```
ventas_diarias/
├─ sql/
│  ├─ 00_tables.sql      # crea tablas si no existen
│  └─ one_click.sql      # ejecuta TODO (full → simula hoy → incremental → valida)
└─ README.md             # este archivo
```

## 🚀 Cómo correrlo (SSMS)

1. Abrí `sql/one_click.sql` en **SQL Server Management Studio**.
2. Seleccioná la **base** correcta (arriba a la izquierda) o descomentá `USE TuBase;` al inicio.
3. Presioná **Execute**.
   El script:

   * crea tablas si faltan,
   * hace **full refresh**,
   * **simula** una venta de **hoy**,
   * hace **incremental**,
   * muestra resultados.

> No necesitás ejecutar ningún otro archivo.

---

## 🧠 Qué hace cada paso (adentro de `one_click.sql`)

### 1) Crea tablas (si no existen)

* **`raw_ventas`** *(fuente)*: ventas tal cual, con estas columnas:

  * `id`, `cliente_id`, `fecha_venta` *(DATETIME2)*, `producto`, `cantidad`, `precio_unitario`.
  * Si no existe, el script también **carga 3 filas de ejemplo**.
* **`ventas_diarias`** *(destino)*: tabla agregada por día

  * `fecha` *(DATE, PK)*, `total_monto`, `cantidad_ventas`, `last_loaded_at`.
* **`etl_control`** *(control de estado)*:

  * `pipeline`, `last_loaded_date` (el **watermark**), `updated_at`.

> **Watermark** = “hasta qué fecha ya cargué”. Lo usa el incremental para traer **solo lo nuevo**.

### 2) Full refresh (carga completa)

* Recalcula **todo** desde `raw_ventas` agrupando por día.
* Hace `DELETE FROM ventas_diarias` y luego **INSERT** de los agregados.
* Actualiza el **watermark** en `etl_control` con la **máxima fecha** cargada.

**Cuándo usarlo:** primera carga, o si cambiaste mucha lógica y querés regenerar todo.
**Ventaja:** simple y seguro. **Costo:** si hay millones de filas, es más pesado.

### 3) Simula dato nuevo

* Inserta una venta con `fecha_venta = HOY` en `raw_ventas`.
  Sirve para que veas que el incremental agrega **solo** lo nuevo.

### 4) Incremental (basado en fecha)

* Lee el watermark de `etl_control`.
* Trae datos **desde `watermark - 1 día`** (*solape*) para cubrir ventas tardías.
* Agrega por día y hace **MERGE** contra `ventas_diarias`:

  * si la fecha existe → **UPDATE** totales;
  * si no existe → **INSERT**.
* Sube el **watermark** a la **nueva fecha máxima**.

**Ventaja:** rápido (procesa poco).
**Riesgo si no hay solape:** perder ventas atrasadas. Por eso solapamos 1 día y actualizamos con `MERGE`.

### 5) Validación

Muestra:

* Últimas filas de `ventas_diarias` (debería aparecer **HOY**).
* Registro de `etl_control` (debería tener el **nuevo watermark**).

---

## 📌 Conceptos clave (en criollo)

* **Full refresh** = “borrón y cuenta nueva”: recalculás **todo** siempre.
* **Incremental** = “solo lo nuevo”: desde el watermark + **solape** (para no perder atrasados).
* **Watermark** = última fecha (o `updated_at`) que quedó cargada con éxito.
* **MERGE** = UPSERT: inserta si no existe / actualiza si existe.

---

## 🔎 Cómo ver resultados rápido

```sql
-- últimas 20 fechas cargadas
SELECT TOP (20) * FROM dbo.ventas_diarias ORDER BY fecha DESC;

-- el watermark del pipeline
SELECT * FROM dbo.etl_control WHERE pipeline = 'ventas_diarias';

-- ¿se cargó hoy?
SELECT COUNT(*) AS existe_hoy
FROM dbo.ventas_diarias
WHERE fecha = CAST(GETDATE() AS DATE);
```

---

## 🧯 Problemas comunes (y solución)

* **“No estoy en la base correcta”** → ajustá o descomentá `USE TuBase;` arriba del script.
* **“No tengo raw\_ventas”** → el script la crea; si ya tenés una con otros nombres, adaptá el cálculo:

  * donde dice `CAST(fecha_venta AS date)`, usá tu columna de fecha.
  * `cantidad * precio_unitario` ajustalo a tu esquema.
* **FKs bloquean el borrado** → el script usa `DELETE` (no `TRUNCATE`), así que no debería fallar.

---

## ♻️ Repetir la prueba

Podés ejecutar `one_click.sql` todas las veces que quieras.

* El **full** recalcula todo.
* La **simulación** agrega una venta de hoy (con un `id` nuevo).
* El **incremental** solo actualiza/insertar lo que cambió desde el watermark.

---

## ✅ Qué aprendiste con este ejercicio

* Diferencia entre **full** e **incremental** y **cuándo** usar cada uno.
* Uso de **watermark** + **solape** para evitar “huecos”.
* **MERGE** como patrón de **UPSERT**.
* Una forma **repetible** de construir un **mart** simple (`ventas_diarias`).

---

