
---

# Pipeline incremental en Python + SQL Server

Este mini–proyecto arma un pipeline **incremental** (append-only) contra SQL Server usando **Python + pyodbc**.
El script:

1. Crea tablas si no existen.
2. Lee el **watermark** (`MAX(fecha)` en la tabla destino).
3. Inserta **solo lo nuevo** (`fecha > watermark`).
4. **Simula 3 días** de datos (hoy-2, hoy-1, hoy) y ejecuta el incremental tras cada día.
5. Muestra un **resumen** (conteos y rango de fechas).

---

## 📦 Estructura

```
ventas_incremental/
└─ python/
   ├─ pipeline.py         # script principal
   ├─ requirements.txt    # dependencias (pyodbc, python-dotenv)
   └─ .env.example        # variables de entorno (conexión a SQL)
```

> **Importante:** el entorno virtual `.venv/` no se trackea en Git (agregado en `.gitignore`).

---

## ▶️ Cómo ejecutar (paso a paso)

1. **Parate en la carpeta**

```powershell
cd SQL-Data-Engineering\ventas_incremental\python
```

2. **Activá tu venv e instalá dependencias**

```powershell
..\..\.venv\Scripts\Activate
pip install -r requirements.txt
```

3. **Configurá el `.env`**

```powershell
Copy-Item .env.example .env
notepad .env
```

Completá con TU instancia (ejemplos):

```
SQL_SERVER=DESKTOP-UFKKV4B\SQLEXPRESS
SQL_DB=DemoStaging           # la DB donde querés crear/usar las tablas
SQL_TRUSTED=true             # true = autenticación de Windows
SQL_ODBC_DRIVER=ODBC Driver 17 for SQL Server
# Si usás usuario/clave (SQL_TRUSTED=false), agregá:
# SQL_USER=sa
# SQL_PASSWORD=TuPassword
```

4. **Corré el pipeline**

```powershell
python pipeline.py
```

### ¿Qué debería imprimir?

Logs como:

```
2025-09-18 | INFO | Última fecha procesada: 1900-01-01
2025-09-18 | INFO | Se insertaron 0 filas nuevas
2025-09-18 | INFO | Simulación: cargadas 3 filas en ventas_crudas para 2025-09-16
2025-09-18 | INFO | Se insertaron 3 filas nuevas
...
2025-09-18 | INFO | Resumen | crudas=9 | limpias=9 | rango=2025-09-16..2025-09-18
```

<img width="858" height="277" alt="PIPELINEPY" src="https://github.com/user-attachments/assets/858bdf98-3dc3-4f5f-a5a0-5685dd743b9f" />

---

## 🧱 Esquema de datos

* **ventas\_crudas** *(fuente sin control)*
  `id (PK), cliente_id, producto_id, fecha (DATE), monto (DECIMAL)`

* **ventas\_limpias** *(destino del pipeline)*
  Misma estructura que `ventas_crudas`. Acá insertamos **solo filas nuevas**.

> Si las tablas no existen, `pipeline.py` las crea.

---

## 🧠 Lógica incremental (en criollo)

* **Watermark:** usamos `MAX(fecha)` de `ventas_limpias` como “última fecha procesada”.
* **Filtro:** `fecha > watermark` → trae **solo días posteriores** a lo ya cargado.
* **Idempotencia:** si corrés de nuevo sin nuevos días, inserta **0 filas**.
* **Simulación:** el script agrega 3 filas por día para **hoy-2**, **hoy-1** y **hoy**, y corre el incremental cada vez.

> Ojo: como el filtro es `>`, **no captura nuevas filas del mismo día** (si el watermark ya es “hoy”). Eso es correcto para el caso **append-only** pura (las nuevas ventas siempre son de días futuros).

---

## 🔎 Validación rápida en SSMS

```sql
SELECT COUNT(*) FROM dbo.ventas_crudas;
SELECT COUNT(*) FROM dbo.ventas_limpias;

SELECT MIN(fecha) AS min_fecha, MAX(fecha) AS max_fecha
FROM dbo.ventas_limpias;

SELECT fecha, COUNT(*) AS filas
FROM dbo.ventas_limpias
GROUP BY fecha
ORDER BY fecha DESC;
```

* Esperado tras una corrida “limpia”: **crudas=9**, **limpias=9**, 3 días (hoy, hoy-1, hoy-2) con **3 filas** cada día.

---

## 🧪 ¿Te dio “0 filas nuevas”? (por qué y cómo probar)

Si ya corriste un ejercicio previo en SQL y `ventas_limpias` quedó con la **fecha máxima = hoy**, el incremental de Python (con `fecha > watermark`) **no** insertará más filas para hoy.

Dos formas de probar que **sí inserta**:

1. **Resetear** (rápida para demo):

```sql
DELETE FROM dbo.ventas_limpias;
DELETE FROM dbo.ventas_crudas;
```

Corré `python pipeline.py` otra vez → deberá terminar con **crudas=9 | limpias=9**.

2. **Mantener datos y captar “mismo día”** (cambio opcional de lógica):
   en `pipeline.py`, podés reemplazar `run_incremental` por una versión con **solape y anti-join por `id`**:

```python
def run_incremental(cnx):
    wm = get_watermark(cnx)
    logging.info(f"Última fecha procesada: {wm}")
    with cnx.cursor() as cur:
        cur.execute("""
            INSERT INTO dbo.ventas_limpias (id, cliente_id, producto_id, fecha, monto)
            SELECT c.id, c.cliente_id, c.producto_id, c.fecha, c.monto
            FROM dbo.ventas_crudas AS c
            LEFT JOIN dbo.ventas_limpias AS l
              ON l.id = c.id
            WHERE c.fecha >= ?       -- solape: incluye el mismo día
              AND l.id IS NULL;      -- evita duplicados por PK
        """, wm)
        inserted = cur.rowcount
    cnx.commit()
    logging.info(f"Se insertaron {inserted} filas nuevas")
    return inserted
```

> Esta variante **incluye hoy** y evita duplicados por `id`. Es el primer paso hacia un **UPSERT** real (con `MERGE`).

---

## 🧯 Troubleshooting

* **`IM002 [Microsoft][ODBC Driver Manager] Data source name not found`**
  → Cambiá `SQL_ODBC_DRIVER` en `.env` al que tengas instalado (17 o 18).
  → Instalá “ODBC Driver for SQL Server” desde Microsoft si falta.

* **Login failed / base no existe**
  → Ajustá `SQL_DB`, `SQL_TRUSTED`, `SQL_USER/SQL_PASSWORD` en `.env`.
  → Verificá que te conectás con SSMS a esa DB.

* **`can't open file 'pipeline.py'`**
  → Estás en otra carpeta. Parate en: `ventas_incremental\python`.

* **`requirements.txt not found`**
  → Ejecutá `pip install -r requirements.txt` **dentro** de `ventas_incremental\python`.

---

## 🧰 Qué aprendiste

* **Incremental por fecha** con `MAX(fecha)` como watermark.
* Diferencia entre **append-only** (`fecha > watermark`) y **solape** (`fecha >= watermark` + anti-join/`MERGE`).
* Simulación de múltiples corridas y **logs** para auditar cuántas filas entran.
* Configuración de **pyodbc** con `.env` y venv en Windows.

---

## 🎯 Próximos pasos (si querés subirle el nivel)

* **UPSERT con `MERGE`** por `id` (soporta updates):
  útil si se corrigen montos o llegan duplicados.
* **Tabla de control** para guardar el watermark (en vez de `MAX(fecha)`).
* **Orquestación**: programarlo con Windows Task Scheduler / SQL Agent / Airflow.
* **Logging a archivo** (rotación) y métricas (filas leídas/cargadas/tiempo).

---

## 📝 Comandos útiles

```powershell
# activar venv (desde python/)
..\..\.venv\Scripts\Activate

# correr pipeline
python pipeline.py

# actualizar dependencias
pip install -r requirements.txt

# subir a git (desde la raíz del repo)
git add ventas_incremental/python/*
git commit -m "pipeline Python incremental con watermark"
git push
```


