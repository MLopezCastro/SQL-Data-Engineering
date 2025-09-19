# ventas_incremental/python/pipeline.py
import os, logging, datetime
import pyodbc
from dotenv import load_dotenv

# ---------------- Config ----------------
load_dotenv()
SERVER   = os.getenv("SQL_SERVER", "localhost")
DATABASE = os.getenv("SQL_DB", "DemoStaging")
TRUSTED  = os.getenv("SQL_TRUSTED", "true").lower() in ("1","true","yes")
USER     = os.getenv("SQL_USER")
PWD      = os.getenv("SQL_PASSWORD")
DRIVER   = os.getenv("SQL_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d"
)

def connect():
    if TRUSTED:
        cs = f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes"
    else:
        cs = f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PWD}"
    return pyodbc.connect(cs, autocommit=False)

# --------------- DDL --------------------
DDL = """
IF OBJECT_ID('dbo.ventas_crudas') IS NULL
BEGIN
  CREATE TABLE dbo.ventas_crudas(
    id INT PRIMARY KEY,
    cliente_id INT NOT NULL,
    producto_id INT NOT NULL,
    fecha DATE NOT NULL,
    monto DECIMAL(12,2) NOT NULL
  );
END;

IF OBJECT_ID('dbo.ventas_limpias') IS NULL
BEGIN
  CREATE TABLE dbo.ventas_limpias(
    id INT PRIMARY KEY,
    cliente_id INT NOT NULL,
    producto_id INT NOT NULL,
    fecha DATE NOT NULL,
    monto DECIMAL(12,2) NOT NULL
  );
END;
"""

def ensure_tables(cnx):
    with cnx.cursor() as cur:
        cur.execute(DDL)
    cnx.commit()

# --------------- FULL -------------------
def full_refresh(cnx):
    with cnx.cursor() as cur:
        cur.execute("DELETE FROM dbo.ventas_limpias;")
        cur.execute("""
            INSERT INTO dbo.ventas_limpias (id, cliente_id, producto_id, fecha, monto)
            SELECT id, cliente_id, producto_id, fecha, monto
            FROM dbo.ventas_crudas;
        """)
        inserted = cur.rowcount
    cnx.commit()
    logging.info(f"Full refresh: se insertaron {inserted} filas")

# ------------- INCREMENTAL --------------
def get_watermark(cnx):
    with cnx.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(fecha), '19000101') FROM dbo.ventas_limpias;")
        (wm,) = cur.fetchone()  # datetime.date
    return wm

def run_incremental(cnx):
    wm = get_watermark(cnx)
    logging.info(f"Última fecha procesada: {wm}")

    with cnx.cursor() as cur:
        cur.execute("""
            INSERT INTO dbo.ventas_limpias (id, cliente_id, producto_id, fecha, monto)
            SELECT id, cliente_id, producto_id, fecha, monto
            FROM dbo.ventas_crudas
            WHERE fecha > ?
        """, wm)
        inserted = cur.rowcount
    cnx.commit()
    logging.info(f"Se insertaron {inserted} filas nuevas")
    return inserted

# -------------- Simulación --------------
def next_id_base(cur):
    cur.execute("SELECT ISNULL(MAX(id),0) FROM dbo.ventas_crudas;")
    (mx,) = cur.fetchone()
    return int(mx) + 1

def simulate_day(cnx, day: datetime.date, n=3):
    with cnx.cursor() as cur:
        base = next_id_base(cur)
        rows = [(base+i, 1+(i%5), 100+(i%5), day, round(10+ i*5.5, 2)) for i in range(n)]
        cur.executemany(
            "INSERT INTO dbo.ventas_crudas (id, cliente_id, producto_id, fecha, monto) VALUES (?,?,?,?,?)",
            rows
        )
    cnx.commit()
    logging.info(f"Simulación: cargadas {n} filas en ventas_crudas para {day}")

# ---------------- Main ------------------
def main():
    cnx = connect()
    try:
        ensure_tables(cnx)

        # 1) incremental inicial (por si ya había crudas)
        run_incremental(cnx)

        # 2) simular tres días y correr incremental luego de cada día
        today = datetime.date.today()
        for delta in (2, 1, 0):   # hoy-2, hoy-1, hoy
            day = today - datetime.timedelta(days=delta)
            simulate_day(cnx, day, n=3)
            run_incremental(cnx)

        # 3) resumen final
        with cnx.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dbo.ventas_crudas;")
            (crudas,) = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM dbo.ventas_limpias;")
            (limpias,) = cur.fetchone()
            cur.execute("SELECT MIN(fecha), MAX(fecha) FROM dbo.ventas_limpias;")
            mn, mx = cur.fetchone()
        logging.info(f"Resumen | crudas={crudas} | limpias={limpias} | rango={mn}..{mx}")

    finally:
        cnx.close()

if __name__ == "__main__":
    main()
