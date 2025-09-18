import os, sys, pyodbc
from dotenv import load_dotenv

load_dotenv()
MODE = (sys.argv[1] if len(sys.argv) > 1 else "incremental").lower()
server = os.getenv("SQL_SERVER", "localhost")
database = os.getenv("SQL_DB", "DemoStaging")
trusted = os.getenv("SQL_TRUSTED", "true").lower() in ("1","true","yes")

if trusted:
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes"
else:
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={os.getenv('SQL_USER')};PWD={os.getenv('SQL_PASSWORD')}"

sp = "dbo.sp_load_ventas_diarias_full" if MODE == "full" else "dbo.sp_load_ventas_diarias_incremental"

with pyodbc.connect(conn_str, autocommit=True) as cn:
    cn.execute(f"EXEC {sp};")
    print(f"OK: {sp}")

row = pyodbc.connect(conn_str).cursor().execute("SELECT COUNT(*) FROM dbo.ventas_diarias").fetchone()
print("rows in ventas_diarias:", row[0])
