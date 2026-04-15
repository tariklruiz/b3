import sqlite3

BASE = r"C:\Users\tarik.lauar\Dropbox\Personal Tarik\01 - Documentos\b3app\backend\data"

for db in ["b3.db", "dividendos.db", "informe_mensal.db"]:
    path = f"{BASE}\\{db}"
    conn = sqlite3.connect(path)
    conn.execute("VACUUM")
    conn.commit()
    conn.close()
    print(f"VACUUM complete: {db}")
