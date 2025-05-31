#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))



from db.schema import connect_db, create_tables

if __name__ == "__main__":
    conn = connect_db()
    create_tables(conn)
    print("✅ Database initialized successfully.")
