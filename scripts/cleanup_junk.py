import sqlite3
from pathlib import Path

con = sqlite3.connect(Path("data/updown_history.db"))
deleted = con.execute(
    """
    DELETE FROM rounds
    WHERE start_price IS NULL
      AND end_price IS NULL
      AND result IS NULL
    """
).rowcount
con.commit()
print("deleted", deleted)
print("remaining", con.execute("SELECT count(1) FROM rounds").fetchone()[0])
print("settled", con.execute("SELECT count(1) FROM rounds WHERE result IS NOT NULL").fetchone()[0])
