import sqlite3
from pathlib import Path

con = sqlite3.connect(Path("data/updown_history.db"))
con.row_factory = sqlite3.Row
print("rounds", con.execute("select count(1) from rounds").fetchone()[0])
print(
    "settled",
    con.execute("select count(1) from rounds where result is not null").fetchone()[0],
)
print(list(con.execute("select result, count(1) c from rounds group by result")))
for r in con.execute(
    """
    select id, result, win_side, start_price, end_price, status_name, collected_from
    from rounds
    order by scraped_at desc
    limit 10
    """
):
    print(dict(r))
