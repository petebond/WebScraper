from sqlalchemy import create_engine
from sqlalchemy import inspect

DATABASE_TYPE = "postgresql"
DBAPI = 'psycopg2'
ENDPOINT = 'chess-db.cxwlqkybpl0p.eu-west-2.rds.amazonaws.com'
USER = 'postgres'
PASSWORD = 'chesspass'
PORT = 5432
DATABASE = 'chessdb'
engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
engine.connect()
insp = inspect(engine)
print(insp)
for table_name in insp.get_table_names():
   for column in insp.get_columns(table_name):
       print("Column: %s" % column['name'])