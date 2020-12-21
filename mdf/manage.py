import os
from .sql.create_db import create
from .sql.delete_db import delete
import sqlalchemy


def mdf_exsists(host, port, db_name, user, password):
    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    engine = sqlalchemy.create_engine(db_url)
    r = engine.execute("""SELECT schema_name FROM information_schema.schemata WHERE schema_name in ('workflow','data')""")
    if r.fetchall() == [('workflow',), ('data',)]:
        return True
    else: 
        return False

def mdf_create(host, port, db_name, user, password):
    """
    If mdf is already created in choosen database this function will raise.
    To recreate mdf from scratch delete mdf first. Be aware that this will delete all of your current data.
    """
    
    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    engine = sqlalchemy.create_engine(db_url)
    if not mdf_exsists(host, port, db_name, user, password):
        escaped_sql = sqlalchemy.text(create)
        engine.execute(escaped_sql)
        print('MDF Created!')
    else:
        raise Exception("Micro Data Factory Already Exsists!")
    
def mdf_delete(host, port, db_name, user, password):
    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    engine = sqlalchemy.create_engine(db_url)
    if mdf_exsists(host, port, db_name, user, password):
        if input("This will delete all of your data! Type 'DELETE MDF' continue?") == 'DELETE MDF':
            escaped_sql = sqlalchemy.text(delete)
            engine.execute(escaped_sql)
            print('MDF Deleted!')
        else:
            print('Aborted!')
    else:
        raise Exception("Micro Data Factory Does Not Exsists!")
    