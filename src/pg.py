import os
# import psycopg2
import warnings
warnings.filterwarnings("ignore")
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, URL
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import prefect as pf

# get the path of the current file :
path = os.path.dirname(os.path.realpath(__file__)) + "/"
# !!!! if in production : put the absolute path of the .env file :
env_path = path + "../.env"

# define a dict with the connection info :

def _load_creds():
    try: 
        load_dotenv(dotenv_path=env_path)
        return {
            "postgresql" : {
                "sqlalchemy" : "postgresql",
                "user": os.environ.get("postgres_user"),
                "password": os.environ.get("postgres_pass"),
                "host": os.environ.get("postgres_host"),
                "database": os.environ.get("postgres_dbname"),
                "port": os.environ.get("postgres_port")
            }
        }
    except Exception as e:
        print("loading .env failed >> ", e)


class sql_connect : 
    """
    This class is an object for postgress connection.
     
    Attributes
    ----------
    source : str
        source of the database (quant, datahub, mysql, ..)
    """
    def __init__(   self,
                    source: str = "postgresql",
                    user: str = None,
                    password: str = None,
                    host: str = None,
                    database: str = None,
                    port: str = None):
        # check if type is in the list :
        if source not in ["postgresql", "datahub", "mysql"]:
            raise Exception("source must be in ['postgresql', 'datahub', 'mysql', ..]")
        else :
            self.source = source
        # load the credentials :
        config_sql = _load_creds()
        # check if the user is not None
        if database is not None:
            config_sql[source]["user"] = user
            config_sql[source]["password"] = password
            config_sql[source]["host"] = host 
            config_sql[source]["port"] = port    
        # allow the user to connect to a specific database :
        if database is not None:
            config_sql[source]["database"] = database
        # check if credentials are provided :
        if config_sql[source]["user"] is None:
            raise Exception("credentials must be provided")
        # create the engine and session :
        try : 
           self.__connect(config_sql, source)
        except Exception as e:
            print("connection to postgres failed >> ", e)  
    def __connect(self, config_sql, source):
        # create engine :
        url_object = URL.create(
            drivername=config_sql[source]["sqlalchemy"],
            username=config_sql[source]["user"],
            password=config_sql[source]["password"], 
            host=config_sql[source]["host"],
            database=config_sql[source]["database"],
            # port=port
        )
        self.engine = create_engine(url_object)
        # create session :
        Session = sessionmaker(bind=self.engine)
        self.session = Session()  
    def query(self, query):
        # Execute the query
        result = self.session.execute(text(query))
        # Fetch all the rows of the resultset
        resultset = result.fetchall()
        # create a dateframe from the resultset using types as columns names and types
        return pd.DataFrame(resultset).convert_dtypes()
    def get_tables_list(self):
        # reflect db schema to MetaData
        metadata_obj = MetaData()
        # reflect tables
        metadata_obj.reflect(self.engine)
        # return list of table names dataframe
        list = [table_obj.name for table_obj in metadata_obj.sorted_tables]
        # convert to pd.DataFrame
        return pd.DataFrame(list, columns=["tables"])
    def get_columns_type(self, table_name):
        # reflect db schema to MetaData
        metadata_obj = MetaData()
        # reflect tables
        metadata_obj.reflect(self.engine)
        # get table object
        table_obj = metadata_obj.tables[table_name]
        # get columns
        columns = [column_obj.name for column_obj in table_obj.columns]
        # get types
        types = [c.type.python_type  for c in table_obj.columns]
        # get table info
        df = pd.DataFrame({"columns": columns,"types": types})
        df["types"] = [str(a) for a in df["types"]]
        return df
    def disconnect(self):
        self.session.close()
        self.engine.dispose()
    def export_table(self, name_table, df, overwrite="append"):
        df.to_sql(name_table, self.engine, if_exists=overwrite, index=False) # if_exists = {‘fail’, ‘replace’, ‘append’}, default ‘fail’
    
# ex postgress > query = "SELECT x.* FROM public.trade_watchlist x"
# ex mssql > query = "SELECT TOP (1000) * FROM [DataHub].[dbo].[vwTrade]"
        
#@pf.task(name="[pg] export")
def export_to_pg(table,data,overwrite="append"): 
    # export table :
    con = sql_connect("postgresql")
    con.export_table(name_table=table,df=data, overwrite=overwrite)
    con.disconnect()