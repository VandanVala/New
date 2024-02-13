import pandas as pd
from sqlalchemy import create_engine
from ConnectorOfSalesforce import ConnectorOfSalesforce
from SchemasOfSalesforce import SchemasOfSalesforce

class ExtractorOfSalesforce:
    _connection = None
    _schemas = None

    def __init__(self):
        self._connection = ConnectorOfSalesforce()
        self._schemas = SchemasOfSalesforce()

    def write_table(self,table_name,initial_load = False) :
        # table_name = table_name.lower()
        engine = create_engine("postgresql+psycopg2://postgres:vala2510@localhost/Salesforce")
        query = self._schemas.get_query(table_name)
        response = self._connection.query_to_raw_data(query)
        records = []
        records.extend(response.json()['records'])
        df = pd.DataFrame(records).drop(columns =['attributes'], axis = 1)

        if initial_load:
            maxDate = '1999-01-01T00:00:00.000+0000'  
        else:
            sql_query = f"Select max(CreatedDate) as MaxDate from {table_name}"
            df = pd.read_sql_query(sql_query, engine)


        if initial_load:
            df.to_sql(name= f'{table_name}', con=engine)
        else:
            df.to_sql(name= f'{table_name}', con=engine, if_exists='append', index=False)


        
initial_load = False
es = ExtractorOfSalesforce()
es.write_table("Lead")
print('Salesforce Extract Succeeded')




