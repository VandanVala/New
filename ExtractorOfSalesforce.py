import pandas as pd
from sqlalchemy import create_engine
from ConnectorOfSalesforce import ConnectorOfSalesforce
from SchemasOfSalesforce import SchemasOfSalesforce
import json

class ExtractorOfSalesforce:
    _connection = None
    _schemas = None

    def __init__(self):
        self._connection = ConnectorOfSalesforce()
        self._schemas = SchemasOfSalesforce()

    def sync_salesforce_data(self,table_name,initial_load = False) :
        # table_name = table_name.lower()
        maxdate = None
        engine = create_engine("postgresql+psycopg2://postgres:vala2510@localhost/Salesforce")

        if str(initial_load).lower() == 'true' or str(initial_load).lower() == '1':
            initial_load = True
        elif str(initial_load).lower() == 'false' or str(initial_load).lower() == '0':
            initial_load = False

        if initial_load:
            maxdate = '1999-01-01T00:00:00.000+0000'  
        else:
            sql_query = f'SELECT MAX("SystemModstamp") AS maxdate FROM {table_name}'
            df = pd.read_sql(sql_query, engine)
            maxdate = df["maxdate"][0]
            
        query = self._schemas.get_query(table_name, maxdate)
        response = self._connection.query_to_raw_data(query)

        if response.json()['records'] == [] or response.json()['records'] == None :
            return print(f'NO update data available for table {table_name}')

        records = []
        records.extend(response.json()['records'])
        df = pd.DataFrame(records).drop(columns =['attributes'], axis = 1)
            
        if initial_load:
            df.to_sql(name= f'{table_name}', con=engine, if_exists='replace')
        else:
            sql_query = f'SELECT "Id" FROM {table_name}'
            dfdatabase = pd.read_sql(sql_query, engine)
            columns_list = self._schemas.get_column_list(table_name)  # Replace these with your actual column names
            dfupdates = pd.DataFrame(columns=columns_list)
            dfinsert = pd.DataFrame(columns=columns_list)

            for index, row in df.iterrows():
                # Create dataframe of rows to updates
                if (dfdatabase["Id"] == row["Id"]).any():
                    dfupdates = pd.concat([dfupdates, pd.DataFrame([row])], ignore_index=True)
                # Create dataframe of rows to insert
                else :
                    dfinsert = pd.concat([dfinsert, pd.DataFrame([row])], ignore_index=True)
            
            if not dfupdates.empty :
                for index, row in dfupdates.iterrows():
                # Update existing row in a table
                    update_query = self._schemas.get_update_query(table_name,row)
                    with engine.begin() as conn:
                        conn.execute(update_query)
                print(f'Data updated in {table_name} table')

            if not dfinsert.empty :
                # Insert new data in a table
                dfinsert.to_sql(name= f'{table_name}', con=engine, if_exists='append', index=False)
                print(f'Data inserted in {table_name} table')
            else :
                print(f'No Data available to inserted in {table_name} table')
                    

initial_load = False
es = ExtractorOfSalesforce()
es.sync_salesforce_data("account", initial_load)
es.sync_salesforce_data("opportunity", initial_load)
es.sync_salesforce_data("lead",initial_load)
print('Salesforce Extract Succeeded')




