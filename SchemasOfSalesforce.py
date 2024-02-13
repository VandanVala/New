from sqlalchemy import text

class SchemasOfSalesforce:
    def __init__(self):
        pass
    
    def get_query(self,table_name, maxdate):
        table_name = table_name.lower()
        query = None

        if table_name == "account" :
            query = (f'SELECT Id,CreatedDate,SystemModstamp,AccountNumber,Phone,Fax,Website,Industry,Name,Type,BillingStreet,BillingCity FROM Account WHERE SystemModstamp >={maxdate}')
        elif table_name == "opportunity" :
            query = (f'SELECT Id,CreatedDate,SystemModstamp,Name,Type FROM Opportunity WHERE SystemModstamp >={maxdate}')
        elif table_name == "lead" :
            query = (f'SELECT Id,CreatedDate,SystemModstamp,IsDeleted,Name,Title,Country,Company FROM Lead WHERE SystemModstamp >={maxdate}')
        return query
    
    def get_update_query(self,table_name,row):
        table_name = table_name.lower()
        query = None

        if table_name == "account" :
            update_query = text(f"""
                UPDATE {table_name}
                SET "Id" = '{row['Id']}',
                    "CreatedDate" = '{row['CreatedDate']}',
                    "SystemModstamp" = '{row['SystemModstamp']}',
                    "AccountNumber" = '{row['AccountNumber']}',
                    "Phone" = '{row['Phone']}',
                    "Fax" = '{row['Fax']}',
                    "Website" = '{row['Website']}',
                    "Industry" = '{row['Industry']}',
                    "Name" = '{row['Name']}',
                    "Type" = '{row['Type']}',
                    "BillingStreet" = '{row['BillingStreet']}',
                    "BillingCity" = '{row['BillingCity']}'
                WHERE "Id" = '{row['Id']}';
                """)
            
        elif table_name == "opportunity" :
            update_query = text(f"""
                UPDATE {table_name}
                SET "Id" = '{row['Id']}',
                    "CreatedDate" = '{row['CreatedDate']}',
                    "SystemModstamp" = '{row['SystemModstamp']}',
                    "Name" = '{row['Name']}',
                    "Type" = '{row['Type']}'
                WHERE "Id" = '{row['Id']}';
                """)
        elif table_name == "lead" :
            update_query = text(f"""
                UPDATE {table_name}
                SET "Id" = '{row['Id']}',
                    "CreatedDate" = '{row['CreatedDate']}',
                    "SystemModstamp" = '{row['SystemModstamp']}',
                    "IsDeleted" = '{row['IsDeleted']}',
                    "Name" = '{row['Name']}',
                    "Title" = '{row['Title']}',
                    "Country" = '{row['Country']}',
                    "Company" = '{row['Company']}'
                WHERE "Id" = '{row['Id']}';
                """)
        return update_query
    
    
    def get_column_list(self,table_name):
        table_name = table_name.lower()
        columns_list = []

        if table_name == "account" :
            columns_list = ["Id",
                    "CreatedDate",
                    "SystemModstamp",
                    "AccountNumber",
                    "Phone",
                    "Fax",
                    "Website",
                    "Industry",
                    "Name",
                    "Type",
                    "BillingStreet",
                    "BillingCity"]
            
        elif table_name == "opportunity" :
            columns_list = ["Id",
                    "CreatedDate",
                    "SystemModstamp",
                    "Name",
                    "Type"]
        elif table_name == "lead" :
            columns_list = ["Id",
                    "CreatedDate",
                    "SystemModstamp",
                    "IsDeleted",
                    "Name",
                    "Title",
                    "Country",
                    "Company"]
        return columns_list
    

