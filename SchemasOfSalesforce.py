from sqlalchemy import text

class SchemasOfSalesforce:
    def __init__(self):
        pass
    
    def get_query(self,table_name, maxdate):
        table_name = table_name.lower()
        query = None

        if table_name == "old_account" :
            query = (f'SELECT Id,CreatedDate,SystemModstamp,AccountNumber,Phone,Fax,Website,Industry,Name,Type,BillingStreet,BillingCity,AnnualRevenue,NumberOfEmployees,Rating,Ownership,OwnerId,CreatedById,CustomerPriority__c,Active__c,SLA__c,LastModifiedDate,LastModifiedById,ShippingStreet,Sic,TickerSymbol,Description,CleanStatus,NumberofLocations__c,UpsellOpportunity__c,SLASerialNumber__c,SLAExpirationDate__c FROM Account WHERE SystemModstamp >={maxdate}')
        elif table_name == "old_opportunity" :
            query = (f'SELECT Id,CreatedDate,SystemModstamp,Name,Type,StageName,Amount,LeadSource,OwnerId,CreatedById,AccountId,LastModifiedDate,LastModifiedById,Probability,ExpectedRevenue,CloseDate,FiscalQuarter,FiscalYear,DeliveryInstallationStatus__c,OrderNumber__c,CurrentGenerators__c,TrackingNumber__c,MainCompetitors__c FROM Opportunity WHERE SystemModstamp >={maxdate}')
        elif table_name == "lead" :
            query = (f'SELECT Id,CreatedDate,SystemModstamp,Name,Title,Country,Company,Salutation,Street,City,State,Phone,Email,LeadSource,Status,Industry,Rating,AnnualRevenue,OwnerId,CreatedById,LastModifiedDate,LastModifiedById,PostalCode,MobilePhone,Fax,Website,Description,NumberOfEmployees,CleanStatus,SICCode__c,ProductInterest__c,CurrentGenerators__c,NumberofLocations__c FROM Lead WHERE SystemModstamp >={maxdate}')
        elif table_name == "user" :
            query = (f'SELECT Id,CreatedDate,SystemModstamp,Username,Name,CompanyName,Email,ProfileId,UserType,LastLoginDate,IsActive,UserRoleId,CreatedById,LastModifiedDate,LastModifiedById FROM User WHERE SystemModstamp >={maxdate}')
        return query
    
    def get_update_query(self,table_name,row):
        table_name = table_name.lower()
        query = None

        if table_name == "old_account" :
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
                    "BillingCity" = '{row['BillingCity']}',
                    "AnnualRevenue" = '{row['AnnualRevenue']}',
                    "NumberOfEmployees" = '{row['NumberOfEmployees']}',
                    "Rating" = '{row['Rating']}',
                    "Ownership" = '{row['Ownership']}',
                    "OwnerId" = '{row['OwnerId']}',
                    "CreatedById" = '{row['CreatedById']}',
                    "CustomerPriority__c" = '{row['CustomerPriority__c']}',
                    "Active__c" = '{row['Active__c']}',
                    "SLA__c" = '{row['SLA__c']}',
                    "LastModifiedDate" = '{row['LastModifiedDate']}',
                    "LastModifiedById" = '{row['LastModifiedById']}',
                    "ShippingStreet" = '{row['ShippingStreet']}',
                    "Sic" = '{row['Sic']}',
                    "TickerSymbol" = '{row['TickerSymbol']}',
                    "Description" = '{row['Description']}',
                    "CleanStatus" = '{row['CleanStatus']}',
                    "NumberofLocations__c" = '{row['NumberofLocations__c']}',
                    "UpsellOpportunity__c" = '{row['UpsellOpportunity__c']}',
                    "SLASerialNumber__c" = '{row['SLASerialNumber__c']}',
                    "SLAExpirationDate__c" = '{row['SLAExpirationDate__c']}'
                WHERE "Id" = '{row['Id']}';
                """)
            
        elif table_name == "old_opportunity" :
            update_query = text(f"""
                UPDATE {table_name}
                SET "Id" = '{row['Id']}',
                    "CreatedDate" = '{row['CreatedDate']}',
                    "SystemModstamp" = '{row['SystemModstamp']}',
                    "Name" = '{row['Name']}',
                    "Type" = '{row['Type']}',
                    "StageName" = '{row['StageName']}',
                    "Amount" = '{row['Amount']}',
                    "LeadSource" = '{row['LeadSource']}',
                    "OwnerId" = '{row['OwnerId']}',
                    "CreatedById" = '{row['CreatedById']}',
                    "AccountId" = '{row['AccountId']}',
                    "LastModifiedDate" = '{row['LastModifiedDate']}',
                    "LastModifiedById" = '{row['LastModifiedById']}',
                    "Probability" = '{row['Probability']}',
                    "ExpectedRevenue" = '{row['ExpectedRevenue']}',
                    "CloseDate" = '{row['CloseDate']}',
                    "FiscalQuarter" = '{row['FiscalQuarter']}',
                    "FiscalYear" = '{row['FiscalYear']}',
                    "DeliveryInstallationStatus__c" = '{row['DeliveryInstallationStatus__c']}',
                    "OrderNumber__c" = '{row['OrderNumber__c']}',
                    "CurrentGenerators__c" = '{row['CurrentGenerators__c']}',
                    "TrackingNumber__c" = '{row['TrackingNumber__c']}',
                    "MainCompetitors__c" = '{row['MainCompetitors__c']}'
                WHERE "Id" = '{row['Id']}';
                """)
        elif table_name == "lead" :
            update_query = text(f"""
                UPDATE {table_name}
                SET "Id" = '{row['Id']}',
                    "CreatedDate" = '{row['CreatedDate']}',
                    "SystemModstamp" = '{row['SystemModstamp']}',
                    "Name" = '{row['Name']}',
                    "Title" = '{row['Title']}',
                    "Country" = '{row['Country']}',
                    "Company" = '{row['Company']}',
                    "Salutation" = '{row['Salutation']}',
                    "Street" = '{row['Street']}',
                    "City" = '{row['City']}',
                    "State" = '{row['State']}',
                    "Phone" = '{row['Phone']}',
                    "Email" = '{row['Email']}',
                    "LeadSource" = '{row['LeadSource']}',
                    "Status" = '{row['Status']}',
                    "Industry" = '{row['Industry']}',
                    "Rating" = '{row['Rating']}',
                    "AnnualRevenue" = '{row['AnnualRevenue']}',
                    "OwnerId" = '{row['OwnerId']}',
                    "CreatedById" = '{row['CreatedById']}',
                    "LastModifiedDate" = '{row['LastModifiedDate']}',
                    "LastModifiedById" = '{row['LastModifiedById']}',
                    "PostalCode" = '{row['PostalCode']}',
                    "MobilePhone" = '{row['MobilePhone']}',
                    "Fax" = '{row['Fax']}',
                    "Website" = '{row['Website']}',
                    "Description" = '{row['Description']}',
                    "NumberOfEmployees" = '{row['NumberOfEmployees']}',
                    "CleanStatus" = '{row['CleanStatus']}',
                    "SICCode__c" = '{row['SICCode__c']}',
                    "ProductInterest__c" = '{row['ProductInterest__c']}',
                    "CurrentGenerators__c" = '{row['CurrentGenerators__c']}',
                    "NumberofLocations__c" = '{row['NumberofLocations__c']}'
                WHERE "Id" = '{row['Id']}';
                """)

        elif table_name == "user" :
            update_query = text(f"""
                UPDATE public.{table_name}
                SET "Id" = '{row['Id']}',
                    "CreatedDate" = '{row['CreatedDate']}',
                    "SystemModstamp" = '{row['SystemModstamp']}',
                    "Username" = '{row['Username']}',
                    "CompanyName" = '{row['CompanyName']}',
                    "Email" = '{row['Email']}',
                    "ProfileId" = '{row['ProfileId']}',
                    "UserType" = '{row['UserType']}',
                    "Name" = '{row['Name']}',
                    "LastLoginDate" = '{row['LastLoginDate']}',
                    "IsActive" = '{row['IsActive']}',
                    "UserRoleId" = '{row['UserRoleId']}',
                    "LastModifiedDate" = '{row['LastModifiedDate']}',
                    "LastModifiedById" = '{row['LastModifiedById']}',
                    "CreatedById" = '{row['CreatedById']}'
                WHERE "Id" = '{row['Id']}';
                """)
        return update_query
    
    
    def get_column_list(self,table_name):
        table_name = table_name.lower()
        columns_list = []

        if table_name == "old_account" :
            columns_list = ["Id","CreatedDate","SystemModstamp","AccountNumber","Phone","Fax","Website","Industry","Name","Type","BillingStreet","BillingCity","AnnualRevenue","NumberOfEmployees","Rating","Ownership","OwnerId","CreatedById","CustomerPriority__c","Active__c","SLA__c","LastModifiedDate","LastModifiedById","ShippingStreet","Sic","TickerSymbol","Description","CleanStatus","NumberofLocations__c","UpsellOpportunity__c","SLASerialNumber__c","SLAExpirationDate__c"]
        elif table_name == "old_opportunity" :
            columns_list = ["Id","CreatedDate","SystemModstamp","Name","Type","StageName","Amount","LeadSource","OwnerId","CreatedById","AccountId","LastModifiedDate","LastModifiedById","Probability","ExpectedRevenue","CloseDate","FiscalQuarter","FiscalYear","DeliveryInstallationStatus__c","OrderNumber__c","CurrentGenerators__c","TrackingNumber__c","MainCompetitors__c"]
        elif table_name == "lead" :
            columns_list = ["Id","CreatedDate","SystemModstamp","Name","Title","Country","Company","Salutation","Street","City","State","Phone","Email","LeadSource","Status","Industry","Rating","AnnualRevenue","OwnerId","CreatedById","LastModifiedDate","LastModifiedById","PostalCode","MobilePhone","Fax","Website","Description","NumberOfEmployees","CleanStatus","SICCode__c","ProductInterest__c","CurrentGenerators__c","NumberofLocations__c"]
        elif table_name == "user" :
            columns_list = ["Id","CreatedDate","SystemModstamp","Username","Name","CompanyName","Email","ProfileId","UserType","LastLoginDate","IsActive","UserRoleId","CreatedById","LastModifiedDate","LastModifiedById"]
        return columns_list
    

