import requests # pip install requests
from consumer_details import CONSUMER_KEY, CONSUMER_SECRET, USERNAME, PASSWORD
import pandas as pd # pip install pandas

# Generate Access Token
def generate_token():    
    payload = {
        'grant_type': 'password',
        'client_id': CONSUMER_KEY,
        'client_secret': CONSUMER_SECRET,
        'username': USERNAME,
        'password': PASSWORD
    }
    oauth_endpoint = '/services/oauth2/token'
    response = requests.post(DOMAIN + oauth_endpoint, data=payload)
    return response.json()

DOMAIN = '<salesforce domain>'
access_token = generate_token()['access_token']
headers = {
    'Authorization': 'Bearer ' + access_token
}

# Example 1. Run a SOQL query
def query(soql_query):
    try:
        # soql_query = 'SELECT name FROM opportunity'
        endpoint = '/services/data/v56.0/query/'
        records = []
        response = requests.get(DOMAIN + endpoint, headers=headers, params={'q': soql_query})
        total_size = response.json()['totalSize']
        records.extend(response.json()['records'])

        while not response.json()['done']:
            response = requests.get(DOMAIN + endpoint + response.json()['nextRecordsUrl'], headers=headers)
            records.extend(response.json()['records'])
        return {'record_size': total_size, 'records': records}
    except Exception as e:
        print(e)
        return

records = query('SELECT id, name, type FROM opportunity')
print(records)
records['record_size']
df = pd.DataFrame(records['records'])
print(df)

# Example 2. Retrieve an object's metadata
def retrieve_object_metadata(object_api_name):
    response = requests.get(DOMAIN + f'/services/data/v54.0/sobjects/{object_api_name}/describe', headers=headers)
    return response.json()

object_id = 'account'
object_metadata = retrieve_object_metadata(object_id)
print(object_metadata)
df_metadata = pd.DataFrame(object_metadata['fields'])
df_metadata.to_csv('account metadata information.csv', index=False)
