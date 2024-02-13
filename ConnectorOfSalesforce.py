import requests
import pandas as pd

class ConnectorOfSalesforce:
    
    def query_to_raw_data(self,soql_query) :
        DOMAIN = 'https://dir0000007kzc2ai-dev-ed.develop.my.salesforce.com'
        payload = {
            'grant_type': 'password',
            'client_id': '3MVG9q4K8Dm94dAx82ZoAUpGJzxCl1nqdqwggxJSZ2vtXBAIwpFzA_a5DAD9jTSxflPLpWDpkDlWCklhpXAET',
            'client_secret': 'D61170486F92AA8EBD2D73D9174F55A644E399EA29A70A12CA433363851817BF',
            'username': 'vandan13@gmail.com',
            'password': 'Vandan@13'
        }
        oauth_endpoint = '/services/oauth2/token'
        response = requests.post(DOMAIN + oauth_endpoint, data=payload)
        token = response.json()['access_token']
        headers = {
        'Authorization': 'Bearer ' + f'{token}'
        }
        endpoint = '/services/data/v54.0/query/'
        response = requests.get(DOMAIN + endpoint, headers = headers , params = {'q': soql_query})
        return response
