import requests
# from credential import USERNAME, PASSWORD

USERNAME = 'vandan13@gmail.com'
PASSWORD = 'Vandan@13'
CONSUMER_KEY = '3MVG9q4K8Dm94dAx82ZoAUpGJz.k1JwVdc9uwREpNPH4FpmNgC_e_sMKSbqGVpDxBgHQkDr3JOry3BYTqWDvg'
CONSUMER_SECRET = '899485EA85F1AD068A4DCD3178921A6AFC9F643A9EE2722F582B6589815E918F'
DOMAIN_NAME = 'https://dir0000007kzc2ai-dev-ed.develop.my.salesforce.com'

# acquire access token
json_data = {
    'grant_type' : 'password',
    'client_id' :  CONSUMER_KEY,
    'client_secret' : CONSUMER_SECRET,
    'username' : USERNAME,
    'password' : PASSWORD
}
response_access_token = requests.post(DOMAIN_NAME + '/services/oauth2/token', data=json_data)
if response_access_token.status_code == 200:
    access_token_id = response_access_token.json()['access_token']
    print('Access token created')

# example retrieve object metadata
print(response_access_token.status_code)
print(response_access_token.reason)
print(response_access_token.json())

headers={
    'Authorization': 'Bearer ' + access_token_id
}
response_sobject = requests.get(DOMAIN_NAME+ '/services/data/v54.0/sobjects',headers=headers)
print(response_sobject.reason)
print(response_sobject.json())

