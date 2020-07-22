import os

from google.oauth2 import service_account
import googleapiclient.discovery

def list_service_accounts(project_id):
    """Lists all service accounts for the current project."""

    credentials = service_account.Credentials.from_service_account_file(
        filename=os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
        scopes=['https://www.googleapis.com/auth/cloud-platform'])

    service = googleapiclient.discovery.build(
        'iam', 'v1', credentials=credentials)

    service_accounts = service.projects().serviceAccounts().list(
        name='projects/' + project_id).execute()

    for account in service_accounts['accounts']:
        print('Name: ' + account['name'])
        print('Email: ' + account['email'])
        print(' ')
    return service_accounts

list_service_accounts('kb-daas-dev')
