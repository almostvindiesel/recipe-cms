import requests
import json
import numpy as np
import pandas as pd
import time
import hashlib
from requests.auth import HTTPBasicAuth
import pymysql
import sqlalchemy
import os
import yaml

from yaml import load, dump

import httplib2
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

def main():

    
    db = MySQL_DB()
    pr = PaprikaRecipe()
    pr.fetch_recipes()
    pr.fetch_categories()
    pr.sync_recipes(limit=None)
    pr.sync_categories()
    pr.sync_recipes_to_db(db.conn)

    # # gr = GoogleDocRecipe()
    # # # gr.fetch_recipes()
    # # # # print(gr.recipe_files)
    # # # gr.sync_recipes_to_db(db.conn)
    # # gr.test_get_document()

class MySQL_DB:
    def __init__(self):

        with open(r'settings.yaml') as file:
            settings = yaml.full_load(file)

            environment = 'database_'+ settings['environment']

            db_u = settings[environment]['user']
            db_p = settings[environment]['pass']
            db_host = settings[environment]['host']
            db_name = settings[environment]['database']

        self.conn = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(db_u, db_p, db_host, db_name)) 

class PaprikaRecipe:
    def __init__(self):
        print ("-"*20, "Initializing PaprikaRecipe")
        with open(r'settings.yaml') as file:
            settings = yaml.full_load(file)

            self.u = settings['paprika']['user']
            self.p = settings['paprika']['pass']

        self.recipe_list = []
        self.categories = None
        self.recipes = []
        
    def fetch_recipes(self):
        """Fetches a list of unique recipe ids"""
        print ("Fetching recipe list")
        url = 'https://www.paprikaapp.com/api/v1/sync/recipes/'
        
        recipe_list = None
        try:
            r = requests.get(url, auth=HTTPBasicAuth(self.u, self.p))
            self.recipe_list = r.json()['result']
        except Exception as e:
            print ("Exception:", e, url)
            
        return self.recipe_list
    
    def fetch_recipe(self, recipe_uid):
        """Fetches metadata for individual recipe"""
        print(".", end="")
        url = 'https://www.paprikaapp.com/api/v1/sync/recipe/{}/'.format(recipe_uid)
        
        try:
            r = requests.get(url, auth=HTTPBasicAuth(self.u, self.p))
            recipe = r.json()['result']
        except Exception as e:
            print ("Exception:", e, url)
            recipe = None
            
        return recipe
    
    
    def fetch_categories(self):
        """Fetches set of categories for recipes"""
        print ("Fetching recipe categories")
        url = 'https://www.paprikaapp.com/api/v1/sync/categories/'
        
        try:
            r = requests.get(url, auth=HTTPBasicAuth(self.u, self.p))
            self.categories = r.json()['result']
        except Exception as e:
            print ("Exception:", e, url)
            recipe = None
            
        return self.categories
        
    def sync_categories(self):
        """Replaces category hashes with their category name"""
        print ("Replace category hash with category name")
        # Create dict from categories
        categories_hash = {}
        for item in self.categories:
            categories_hash[item['uid']] = item['name'] 
            
        for recipe in self.recipes:
            recipe['category_names'] = []
            for category_uid in recipe['categories']:
                recipe['category_names'].append(categories_hash[category_uid])

            recipe['category_names'] = ','.join(recipe['category_names'])

    
    def sync_recipes(self, limit=100000):
        """Fetches metadata for all (or a subset) of recipes"""
        print ("Fetching metadata for recipes")

        if limit is None:
            limit = 100000
        for recipe in self.recipe_list[:limit]:
            recipe = self.fetch_recipe(recipe['uid'])
            if recipe:
                if recipe['in_trash'] == False:
                    self.recipes.append(recipe)
        print ("\n")
        return self.recipes
    
    def sync_recipes_to_db(self, db_conn):
        """Sync recipe to db"""
        print ("Writing recipes to db")

        ddl = """
        create table recipe_paprika (
          uid VARCHAR(200),
          name VARCHAR(200),
          rating int,
          created datetime,
          category_names VARCHAR(200),
          directions text,
          description text,
          image_url text,
          photo_url text,
          ingredients text,
          source VARCHAR(200),
          source_url text
        ) DEFAULT CHARSET=utf8mb4;
        """

        try:
            db_conn.execute("use recipes")
            db_conn.execute("drop table if exists recipes.recipe_paprika;")
            db_conn.execute(ddl)
            print (" --> dropped and recreated recipes table")
        except Exception as e:
            print ("Exception:", e)

        
        fields = ['uid','name','rating','created','category_names','description',
                  'directions','image_url','photo_url','ingredients','photo_url','source','source_url']
        df_recipes = pd.DataFrame(self.recipes)


        

        try:
            #print (df_recipes[fields])
            print (" --> saving {} recipes to recipe_paprika ...".format(len(df_recipes)))
            df_recipes[fields].to_sql(con=db_conn, 
                                      schema='recipes',
                                      name='recipe_paprika', 
                                      if_exists='append',
                                      index=False)
            print ("DONE")
            
        except Exception as e:
            print (" !!! ERROR Data didn't load to MySQL DB:", e) 

        return df_recipes[fields]

# class GoogleDocRecipe:
#     def __init__(self):
#         print ("-"*20, "Initializing GoogleDocRecipe")
#         self.SCOPES = 'https://www.googleapis.com/auth/drive.metadata.readonly'
#         self.CLIENT_SECRET_FILE = 'client_secret.json'
#         self.APPLICATION_NAME = 'Drive API Python Quickstart'
#         self.recipe_files = []

#     def get_credentials(self):

#         """Gets valid user credentials from storage.

#         If nothing has been stored, or if the stored credentials are invalid,
#         the OAuth2 flow is completed to obtain the new credentials.

#         Returns:
#             Credentials, the obtained credential.
#         """
#         print ("Getting credientials")
#         home_dir = os.path.expanduser('~')
#         credential_dir = os.path.join(home_dir, '.credentials')
#         if not os.path.exists(credential_dir):
#             os.makedirs(credential_dir)
#         credential_path = os.path.join(credential_dir,
#                                        'drive-python-quickstart.json')

#         store = Storage(credential_path)
#         credentials = store.get()
#         if not credentials or credentials.invalid:
#             flow = client.flow_from_clientsecrets(self.CLIENT_SECRET_FILE, self.SCOPES)
#             flow.user_agent = self.APPLICATION_NAME
#             if flags:
#                 credentials = tools.run_flow(flow, store, flags)
#             else: # Needed only for compatibility with Python 2.6
#                 credentials = tools.run(flow, store)
#             print('Storing credentials to ' + credential_path)        
#         return credentials

#     # def test_get_document(self):

#     #     with open(r'settings.yaml') as file:
#     #         settings = yaml.full_load(file)

#     #         # !!! hide me
#     #         client_id = settings['google_drive']['client_id']
#     #         client_secret = settings['google_drive']['client_secret']
#     #         SCOPES = settings['google_drive']['SCOPES']
#     #         DOCUMENT_ID = settings['google_drive']['DOCUMENT_ID']
       

#     #     creds = None
#     #         # The file token.pickle stores the user's access and refresh tokens, and is
#     #         # created automatically when the authorization flow completes for the first
#     #         # time.
#     #         if os.path.exists('token.pickle'):
#     #             with open('token.pickle', 'rb') as token:
#     #                 creds = pickle.load(token)
#     #         # If there are no (valid) credentials available, let the user log in.
#     #         if not creds or not creds.valid:
#     #             if creds and creds.expired and creds.refresh_token:
#     #                 creds.refresh(Request())
#     #             else:
#     #                 flow = InstalledAppFlow.from_client_secrets_file(
#     #                     'credentials.json', SCOPES)
#     #                 creds = flow.run_local_server(port=0)
#     #             # Save the credentials for the next run
#     #             with open('token.pickle', 'wb') as token:
#     #                 pickle.dump(creds, token)

#     #         service = build('docs', 'v1', credentials=creds)

#     #         # Retrieve the documents contents from the Docs service.
#     #         document = service.documents().get(documentId=DOCUMENT_ID).execute()

#     #         print('The title of the document is: {}'.format(document.get('title')))


#     #     return True

#     # def fetch_recipes(self):
#     #     print ("Fetching recipes")


#     #     limit = 100
#     #     #print ("LIMIT",limit)

#     #     credentials = self.get_credentials()
#     #     http = credentials.authorize(httplib2.Http())
#     #     service = discovery.build('drive', 'v2', http=http)

#     #     parents_query = """
#     #     '1fMHrwD1J3-WhV7d-HIqbvmPhxomlP6k-' in parents 
#     #     or '1Gv0h9svXbcAw86fOJlqkHHfgN5_U8r-C' in parents 
#     #     or '1iprey3gooBGeOP3Sc2UcoHsLwOGSBeK4' in parents
#     #     or '1Q0L7yT6XnbW8cLJGkV-cTpS7B-sdMKT5' in parents
#     #     """

#     #     results = service.files().list(maxResults=limit,q=parents_query).execute()
#     #     #results = service.files().list(maxResults=30).execute()
#     #     items = results.get('items', [])
#     #     if not items:
#     #         print('No files found.')
#     #     else:
#     #         print('Found {} files/recipes'.format(len(items)))

#     #         #print('Files:')
#     #         for item in items:
#     #             if item['mimeType'] == 'application/vnd.google-apps.document' and item['explicitlyTrashed'] == False:
#     #                 #print item['title']
#     #                 self.recipe_files.append({
#     #                     'id':item['id'], 
#     #                     'mimeType': item['mimeType'], 
#     #                     'name':item['title'],
#     #                     'parent':item['parents'][0]['id'],
#     #                     'starred': item['labels']['starred'],
#     #                     'version' : int(item['version'])
#     #                     #,'type': 'googledoc'
#     #                 })



#     #     # try:
#     #     #     self.recipe_files = sorted(self.recipe_files, key=lambda k: k['version'], reverse=True) 
#     #     #     msg = 'success'
#     #     # except Exception as e:
#     #     #     msg = "Something went wrong", str(e) 
#     #     #     print (msg)

#     #     return self.recipe_files

#     #     # return jsonify({'data':files,'msg':msg})


#     # def sync_recipes_to_db(self, db_conn):
#     #     """Sync recipe to db"""
#     #     print ("Writing recipes to db")

#     #     ddl = """
#     #     create table recipe_google (
#     #       id VARCHAR(200),
#     #       name VARCHAR(200),
#     #       parent VARCHAR(200),
#     #       starred BOOLEAN,
#     #       mimeType VARCHAR(200),
#     #       version bigint
#     #     ) DEFAULT CHARSET=utf8mb4;
#     #     """


#     #     try:
#     #         db_conn.execute("use recipes")
#     #         db_conn.execute("drop table if exists recipes.recipe_google")
#     #         db_conn.execute(ddl)
#     #         print (" --> dropped and recreated recipes table")
#     #     except Exception as e:
#     #         print ("Exception:", e)

        

#     #     fields = ['id','name','parent','starred','version','mimeType']
#     #     df_recipes = pd.DataFrame(self.recipe_files)
        

#     #     try:
#     #         #print (df_recipes[fields])
#     #         print (" --> saving {} recipes to recipe_google".format(len(df_recipes)))
#     #         df_recipes[fields].to_sql(con=db_conn, 
#     #                                   schema='recipes',
#     #                                   name='recipe_google', 
#     #                                   if_exists='append',
#     #                                   index=False)
            
#     #     except Exception as e:
#     #         print (" !!! ERROR Data didn't load to MySQL DB:", e) 

#     #     return df_recipes[fields]

if __name__ == "__main__":
    main()



    





