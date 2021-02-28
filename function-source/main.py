#import pymongo as pymongo
#from google.cloud import datastore
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import json

def farmlink_usda_scrape(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload
         context (google.cloud.functions.Context): Metadata for the event.
    """

    # Use the application default credentials (this may/may not be needed once write/read rules are changed in firestore)
    project_id = 'farmlink-304820'
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred, {
    'projectId': project_id,
    })

    db = firestore.client()

    # NOTE: When writing to firestore with JSON data, the highest key in the JSON data must already exist as a document
    # E.g. if a "test" document exists under some collection, a valid JSON object could be 
    # {"test" : {"drew": 9.0}}

    with open('test.json') as json_file:
        data = json.load(json_file)
        print(data)
        doc_ref = db.collection(u'farmlink_transactions').document(u'blueberries')
        doc_ref.set(data)
        

    
