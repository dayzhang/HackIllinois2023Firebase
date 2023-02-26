import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from scrape_keycaps import *
from scrape_switches import *

def delete_collection(coll_ref, batch_size):
    docs = coll_ref.list_documents(page_size=batch_size)
    deleted = 0

    for doc in docs:
        print(f'Deleting doc {doc.id} => {doc.get().to_dict()}')
        doc.delete()
        deleted = deleted + 1

    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size)

def main(event, context):
    cred = credentials.Certificate('serviceAccountKey.json')
    firebase_admin.initialize_app(cred, 
    {
    'databaseURL': 'https://hackillinois2023-af9e1.firebaseio.com/'
    })
    
    db = firestore.client()

    caps = (get_cannon_keycaps, get_dang_keycaps, get_kbd_keycaps, get_keys_keycaps, get_kono_keycaps, get_novelkeys_keycaps, get_space_keycaps, get_osume_keycaps)
    x = []
    for i, func in enumerate(caps):
        curr = func()
        x.append(curr)
        print(i)
    
    df = pd.concat(x, ignore_index=True)
    df.to_csv("/tmp/keycaps.csv")
    
    doc_ref = db.collection(u'keycaps')# Import data
    delete_collection(doc_ref, 50)
    
    df = pd.read_csv('/tmp/keycaps.csv')
    tmp = df.to_dict(orient='records')
    list(map(lambda x: doc_ref.add(x), tmp))

    """
    switches = (get_kono_switches, get_novelkeys_switches, get_cannon_switches, get_dang_switches, get_kbd_switches, get_keys_switches)
    y = []
    for i, func in enumerate(switches, start=4):
        curr = func()
        y.append(curr)  
        print(i)
        
    df_switch = pd.concat(y, ignore_index=True)
    df_switch.to_csv("/tmp/switches.csv")

    doc_ref = db.collection(u'switches')# Import data
    delete_collection(doc_ref, 50)
    
    df = pd.read_csv('/tmp/switches.csv')
    tmp = df.to_dict(orient='records')
    list(map(lambda x: doc_ref.add(x), tmp))
    """