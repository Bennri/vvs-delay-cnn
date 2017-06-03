from cloudant import Cloudant
import os
import json
from cloudant.error import CloudantClientException
import sys


def init_db(db, stations, lines):
    minutes_of_a_week = 7 * 24 * 60
    minutes_of_an_interval = 5
    intervals = minutes_of_a_week // minutes_of_an_interval
    docs = [{'data':
             {str(station_id): {
                   line: [0, 0] for line in lines
               } for station_id in stations},
             '_id': str(x)
             } for x in range(intervals)]

    step_size = 55
    for x in range(0, intervals, step_size):
        print 'sending data...', x
        sys.stdout.flush()
        db.bulk_docs(docs[x:x + step_size])
    print 'initialized database'


def get_db_client(cred_file):
    creds = None

    if 'VCAP_SERVICES' in os.environ:
        vcap = json.loads(os.getenv('VCAP_SERVICES'))
        if 'cloudantNoSQLDB' in vcap:

            creds = vcap['cloudantNoSQLDB'][0]['credentials']
    elif os.path.isfile(cred_file):
        with open(cred_file) as f:

            vcap = json.load(f)
            creds = vcap['cloudantNoSQLDB'][0]['credentials']

    url = 'https://' + creds['host']
    user = creds['username']
    password = creds['password']
    return Cloudant(user, password, url=url, connect=True)


def get_db():
    client = get_db_client('vcap.json')
    stations = json.load(open('stations.json', 'r'))['stations']
    lines = ['s1f', 's2f', 's3f', 's4f', 's5f', 's6f', 's60f',
             's1b', 's2b', 's3b', 's4b', 's5b', 's6b', 's60b']

    try:
        db = client.create_database('statistics', throw_on_exists=True)
        init_db(db, stations, lines)
    except CloudantClientException:
        print 'Using existing db'
        db = client.get('statistics', remote=True)

    return client, db
