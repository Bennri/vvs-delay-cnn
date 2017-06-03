# Copyright 2015 IBM Corp. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from flask import Flask, jsonify, request
import json
from db_handler import get_db
import time

app = Flask(__name__)
client, db = get_db()
# document = db.get_query_result({
#     '_id': {'$eq': 'statistics'}
# })

minutes_of_an_interval = 5
intervals_each_hour = 60 // minutes_of_an_interval


def get_id_from_timestamp(timestamp):
    gmt = time.gmtime(timestamp * 1000)
    insert_id = gmt.tm_wday * 24 * intervals_each_hour
    insert_id += gmt.tm_hour / intervals_each_hour
    insert_id += gmt.tm_min // minutes_of_an_interval

    return str(insert_id)


@app.route('/', methods=['POST'])
def postData():
    print "#" * 80
    print json.loads(request.data)['docs']
    print "#" * 80
    docs = json.loads(request.data)['docs']
    new_docs = []
    for doc in docs:
        timestamp = doc['timestamp']
        insert_id = get_id_from_timestamp(timestamp)
        current_doc = db[insert_id]
        station = doc['station']
        for line in doc['results']['lines']:
            for line_result in doc['results']['lines'][line]:
                # "2017-06-03T09:33:00Z"
                parse_format = '%Y-%m-%dT%H:%M:%SZ'
                planed = time.strptime(line_result['departureTimePlanned'], parse_format)
                estimated = time.strptime(line_result['departureTimeEstimated'], parse_format)
                delay = time.mktime(estimated) - time.mktime(planed)
                direction = 'b' if 'R' in map(str.strip, str(line_result['id']).split(':')) else 'f'
                current_doc['data'][station][line.lower()+direction][0] += delay
                current_doc['data'][station][line.lower()+direction][1] += 1
        new_docs.append(current_doc)
        db[insert_id] = current_doc

    step_size = 55
    for x in range(0, len(new_docs), step_size):
        print 'sending data...', x
        db.bulk_docs(new_docs[x:x + step_size])
    return '', 200


@app.route('/', methods=['GET'])
def getDelay():
    timestamp = float(request.args.get('timestamp'))
    insert_id = get_id_from_timestamp(timestamp)
    current_doc = db[insert_id]
    return jsonify(current_doc), 200


port = os.getenv('PORT', '5000')
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(port))
