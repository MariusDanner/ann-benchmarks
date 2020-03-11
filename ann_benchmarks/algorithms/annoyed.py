from ann_benchmarks.algorithms.base import BaseANN
from kafka import KafkaProducer, KafkaConsumer
import requests
import time
import json

URL = "http://localhost:5000/params"
class Annoyed(BaseANN):

    def __init__(self, metric, n_trees=4):
        if metric == 'angular':
            metric_id = 1
        elif metric == 'euclidean':
            metric_id = 0
        else: 
            raise NotImplementedError(
                f'AnnoyED doesn\'t support metric {metric}')
        self._metric = metric
        self.query_id = 0
        self.name = f'Annoyed(n_trees={n_trees})'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
            }
        response = requests.request("POST", URL, headers=headers, data = f'{n_trees};{300000};{metric_id}')
        print(response.text.encode('utf8'))
        print(n_trees)
        self._producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        self._consumer = KafkaConsumer('sink-topic',bootstrap_servers=['localhost:9092'])


    def publish_message(self, topic_name, key, value):
        try:
            key_bytes = bytes(key, encoding='utf-8')
            value_bytes = bytes(value, encoding='utf-8')
            self._producer.send(topic_name, key=key_bytes, value=value_bytes)
            self._producer.flush()
        except Exception as ex:
            print('Exception in publishing message')
            print(ex)



    def fit(self, X):
        self.start_time = time.time()
        self.stop_time = None
        for i, x in enumerate(X):
            datapoint = {
                'datapointID': f'{i}',
                'vector': x.tolist(),
                'persist': True,
                'write': False
            }
            self.publish_message('source-topic', f'{i}', json.dumps(datapoint))

    def query(self, q, n):
        datapoint = {
                'datapointID': 'Query Point',
                'vector': q.tolist(),
                'persist': False,
                'write': True,
                'k': n
            }
        self.publish_message('source-topic', f'Query Point {self.query_id}', json.dumps(datapoint))
        self.query_id += 1
        msg = next(self._consumer)
        if not self.stop_time:
            self.stop_time = time.time()
            print('Fitting time:', self.stop_time - self.start_time)
        nn = json.loads(msg.value)
        ret = [int(dp) for dp in nn['list']]
        return ret
    
        



    def __str__(self):
        return self.name
