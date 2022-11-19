from confluent_kafka import Consumer, TopicPartition
import redis
import json
import matplotlib.pyplot as plt
import numpy as np
from statsmodels.tsa.arima.model import ARIMA

def printMetrics(a, v, s):
        print("Average: ", a)
        print("Variance: ", v)
        print("Standard Deviation: ", s)

class DataCapture():
    def __init__(self) -> None:
        self.conf = {
            'bootstrap.servers': 'localhost:29092',
            'group.id': 'test14',     
            'enable.auto.commit': 'false',
            'auto.offset.reset': 'earliest',
            'max.poll.interval.ms': '500000',
            'session.timeout.ms': '120000',
            'request.timeout.ms': '120000'
        }
        self.r = redis.Redis(host='0.0.0.0', port=6379)
        self.r.delete('solargis-data')
    

    def consume(self, topic='lab-kafka'):
        self.consumer = Consumer(self.conf)
        self.topic = topic
        self.consumer.subscribe([self.topic])

        try:
            while True:
                msgs = self.consumer.consume(100, 1.0)                
                if msgs is None:
                    continue

                for msg in msgs:
                    event = msg.value()
                    partition = msg.partition()
                    offset = msg.offset()                                      

                    if event is not  None:
                        self.r.sadd('solargis-data', json.dumps(json.loads(event.decode('utf-8'))))
                        print("consumed")

                        
                    self.consumer.commit(offsets=[TopicPartition(topic = self.topic, partition=partition, offset=offset+1)], asynchronous = False)

                data = list(self.r.smembers('solargis-data'))
                data = data[-100:]
                data = [json.loads(x) for x in data]
                temps = [x['Temperature'] for x in data]

                
                if len(temps) != 0:
                    average = np.mean(temps)
                    variance = np.var(temps)
                    standard_deviation = np.std(temps)
                    
                    printMetrics(average, variance, standard_deviation)

                    mod = ARIMA(temps, order=(5, 1, 0))
                    res = mod.fit()
                    forecast = res.forecast(10, alpha=0.05)
                    plt.plot([i for i in range(len(temps))], temps, label='temperatures', color='blue')
                    plt.plot([i for i in range(len(temps), len(temps)+len(forecast))], forecast, label='forecast', color='red')
                    plt.legend(loc='upper left')
                    plt.savefig('./forecast.png')
                    plt.close()
                
        except KeyboardInterrupt:
            print("interrupted error ")
            self.consumer.close()

capture = DataCapture()
capture.consume('lab-kafka') 