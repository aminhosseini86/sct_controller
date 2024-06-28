import json
import requests
import pymysql
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import pika
from datetime import datetime

es = Elasticsearch()


def send_command(data):
    rabbitmq = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', heartbeat=0))
    rc = rabbitmq.channel()
    rc.queue_declare(queue='SCOS')
    rc.basic_publish(exchange='', routing_key='SCOS', body=data.encode())
    rabbitmq.close()


def send_callback(url, data):
    res = requests.post(
        url="http://127.0.0.1:8091/api/v1/" + url, json=data,
        headers={'Authorization': '$2y$12$XkZr2ko0b5wpPp5vLQQfAum8fcLjil0Pq/FjeZJb7gRh2d9mOfw0W'}
    )
    # print(res.content)
    # print(res.status_code)


def check_callback(msg):
    # print(msg)
    if (msg['command'] == 'L0' or msg['command'] == 'L1') and len(msg['data']) == 0:
        # print("AAA")
        db = pymysql.connect(
            host="localhost",
            user="root",
            passwd="Scooter0241#$db",
            database="scooter"
        )

        cursor = db.cursor()
        cursor.execute("SELECT `id`, `tracking_code`, `call_back`, `data` FROM `commands` "
                       "WHERE `imei`=%s AND `command`='R0' ORDER BY `timestamp` DESC LIMIT 1", (msg['imei'],))
        result = cursor.fetchone()
        if result is not None:
            if result[2] == 0:
                send_callback(url='iot/requests/' + str(result[1]) + '/reply', data={"message": "Done"})
                cursor.execute("UPDATE `commands` SET `call_back`='1' WHERE `id`=%s", (result[0],))
                db.commit()
        cursor.close()
        db.close()
    elif msg['command'] == 'W0' and msg['source'] == 'SCOR':
        send_callback(url='exam/7', data={"imei": msg['imei'], "answers": [
            {"question_id": 8, "type": 3, "value": msg['imei']},
            {"question_id": 12, "type": 1, "value": int(msg['data'][0]) + 32}
        ]})


def process_msg(msg):
    msg = json.loads(msg.decode())
    # print(msg)
    check_callback(msg)
    if msg['source'] == 'SCOR':
        # check_callback(msg)
        allow_commands = ['R0', 'L0', 'L1', 'W0']
        data = '*SCOS,OM,' + msg['imei']
        if msg['command'] == 'R0':
            if msg['data'][0] == '0':
                data += ',L0,' + msg['data'][1] + ',' + msg['data'][2] + ',' + str(
                    int(datetime.now().timestamp())) + '#\n'
            else:
                data += ',L1,' + msg['data'][1] + ',' + msg['data'][2] + ',' + str(
                    int(datetime.now().timestamp())) + '#\n'
        elif msg['command'] == 'L0':
            data += ',L0#\n'
        elif msg['command'] == 'L1':
            data += ',L1#\n'
        elif msg['command'] == 'W0':
            data += ',W0#\n'

        if msg['command'] in allow_commands:
            # print("Send Command", data)
            send_command(data)


if __name__ == '__main__':
    consumer = KafkaConsumer('scooter', bootstrap_servers=['localhost:9092'])
    for message in consumer:
        process_msg(message.value)
