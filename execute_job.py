import json
import random
import sqlite3
from time import sleep
import init_db
from kafka import KafkaProducer

from config import DATABASE

STATUS_NEW = 'NEW'
STATUS_INPROGRESS = 'IN_PROGRESS'
STATUS_COMPLETED = "COMPLETED"
SQL_SCRIPT = 'resources/config.sql'


def poll_db_for_a_job():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.execute('select * from job where STATUS = ?', (STATUS_NEW,))
    result = cursor.fetchone()
    conn.close()
    return result


def get_a_job():
    job = poll_db_for_a_job()
    if job:
        return dict(id=job[0], objects=job[1].split(','), count=job[2], kafka_host=job[3], zookeeper_host=job[4],
                    topics=job[5])
    return None


def update_job_status(job_id, status):
    conn = sqlite3.connect(DATABASE)
    conn.execute('update job set STATUS = ? where id = ?', (status, job_id))
    conn.commit()
    conn.close()


def get_default_object_data_by_name(name):
    print('Fetching Default object data from Config.')
    conn = sqlite3.connect(DATABASE)
    cursor = conn.execute('select * from config where key = ?', (name,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return json.loads(result[2].replace("\'", "\""))
    else:
        return None


def get_dynamic_field_by_object_name(object_name):
    dynamic_fields = []
    conn = sqlite3.connect(DATABASE)
    cursor = conn.execute('select * from config where key = ?', (object_name + '.fields.dynamic',))
    result = cursor.fetchone()
    conn.close()
    if result:
        dynamic_fields = str(result[2]).split(',')
    return dynamic_fields


def get_random_number():
    return random.randint(0, 10000)


def execute_job(job):
    print("Executing job")
    object_names_to_dump = job['objects']
    no_of_objects_to_dump = job['count']
    topics = job['topics'].split(',')
    for object_name in object_names_to_dump:
        print('Processing object : ' + object_name)
        object_to_dump = get_default_object_data_by_name(object_name)

        if object_to_dump is None:
            print("Default object structure not found in config. Skipping object : " + object_name)
            continue

        count = 0
        print('Preparing and writing ' + str(no_of_objects_to_dump) + ' objects to Kafka')
        while count < no_of_objects_to_dump:
            for dynamic_field in get_dynamic_field_by_object_name(object_name):
                object_to_dump[dynamic_field] += str(get_random_number())
            producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'),
                                     bootstrap_servers=[job['kafka_host']])
            for topic in topics:
                producer.send(topic, object_to_dump)
            count += 1
        print('Processing object ' + object_name + ' is Successful.')
    print("Done!")


def execute():
    job = get_a_job()
    if not job:
        print("No job to execute")
        sleep(5)
    else:
        update_job_status(job["id"], STATUS_INPROGRESS)
        execute_job(job)
        update_job_status(job["id"], STATUS_COMPLETED)
    execute()


def main():
    #init_db.initialize_db_from_file(SQL_SCRIPT)
    execute()


if __name__ == '__main__':
    main()
