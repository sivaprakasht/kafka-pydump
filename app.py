import sqlite3

from config import DATABASE
from flask import Flask, request
from flask_restful import Api, Resource

app = Flask(__name__)
api = Api(app)


class Job(Resource):
    def get(self):
        conn = sqlite3.connect(DATABASE)
        cursor = conn.execute('select * from job')
        jobs = cursor.fetchall()
        job_objects = []
        for job in jobs:
            job_object = {"id": job[0], "objects": job[1], "count": job[2], "kafka_host": job[3],
                          "zookeeper_host": job[4], "topics": job[5], "status": job[6], "name": job[7],
                          "description": job[8]}
            job_objects.append(job_object)
        conn.close()
        return job_objects, 200

    def post(self):
        job = request.json
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        try:
            cursor.execute(
                'insert into job (OBJECTS,COUNT,KAFKA_HOST,ZOOKEEPER_HOST,TOPICS,STATUS,NAME,DESCRIPTION) values (?,?,?,?,?,?,?,?)',
                (job['objects'], job['count'], job['kafka_host'], job['zookeeper_host'], job['topics'], 'NEW',
                 job['name'], job['description']))
            id = cursor.lastrowid;
        except KeyError as keyError:
            return str(keyError) + 'is missing/misspelled in the request', 400
        finally:
            conn.commit()
            conn.close()

        return id, 201


class Config(Resource):
    def post(self):
        configs = request.json
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        count = 0
        try:
            for config in configs:
                cursor.execute('insert into config (KEY,VALUE) values (?,?)', (config, str(configs[config])))
            conn.commit()
            count += 1
        except sqlite3.IntegrityError as integrityError:
            return str(integrityError), 500
        finally:
            conn.close()
        return str(count) + " new Configs inserted", 201


api.add_resource(Job, "/jobs")
api.add_resource(Config, "/configs")
app.run(host='0.0.0.0', port=80)
