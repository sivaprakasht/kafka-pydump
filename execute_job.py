import sqlite3
from time import sleep

import init_db
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
        return {"id": job[0], "objects": job[1].split(','), "count": job[2], "kafka_host": job[3],
                "zookeeper_host": job[4],
                "topic": job[5]}
    return None


def update_job_status(id, status):
    conn = sqlite3.connect(DATABASE)
    conn.execute('update job set STATUS = ? where id = ?', (status, id))
    conn.commit()
    conn.close()


def execute_job(job):
    print("Executing job")
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
    init_db.initialize_db_from_file(SQL_SCRIPT)
    execute()


if __name__ == '__main__':
    main()
