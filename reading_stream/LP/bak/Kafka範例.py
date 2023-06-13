#!/usr/bin/ python3
import json
import sys

from confluent_kafka import Consumer, Producer

if __name__ == "__main__":
    exitcode = 0
    try:
        p = Producer({"bootstrap.servers": "172.31.48.34:9092"})
        # p = Producer({'bootstrap.servers':'localhost:9092'})

        seq = 1
        testdata = {"id": seq, "data": "amazon s3 " + str(seq)}
        topics = json.dumps(testdata)
        print(topics)

        p.produce("lp_raw", topics.encode("utf-8"), partition=1)
        p.flush()
        p.poll(0)

    # except KafkaTimeoutError as e1:
    #     exitcode = 1
    #     print(e1)
    except Exception as e2:
        exitcode = 1
        print(e2)
    finally:
        sys.exit(exitcode)
