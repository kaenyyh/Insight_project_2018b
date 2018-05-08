from kafka import KafkaProducer, KafkaConsumer
import time, sys
from utils_function import parse_header
from time import gmtime, strftime


def main():
    producer = KafkaProducer(bootstrap_servers='ec2-34-213-54-16.us-west-2.compute.amazonaws.com:9092', key_serializer=str.encode, value_serializer=str.encode)

    count = 0
    with open(input.txt) as f:
        for line in f:
            curtime = strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            new_line = line + curtime
            producer.send("ctest", key=str(count), value=new_line)
            count += 1
    f.close()

if __name__ == "__main__":
    main()
