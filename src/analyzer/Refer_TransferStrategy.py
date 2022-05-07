from psutil import cpu_count, cpu_freq, cpu_percent, getloadavg
from psutil import virtual_memory
from time import sleep
from hashlib import sha256
from kafka import KafkaProducer,KafkaConsumer
from kafka.errors import KafkaTimeoutError,NoBrokersAvailable

import logging
import coloredlogs
# from InformationStrategy import InformationStrategy

'''
    Transfer strategy determine these things:
        - when to offloading
        - which job to offloading
        - how to offloading:
            - communication mechanism
            - 
    notify each other (via kafka) when transfering
'''

RAM_CAP_AVAILABLE = 10000 * 1024 * 1024
CPU_AVG_LOAD = 70.0
MEGABYTES = 1024 * 1024
OFFLOAD_TASKS_TOPIC = "off-load-tasks"
OFFLOAD_RESULT_TOPIC = "off-load-results"
LOCAL_BROKER = "192.168.1.49:9092"
REMOTE_BROKER = "192.168.1.60:9092"
LOCAL_IP = "192.168.1.49"
REMOTE_IP = "192.168.1.60"


class TransferStrategy():
    def __init__(self):
        super().__init__()
        self.src_ip = '127.0.0.1'
        self.dst_ip = '127.0.0.1'
        self.kafkaProducer = None
        self.kafkaConsumer = None

    '''
        DETERMINE when to trigger offloading jobs according to several condition:
            - cpu_avg_load
            - ram_cap
        return 
    '''

    def set_src_ip(self,src_ip):
        self.src_ip = src_ip
        logging.info("DONE setting src_ip to: %r"%(self.src_ip))
    def set_dst_ip(self,dst_ip):
        self.dst_ip = dst_ip
        logging.info("DONE setting dst_ip to: %r"%(self.dst_ip))
    def get_dst_ip(self):
        return self.dst_ip
    
    # adding try-catch plz
    def initKafkaProducer(self,boot_strap_server_conn_string):
        try:
            self.kafkaProducer = KafkaProducer(
                bootstrap_servers=boot_strap_server_conn_string,
                max_request_size=2048000,
                # compression_type='lz4'
                )
            logging.info("DONE setting up Kafka Producer")
        except NoBrokersAvailable:
            logging.error("NO remote broker available, use local broker")
            #masturbating ? 
            self.kafkaProducer = KafkaProducer(
                bootstrap_servers="localhost:9092",
                max_request_size=2048000,
                # compression_type='lz4'
            )

    def initKafkaConsumer(self,bootstrap_server_conn_string):
        self.kafkaConsumer = KafkaConsumer(
            bootstrap_servers=bootstrap_server_conn_string,
            request_timeout_ms = 2000
            )
        logging.info("DONE setting up kafka consumer")

    def trigger_offloading_based_on_ram_cap_available(self, threshold=RAM_CAP_AVAILABLE):
        if(virtual_memory()[1] < RAM_CAP_AVAILABLE):
            logging.warning("avail_mem: %r"%(virtual_memory()[1]))
            return True
        return False

    def trigger_offloading_based_on_cpu_avgload(self, threshold=CPU_AVG_LOAD):
        return True if getloadavg()[1] > CPU_AVG_LOAD else False

    def crafting_data_to_offload(self, data):
        logging.info("Hey im an innocent one %d"%(len(data)))
        sha256().update(data)
        flow_hash = sha256().digest()
        header = [
            ("src", bytes(self.src_ip,'utf-8')),
            ("flow_hash", flow_hash)
        ]
        value = data
        return(header, value)

    def start_offloading(self, data):
        try:
            crafted_data = self.crafting_data_to_offload(data)
            self.kafkaProducer.send(
                topic=OFFLOAD_TASKS_TOPIC,
                headers=crafted_data[0],
                value=crafted_data[1]
            )
        # saving something to DB to keep track of records
            logging.info("DONE submitted data to %r" % (self.dst_ip))
            logging.info(
                "you should try save this data into some kind of db\
                    before receiving your prediction results")            
        except KafkaTimeoutError:
            logging.error("CANNOT connect to %r"%(self.dst_ip))
            pass

    '''
        should call this subscribe function before offloading ?? 
    '''

    def subscribe_to_result(self,topic_name):
        self.kafkaConsumer.subscribe(topic_name)

        for msg in self.kafkaConsumer:
            if(msg.headers[0][1].decode('utf-8') == self.src_ip):
                # logging.info("from %r with love"%())
                logging.info('this is the flow hash:')
                logging.info(msg.headers[1][1])
                logging.info("this is the predicted result:")
                logging.info(msg.value.decode('utf-8'))
                logging.info("===THANK YOU===")
                self.kafkaConsumer.unsubscribe()
                logging.info("===DONE SUBSCRIBING===")
                return msg.value.decode('utf-8')
        pass

    def terminate_producer(self):
        self.kafkaProducer.close()


def main():

    while True:
        transferStrategy = TransferStrategy()
        print(transferStrategy.trigger_offloading_based_on_cpu_avgload())
        print(transferStrategy.trigger_offloading_based_on_ram_cap_available())
        sleep(0.5)


if __name__ == "__main__":
    main()
