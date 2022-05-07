
from ksql import KSQLAPI
from ksql.errors import KSQLError
from contextlib import suppress
from operator import itemgetter
from kafka import KafkaProducer
from time import sleep
from os import getenv
from tcp_latency import measure_latency
from random import randint

import logging
import coloredlogs
import json
import socket
coloredlogs.install()


# CONSTANTs

CPU_LOAD_THRESHOLD = 0.7
MEM_AVAILABLE_MIN = 1500 * 1024 * 1024
CENTER_SERVER = getenv("KAFKA_BROKER_CENTER_SERVER")
CENTER_SERVER_PORT = 8088
MIN_CPU_LOAD = 0.5
MAX_CPU_LOAD = 60.0
MAX_RAM_CAP = 1500
MIN_LATENCY = 10
CENTER_SERVER_CONFLUENT_CONN_STRING = getenv(
    'CENTER_SERVER_CONFLUENT_KSQL_CONN_STRING')
# CENTER_SERVER_CONFLUENT_CONN_STRING = "http://192.168.1.60:8088"
##
KAFKA_SERVER = '192.168.1.60'
KAFKA_PORT = '9092'
TOPIC = 'location-strategy-center-server'
##
LATENCY_FIRST = 'latency'
CPU_AVGLOAD_FIRST = 'cpu_avgload'
RAM_CAP_FIRST = 'ram_cap'

# incase of .env error, use these manually, need some automation mechanism later
'''
CPU_LOAD_THRESHOLD      = 0.7
MEM_AVAILABLE_MIN       = 500 * 1024 * 1024
CENTER_SERVER           = "192.168.1.60"
CENTER_SERVER_PORT      = 8088
MIN_CPU_LOAD            = 0.5
MAX_CPU_LOAD            = 60.0
MAX_RAM_CAP             = 1000
MIN_LATENCY             = 50
CENTER_SERVER_CONFLUENT_CONN_STRING = "http://192.168.1.60:8088"
##
KAFKA_SERVER            = '192.168.1.60'
KAFKA_PORT              = '9092'
TOPIC                   = 'location-strategy-center-server'
##
LATENCY_FIRST           = "latency"
CPU_AVGLOAD_FIRST       = "cpu_avgload"
RAM_CAP_FIRST           = "ram_cap"
'''


'''
    select the available node to offload:
        - get list of current available nodes via Kafka
        - select
        - publish some info to kafka

'''


class LocationStrategy:
    def __init__(self,
                 center_server_conn_string=CENTER_SERVER_CONFLUENT_CONN_STRING,
                 kafka_server=KAFKA_SERVER,
                 kafka_port=KAFKA_PORT,
                 kafka_topic=TOPIC):
        super().__init__()

        self.kafkaProducer = KafkaProducer(
            bootstrap_servers=kafka_server + ":" + kafka_port
        )
        self.ksqlClient = KSQLAPI(center_server_conn_string)
        logging.basicConfig(level=logging.INFO)
        self.candidates = []
        self.lowestLatencyCandidates = []
        self.highestRAMCapCandidates = []
        self.lowestCPUAvgLoadCandidates = []
    '''
        at the very first level, just use a predefined list of possible candidates
    '''

    def create_ksql_stream(self):
        try:
            create_stream = self.ksqlClient.ksql('''
                                        drop stream information_strategy_center_server;
                                        create stream if not exists information_strategy_center_server (
                                            src_server_private_ip varchar,
                                            mac_addr varchar,
                                            hostname varchar,
                                            cpu_avgload double,
                                            mem_free int,
                                            network_latency double,
                                            src_server_public_ip varchar)
                                        with (kafka_topic='information-strategy-center-server',format='json'    )
                                        ''')
        except KSQLError as e:
            logging.error("sSTH went wrong with KSQL : %r", e)
            return -1
        pass

    '''
        @deprecated
        just get a list of possible candidates via .txt file
    '''

    def get_all_possible_candidates(self):
        candidate_list = []
        with open('node-candidates.txt', 'r') as candidates_file:
            for each in candidates_file.readlines():
                candidate_list.append(each)
        self.candidates = candidate_list

    '''
        query the candidate which has LOWEST latency DIRECTLY from the KSQL stream on Center server
        save the list on self.lowestLatencyCandidates
        @return [
            {private_ip,mac_addr,hostname,cpu_avgload,mem_free,network_latency,public_ip}
            ]
        TODO: query for the number of nodes that currently publishing to information strategy topic then adjust the emitting changes in the stream
    '''

    def query_candidates_with_low_latency(self, preferred_latency=MIN_LATENCY, num_of_publishing_nodes=5):
        max_cpu_cap = MAX_CPU_LOAD
        max_ram_cap = 100
        min_latency = preferred_latency

        # reset candidates list
        self.lowestLatencyCandidates = []
        self.candidates = []
        query_for_IS = "select * from information_strategy_center_server emit changes limit " + \
            str(num_of_publishing_nodes)
        query = self.ksqlClient.query(query_for_IS)

        already_added_candidates = []

        # need better try-catch logic....
        # with suppress(RuntimeError):
        try:
            for records in query:
                # logging.info(records)
                if "row" in records:
                    # print("processing : ", records[0:-2])
                    r = json.loads(records[0:-2])

                    # if r['row']['columns'][5] < min_latency:
                    #     min_latency = r['row']['columns'][5]
                    if r['row']['columns'][3] > max_cpu_cap:
                        max_cpu_cap = r['row']['columns'][3]
                    if r['row']['columns'][4] > max_ram_cap:
                        max_ram_cap = r['row']['columns'][4]

                    private_ip = r['row']['columns'][0]
                    mac_addr = r['row']['columns'][1]
                    cpu_avg_load = r['row']['columns'][3]
                    ram_cap = r['row']['columns'][4]
                    net_latency = r['row']['columns'][5]
                    public_ip = r['row']['columns'][6]

                    self.candidates.append([r['row']['columns'][2], (
                            private_ip,  # 0
                            mac_addr,  # 1
                            cpu_avg_load,  # 2
                            ram_cap,  # 3
                            net_latency,  # 4,
                            public_ip  # 5
                        )])
        except RuntimeError:
            logging.error("done querying stream KSQL")
            pass
            #     logging.error("KSQLDB timed out...... SADLY")
            #     self.candidates = []
        except socket.timeout:
            logging.error("socket error")

        for each in self.candidates:
            try:
                # logging.info(each[1][0])
                current_latencee = measure_latency(host=each[1][0],port=9092)[0] #currently  use private IP
                logging.info("latency from  here to -> %r: %r" %
                             (each[1][0], current_latencee))
                # logging.info(each[0])
                # logging.info(min_latency)
                if(current_latencee <= min_latency and each[0] not in already_added_candidates):
                    self.lowestLatencyCandidates.append(each)
                    already_added_candidates.append(each[0])
                    # logging.info("possible candidates with low network latency: %r " % (
                    #     [x for x in self.lowestLatencyCandidates]))
            except TypeError as e:
                logging.error("having trouble with pinging command")
                logging.error(e)
        return self.lowestLatencyCandidates

    def select_the_lowest_latency(self):
        return self.lowestLatencyCandidates[randint(0, len(self.lowestLatencyCandidates)-1)]

    '''
        - SELECT the candiate which has the required computing cap (RAM) from the queried list (lowest latency)
        @return[
            {private_ip,mac_addr,hostname,cpu_avgload,mem_free,network_latency}
        ]
    '''

    def select_candidates_by_RAM_cap(self, required_ram_cap=MAX_RAM_CAP):
        potential_nodes = []
        for each_candidates in self.lowestLatencyCandidates:
            if(each_candidates[1][3] >= required_ram_cap):
                potential_nodes.append(each_candidates)
        self.highestRAMCapCandidates = potential_nodes
        # print("done filtering by RAM cap : ", self.highestRAMCapCandidates)
        return potential_nodes

    '''
        - SELECT the candiate which has the required computing cap (lowestCPU avg load / 1min) from the queried list
        @return[
            {private_ip,mac_addr,hostname,cpu_avgload,mem_free,network_latency}
        ]
    '''

    def select_candidates_by_CPU_avg(self, required_maximum_cpu_avg_load_1_min=MAX_CPU_LOAD, minimum_cpu_load_1_min=MIN_CPU_LOAD):
        potential_nodes = []
        for each_candidates in self.candidates:
            if(each_candidates[1][2] <= required_maximum_cpu_avg_load_1_min and each_candidates[1][2] >= MIN_CPU_LOAD):
                potential_nodes.append(each_candidates)
        self.lowestCPUAvgLoadCandidates = potential_nodes
        # print("done filtering by CPU avgload", self.lowestCPUAvgLoadCandidates)
        return potential_nodes

    '''
        notify each others about possible candidates according to specified criteria
    '''

    def publish_lowest_latency_candidates_to_kafka(self, topic=TOPIC):
        data = json.dumps(self.lowestLatencyCandidates).encode('utf-8')
        key = json.dumps("net_latency").encode('utf-8')
        self.kafkaProducer.send(topic, key=key, value=data)
        pass

    def publish_highest_RAM_cap_candidates_to_kafka(self, topic=TOPIC):
        data = json.dumps(self.highestRAMCapCandidates).encode('utf-8')
        key = json.dumps("ram_cap").encode('utf-8')
        self.kafkaProducer.send(topic, key=key, value=data)
        pass

    def publish_lowest_CPU_avgload_candidates_to_kafka(self, topic=TOPIC):
        data = json.dumps(self.lowestCPUAvgLoadCandidates).encode('utf-8')
        key = json.dumps("cpu_avgload").encode('utf-8')
        self.kafkaProducer.send(topic, key=key, value=data)
        pass

    '''
        DECISION: select list of nodes for offloading by latency and ram cap > 1000 Mb
        @return [
            {ip_addr,mac_addr,hostname}
        ]
    '''

    def select_candidates_to_offload(self, selection_criteria=LATENCY_FIRST):
        if(selection_criteria == LATENCY_FIRST):
            # print("you chose the one with lowest latency")
            max_ram_available = 1000
            selection = []
            logging.info(self.lowestLatencyCandidates)
            for each_candidate in self.lowestLatencyCandidates:
                if(each_candidate[1][3] > max_ram_available):
                    selection.append(each_candidate)
            return selection
        # elif(selection_criteria == CPU_AVGLOAD_FIRST):
        #     # print("you chose the one with  lowest load")
        #     return self.lowestCPUAvgLoadCandidates
        # else:
        #     # print("you chose the biggest dick")
        #     return self.highestRAMCapCandidates

    '''
        TODO: select_candidate_via_2_criterias

    '''

    '''

    '''

# observing


def main():

    logging.info(CENTER_SERVER_CONFLUENT_CONN_STRING)
    locationStrategy = LocationStrategy(CENTER_SERVER_CONFLUENT_CONN_STRING)
    logging.info(locationStrategy)
    locationStrategy.create_ksql_stream()
    # locationStrategy.get_all_possible_candidates()

    while True:
        locationStrategy.query_candidates_with_low_latency()
        locationStrategy.select_candidates_by_RAM_cap()
        locationStrategy.select_candidates_by_CPU_avg()
        # logging.info("wow i  found the most compatible node with lowest latency\n... %r"%(locationStrategy.select_the_lowest_latency()))
        # logging.info("otherwise we have the RAM maniac: %r"%(locationStrategy.select_candidates_by_RAM_cap()))
        # logging.info("otherwise we have the CPU maniac: %r"%(locationStrategy.select_candidates_by_CPU_avg()))
        # print("final candidates list: ",locationStrategy.lowestLatencyCandidates)
        # locationStrategy.publish_lowest_latency_candidates_to_kafka()
        # locationStrategy.publish_lowest_CPU_avgload_candidates_to_kafka()
        # locationStrategy.publish_highest_RAM_cap_candidates_to_kafka()
        # print("done publish to kafka...")

        the_chosen_one = locationStrategy.select_candidates_to_offload()
        # print(the_chosen_one)
        logging.info("I am the chosen one %r" % (the_chosen_one))
        # sleep(1)
        # print(chr(27)+'[2j')
        # print('\033c')
        # print('\x1bc')
        # sleep(0.25)


if __name__ == "__main__":
    main()
