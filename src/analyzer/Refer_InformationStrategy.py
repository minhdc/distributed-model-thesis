# -*- coding: utf-8 -*-
from psutil import cpu_count, cpu_freq, cpu_percent, getloadavg
from psutil import virtual_memory
from time import sleep
from os import system, getenv
from tcp_latency import measure_latency
import urllib.request
import netifaces as ni
from socket import gethostname
from getmac import get_mac_address as gma
from kafka import KafkaProducer

import json
import platform
import logging
import coloredlogs

coloredlogs.install()

# CONST
MEGABYTES = 1024.0 * 1024.0
MEGAHERTZ = 1024
# refactor as _center_server
# KAFKA_SERVER = getenv("KAFKA_BROKER_CENTER_SERVER")
# KAFKA_PORT = getenv("KAFKA_PORT_CENTER_SERVER")  # refactor as _center_server
# TOPIC = getenv('TOPIC_LOCATION_STRATEGY_CENTER_SERVER')
KAFKA_BROKER_CENTER_SERVER    = '192.168.1.60'
KAFKA_PORT_CENTER_SERVER      = '9092'
KAFKA_SERVER = '192.168.1.60'
KAFKA_PORT = '9092'
TOPIC = 'information-strategy-center-server'
TOPIC_INFORMATION_STRATEGY_CENTER_SERVER = 'information-strategy-center-server'
# if failed using following, need some automation later
'''
MEGABYTES       = 1048576.0
MEGAHERTZ       = 1024.0
KAFKA_BROKER_CENTER_SERVER    = '192.168.1.60'
KAFKA_PORT_CENTER_SERVER      = '9092'
TOPIC_INFORMATION_STRATEGY_CENTER_SERVER = 'information-strategy-center-server'
'''

'''
    collect resource information + status of the current node
'''


class InformationStrategy:
    def __init__(self, hostname=gethostname(),
                 src_server=None,
                 dst_server=KAFKA_SERVER,
                 dst_port=KAFKA_PORT,
                 topic=TOPIC):

        super().__init__()
        logging.info(dst_server)
        logging.info(dst_port)
        self.cpu_count = cpu_count()
        self.cpu_freq = cpu_freq()
        self.cpu_percent = cpu_percent()
        self.cpu_getloadavg = getloadavg()

        self.memory_stat = virtual_memory()

        self.hostname = hostname
        self.mac_addr = None
        self.src_server_public_ip = src_server
        self.src_server_private_ip = src_server
        self.latency = 0.0  # milliseconds

        self.producer = KafkaProducer(
            bootstrap_servers=dst_server+':'+dst_port)
        self.topic = topic

    '''
        @return: host name
    '''

    def get_hostname(self):
        self.hostname = gethostname()
        return self.hostname

    '''
        @return: list of all private IPs from all NICs
    '''

    def get_private_ip(self):
        private_nic_ip_addresses = []
        for each_ip in ni.interfaces():
            try:
                ip = ni.ifaddresses(each_ip)[ni.AF_INET][0]['addr']
                private_nic_ip_addresses.append(ip)
            except:
                pass
        self.src_server_private_ip = private_nic_ip_addresses[1]

    '''
        @return: public IP addr
    '''

    def get_external_ip(self):
        # need another suggestions/source of public IP check
        try:
            self.src_server_public_ip = urllib.request.urlopen(
                'https://ident.me').read().decode('utf-8')
            # logging.info("current public IP:")
            # logging.info(self.src_server_public_ip)
        except:
            pass

    '''
        @return: TCP ping value from src_host to dst_host
    '''

    def get_latency(self, ip='127.0.0.1', wait=0):
        return measure_latency(host=ip, wait=wait)[0]

    '''
        @return: total RAM capacity
    '''

    def get_total_mem(self):
        return self.memory_stat[0]

    '''
        @return: available RAM capacity
    '''

    def get_available_mem(self):
        return self.memory_stat[1]

    '''
        need another suggestions for getting mac_addr instead of using 3rd party lib
        @return mac_addr
    '''

    def get_mac_addr(self):
        self.mac_addr = gma()

    '''
        publish resource information of current node to the broker
    '''

    def publish_node_info_to_broker(self, topic, data=None):
        return self.producer.send(topic, data)

    def do_monitor_system_resource_and_publish_to_kafka(self):
        system_metrics = {}
        while True:
            system_metrics['mac_addr'] = self.mac_addr
            system_metrics['hostname'] = self.hostname
            system_metrics['src_server_public_ip'] = self.src_server_public_ip
            system_metrics['src_server_private_ip'] = self.src_server_private_ip
            system_metrics['cpu_counts'] = self.cpu_count
            system_metrics['cpu_freq'] = self.cpu_freq[0]/MEGAHERTZ
            system_metrics['cpu_percent'] = self.cpu_percent
            system_metrics['cpu_avgload'] = self.cpu_getloadavg[1]
            # only available RAM
            system_metrics['mem_free'] = self.memory_stat[1]/MEGABYTES
            system_metrics['network_latency'] = self.get_latency(
                ip=KAFKA_SERVER, wait=0)

            # PUBLISH TO BROKER for each 5 secs
            # logging.info("publishing to kafka broker: ")
            # logging.info(system_metrics)
            # logging.info(self.publish_node_info_to_broker(
            #     topic=TOPIC, data=json.dumps(system_metrics).encode('utf-8')))
            sleep(0.05)
        pass


def main():
    informationStrategy = InformationStrategy(src_server=gethostname())
    system_metrics = {}
    while True:
        # GET SYSTEM METRICS
        informationStrategy.get_external_ip()
        informationStrategy.get_private_ip()
        informationStrategy.get_hostname()
        informationStrategy.get_mac_addr()

        system_metrics['mac_addr'] = informationStrategy.mac_addr
        system_metrics['hostname'] = informationStrategy.hostname
        system_metrics['src_server_public_ip'] = informationStrategy.src_server_public_ip
        system_metrics['src_server_private_ip'] = informationStrategy.src_server_private_ip
        system_metrics['cpu_counts'] = informationStrategy.cpu_count
        system_metrics['cpu_freq'] = informationStrategy.cpu_freq[0]/MEGAHERTZ
        system_metrics['cpu_percent'] = informationStrategy.cpu_percent
        system_metrics['cpu_avgload'] = informationStrategy.cpu_getloadavg[1]
        # only available RAM
        system_metrics['mem_free'] = informationStrategy.memory_stat[1]/MEGABYTES
        system_metrics['network_latency'] = informationStrategy.get_latency(
            ip=KAFKA_SERVER, wait=0)

        # PUBLISH TO BROKER
        print("publishing to kafka broker: ", informationStrategy.publish_node_info_to_broker(
            topic=TOPIC, data=json.dumps(system_metrics).encode('utf-8')))

        # CLEAR STDOUT DEBUG SCREEN
        # logging later
        sleep(0.05)
        print(chr(27)+'[2j')
        print('\033c')
        print('\x1bc')


if __name__ == "__main__":
    main()
