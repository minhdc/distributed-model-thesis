# import pytest
from os import getenv

from InformationStrategy import InformationStrategy

'''
    exploratory tests
'''
informationStrategy = InformationStrategy()
informationStrategy.get_hostname()
informationStrategy.do_monitor_system_resource_and_publish_to_kafka()

'''
    TODO: write unit tests
'''