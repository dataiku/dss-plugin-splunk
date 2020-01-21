from six.moves import xrange
from dataiku.connector import Connector
from splunklib.binding import connect
import json, re

class SplunkIndexConnector(Connector):
    DEFAULT_SPLUNK_PORT = "8089"
    ISO_8601_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%Q"
    EPOCH_TIME_FORMAT = "%s.%Q"

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        self.splunk_instance = config.get('splunk_login')['splunk_instance']
        self.parse_url()
        self.splunk_username = config.get('splunk_login')['splunk_username']
        self.splunk_password = config.get('splunk_login')['splunk_password']
        self.index_name = config.get('index_name')
        self.search_string = config.get('search_string')
        self.earliest_time = config.get('earliest_time')
        self.latest_time = config.get('latest_time')
        if len(self.earliest_time) == 0:
            self.earliest_time = None
        if len(self.latest_time) == 0:
            self.latest_time = None
        print('ALX:init:earliest_time={}, latest_time={}'.format(self.earliest_time, self.latest_time))
        self.client = connect(
            host = self.splunk_host,
            port = self.splunk_port,
            username =self.splunk_username, 
            password = self.splunk_password
        )

    def parse_url(self):
        regex = '(?:http.*://)?(?P<host>[^:/ ]+).?(?P<port>[0-9]*).*'
        groups = re.search(regex, self.splunk_instance)
        self.splunk_port = groups.group('port')
        if self.splunk_port is "" or self.splunk_port is None or len(self.splunk_port)==0:
            self.splunk_port = self.DEFAULT_SPLUNK_PORT
        self.splunk_host = groups.group('host')


    def get_read_schema(self):
        # In this example, we don't specify a schema here, so DSS will infer the schema
        # from the columns actually returned by the generate_rows method
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):

        args = {
            'search':"search {} index={}".format(self.search_string, self.index_name), 
            'output_mode':"json",
            'timeout':60,
            'time_format':self.ISO_8601_TIME_FORMAT,
            'count':0
        }
        if self.earliest_time is not None:
            args['earliest_time'] = self.earliest_time
        if self.latest_time is not None:
            args['latest_time'] = self.latest_time

        splunk_response = self.client.get('search/jobs/export', **args)
        while True:
            content = splunk_response.body.read()
            if len(content) == 0:
                break
            for sample in content.split("\n"):
                if sample == "":
                    continue
                json_sample = json.loads(sample)
                if "result" in json_sample:
                    yield json_sample["result"]
                else:
                    break


    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):
        """
        Returns a writer object to write in the dataset (or in a partition).

        The dataset_schema given here will match the the rows given to the writer below.

        Note: the writer is responsible for clearing the partition, if relevant.
        """
        raise Exception("Unimplemented")


    def get_partitioning(self):
        """
        Return the partitioning schema that the connector defines.
        """
        raise Exception("Unimplemented")


    def list_partitions(self, partitioning):
        """Return the list of partitions for the partitioning scheme
        passed as parameter"""
        return []


    def partition_exists(self, partitioning, partition_id):
        """Return whether the partition passed as parameter exists

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise Exception("unimplemented")


    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise Exception("unimplemented")


class SplunkDatasetWriter(object):
    def __init__(self):
        pass

    def write_row(self, row):
        """
        Row is a tuple with N + 1 elements matching the schema passed to get_writer.
        The last element is a dict of columns not found in the schema
        """
        raise Exception("unimplemented")

    def close(self):
        pass
