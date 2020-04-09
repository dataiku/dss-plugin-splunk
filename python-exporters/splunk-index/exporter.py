from dataiku.exporter import Exporter
import re
import json
import logging
from splunklib.binding import connect
import splunklib.client as client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='splunk plugin %(levelname)s - %(message)s')


class SplunkIndexExporter(Exporter):

    DEFAULT_SPLUNK_PORT = "8089"
    ISO_8601_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%Q"
    EPOCH_TIME_FORMAT = "%s.%Q"

    def __init__(self, config, plugin_config):
        """
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        logger.info("SplunkIndexExporter:Init")
        self.config = config
        self.plugin_config = plugin_config
        self.splunk_socket = None
        self.dataset_schema = None
        self.row_index = 0
        try:
            self.splunk_instance = config.get('splunk_login')['splunk_instance']
            self.parse_url()
            self.splunk_username = config.get('splunk_login')['splunk_username']
            self.splunk_password = config.get('splunk_login')['splunk_password']
        except Exception as err:
            raise Exception("The Splunk instance URL or login details are not filled in. ({})".format(err))
        self.splunk_app = config.get('splunk_app')
        self.index_name = config.get('index_name').lower()
        self.search_string = ""
        self.splunk_sourcetype = config.get('splunk_sourcetype')
        self.source_host = config.get("source_host", "dss")
        self.overwrite_existing_index = config.get('overwrite_existing_index', False)
        args = {
            "host": self.splunk_host,
            "port": self.splunk_port,
            "username": self.splunk_username,
            "password": self.splunk_password
        }

        if not self.splunk_app == "":
            args["app"] = self.splunk_app

        self.client = connect(**args)
        logger.info("SplunkIndexExporter:Connected to Splunk")
        self.authorization_token = self.client.token

    def parse_url(self):
        regex = '(?:http.*://)?(?P<host>[^:/ ]+).?(?P<port>[0-9]*).*'
        groups = re.search(regex, self.splunk_instance)
        self.splunk_port = groups.group('port')
        if self.splunk_port == "" or self.splunk_port is None:
            self.splunk_port = self.DEFAULT_SPLUNK_PORT
        self.splunk_host = groups.group('host')

    def open(self, schema):
        self.dataset_schema = schema
        service = client.connect(
            host=self.splunk_host,
            port=self.splunk_port,
            username=self.splunk_username,
            password=self.splunk_password
        )
        try:
            if self.overwrite_existing_index:
                service.indexes.delete(self.index_name)
        except Exception as Err:
            logger.info('deleting error={}'.format(Err))
        try:
            myindex = service.indexes[self.index_name]
        except Exception as Err:
            logging.info("Creating indexe following {}".format(Err))
            myindex = service.indexes.create(self.index_name)
        self.splunk_socket = myindex.attach(sourcetype=self.splunk_sourcetype, host=self.source_host)

    def open_to_file(self, schema, destination_file_path):
        """
        Start exporting. Only called for exporters with behavior OUTPUT_TO_FILE
        :param schema: the column names and types of the data that will be streamed
                       in the write_row() calls
        :param destination_file_path: the path where the exported data should be put
        """
        raise Exception("Unimplemented")

    def write_row(self, row):
        self._send_row(row, self.splunk_socket)

    def _send_row(self, row, splunk_socket):
        event = {}
        for value, schema in zip(row, self.dataset_schema["columns"]):
            column_name = schema["name"]
            event[column_name] = value
        event.pop("_raw", None)
        if self.splunk_sourcetype == "_json":
            event_string = json.dumps(event) + '\r\n'
        else:
            event_string = self._generate_event_string(event) + '\r\n'
        splunk_socket.send(event_string.encode())

    def _generate_event_string(self, event):
        elements = []
        for element in event:
            elements.append(element + "=" + str(json.dumps(event[element])))
        return " ".join(elements)

    def close(self):
        """
        Perform any necessary cleanup
        """
        self.splunk_socket.close()
