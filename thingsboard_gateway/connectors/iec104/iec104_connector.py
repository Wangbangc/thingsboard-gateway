# thingsboard-gateway/extensions/my_custom_connector/my_custom_connector.py

import logging
from thingsboard_gateway.connectors.connector import Connector  # 导入基类


class iecConnector(Connector):
    def __init__(self, gateway, config, connector_name):
        self._gateway = gateway
        self._config = config
        self._connector_name = connector_name
        self.__log = self._gateway.get_logger(connector_name)  # 获取日志器
        self.__log.info("初始化我的连接器 Config: %s", config)
        self._connected = False

    def open(self):
        self.__log.info("Simulating connection establishment...")
        # 你的连接逻辑 here
        self._connected = True

    def close(self):
        self.__log.info("Simulating closing connection...")
        # 你的断开逻辑 here
        self._connected = False

    def get_name(self):
        return self._connector_name

    def is_connected(self):
        return self._connected

    def on_attributes_update(self, content):
        self.__log.info("Received attribute update: %s", content)
        # 处理来自平台的属性下行消息