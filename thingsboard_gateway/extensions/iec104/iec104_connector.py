#     Copyright 2025. ThingsBoard
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.tb_utility.tb_logger import init_logger
from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService  # 用于类型提示和访问 Gateway 方法
from thingsboard_gateway.gateway.constants import CONNECTOR_PARAMETER, DEVICE_SECTION_PARAMETER, DATA_PARAMETER, \
    RPC_METHOD_PARAMETER, RPC_PARAMS_PARAMETER

from threading import Thread
import time  # 用于 run 方法中的 sleep


class iecConnector(Connector, Thread):
    def __init__(self, gateway: TBGatewayService, config: dict, connector_name: str):
        super().__init__()  # 根据官方Modbus连接器，这里不传递参数

        self.__gateway = gateway  # 存储 Gateway 实例
        self._config = config  # 存储连接器配置
        self._connector_name = connector_name  # 存储连接器名称

        # 使用 tb_utility.tb_logger.init_logger 初始化日志
        self.log = init_logger(self.__gateway, connector_name,
                               config.get('logLevel', 'INFO'),
                               enable_remote_logging=config.get('enableRemoteLogging', False),
                               is_connector_logger=True)

        self.log.debug("Initializing iecConnector: %s", connector_name)

        self._connected = False  # 标记连接器是否与外部设备连接
        self._stopped = False  # 标记连接器线程是否停止
        self.daemon = True  # 将线程设置为守护线程，随主程序退出而退出

    # 以下是 Connector 基类要求实现的方法

    def get_id(self):
        """返回连接器的唯一标识。使用连接器名称作为简易ID。"""
        return self._connector_name  # 简化，假设名称是唯一的

    def get_name(self):
        """返回连接器的名称。"""
        return self._connector_name

    def get_type(self):
        """返回连接器的类型。"""
        return "IEC104"  # 根据你的协议类型定义

    def get_config(self):
        """返回连接器的配置字典。"""
        return self._config

    def is_connected(self):
        """返回连接器是否与外部设备保持连接。"""
        return self._connected

    def is_stopped(self):
        """返回连接器内部处理循环是否已停止。"""
        return self._stopped

    def open(self):
        """
        启动连接器（通常是启动其内部线程）。
        这将调用 Thread 的 start() 方法，进而执行 run() 方法。
        """
        self.log.info("Opening IEC connector and starting its thread.")
        self._stopped = False
        self.start()  # 启动 run 方法
        self.log.debug("IEC connector thread started successfully.")

    def close(self):
        """
        停止连接器线程并清理资源。
        设置停止标志，等待线程结束，并标记为未连接。
        """
        self.log.info("Closing IEC connector and stopping its thread.")
        self._stopped = True  # 通知 run 方法停止
        self._connected = False  # 标记为未连接

        # 尝试优雅地停止线程
        if self.is_alive():
            self.join(timeout=5)  # 最多等待5秒让线程自行结束
            if self.is_alive():
                self.log.warning("IEC connector thread did not terminate gracefully within 5 seconds.")

        self.log.info("IEC connector thread stopped and resources cleaned.")
        self.log.stop()  # 关闭日志处理器，释放文件句柄等

    def run(self):
        """
        连接器的主运行循环。
        这个方法在调用 open() 后由 Thread 自动执行。
        在这里放置连接、数据采集和发送的周期性逻辑。
        """
        self.log.info("IEC connector thread '%s' has started its main loop.", self._connector_name)

        # 简化连接模拟
        # 在实际应用中，这里会是你的IEC104客户端的连接代码
        self.log.info("Simulating connection to IEC device...")
        # 假设连接成功，或者至少尝试连接
        time.sleep(1)  # 模拟连接耗时
        self._connected = True  # 标记为已连接
        self.log.info("Simulated connection successful. IEC connector is now marked as 'connected'.")

        while not self._stopped:
            # 这是一个最小的运行循环，仅用于保持连接器活动
            # 在实际应用中，这里会包含：
            # 1. 检查连接状态，如果断开则尝试重连
            # 2. 从IEC设备读取数据
            # 3. 将数据转换为ThingsBoard格式并通过 __gateway 发送
            # 4. 处理内部队列（如果有）

            # 为了框架运行，我们只做简单的日志和休眠
            self.log.debug("IEC connector '%s' is running. Connected: %s", self._connector_name, self._connected)

            # 模拟每5秒轮询一次，实际轮询间隔应从config中获取
            time.sleep(self._config.get("pollingInterval", 5))

        self.log.info("IEC connector thread '%s' main loop finished.", self._connector_name)

    def server_side_rpc_handler(self, content: dict):
        """
        处理来自ThingsBoard服务器的RPC请求。
        这个方法是一个阻塞调用，需要尽快返回响应。
        """
        self.log.info("Received server-side RPC request: %s", content)

        device_name = content.get(DEVICE_SECTION_PARAMETER)
        request_id = content.get('id')
        method = content.get(RPC_METHOD_PARAMETER)
        params = content.get(RPC_PARAMS_PARAMETER)

        self.log.debug("RPC details: device_name=%s, method=%s, params=%s", device_name, method, params)

        # 模拟RPC处理
        # 在实际应用中，这里会是向IEC设备发送命令的逻辑
        response_payload = {"status": "success",
                            "message": f"RPC method '{method}' for device '{device_name}' processed (simulated)."}

        # 通过 Gateway 发送RPC响应回ThingsBoard
        if request_id:
            self.__gateway.send_rpc_reply(device_name, request_id, response_payload)

        return response_payload  # 返回给 Gateway，Gateway也会用这个来发送响应

    def on_attributes_update(self, content: dict):
        """
        处理来自ThingsBoard服务器的属性更新请求。
        """
        self.log.info("Received attributes update from ThingsBoard: %s", content)

        device_name = content.get(DEVICE_SECTION_PARAMETER)
        attributes = content.get(DATA_PARAMETER)

        self.log.debug("Attribute update details: device_name=%s, attributes=%s", device_name, attributes)

        # 模拟属性更新处理
        # 在实际应用中，这里会是向IEC设备写入属性值或配置的逻辑
        self.log.info("Simulating attribute update for device '%s' with data: %s", device_name, attributes)

        # 通常属性更新不需要返回响应给ThingsBoard，但如果需要，可以通过 telemetry 或 shared attributes 发送回执。