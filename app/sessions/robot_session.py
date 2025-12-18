import time
from enum import Enum

class RobotState(Enum):
    CONNECTED = "CONNECTED"
    OFFLINE = "OFFLINE"

class RobotSession:
    def __init__(self, robot_id: str, robot_control_stub = None):
        self.robot_id = robot_id
        
        self.state = RobotState.CONNECTED
        self.created_at = time.time()
        self.last_heartbeat = time.time()
        self.robot_addr:str = ""
        
        # robot 쪽도 bidnirectional stream 이 있어야 하고, gateway 쪽도 bidirectional stream이 있어야..
        # 근데 이게 최선인가?
        self.robot_stream = None     # robot → api
        self.gateway_stream = None   # gateway → api

    def touch(self):
        self.last_heartbeat = time.time()

    def mark_offline(self):
        self.state = RobotState.OFFLINE
