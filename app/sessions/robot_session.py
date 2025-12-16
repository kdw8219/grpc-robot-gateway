import time
from enum import Enum

class RobotState(Enum):
    CONNECTED = "CONNECTED"
    OFFLINE = "OFFLINE"

class RobotSession:
    def __init__(self, robot_id: str, control_stub = None):
        self.robot_id = robot_id
        self.control_stub = control_stub

        self.state = RobotState.CONNECTED
        self.created_at = time.time()
        self.last_heartbeat = time.time()
        self.robot_addr:str = ""
        
         # control / signaling 용 핸들
        self.control_channel = None      # SendCommand를 처리할 대상
        self.signal_stream = None

    def touch(self):
        self.last_heartbeat = time.time()

    def mark_offline(self):
        self.state = RobotState.OFFLINE
