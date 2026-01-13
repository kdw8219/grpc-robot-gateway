from enum import Enum
import asyncio
import time
import typing

class RobotState(Enum):
    CONNECTED = "CONNECTED"
    OFFLINE = "OFFLINE"

class SessionChannel(Enum):
    ROBOT_SIGNAL = "ROBOT_SIGNAL"
    SERVER_SIGNAL = "SERVER_SIGNAL"
    COMMAND = "COMMAND"


class RobotSession:
    def __init__(self, robot_id: str, robot_control_stub = None):
        self.robot_id = robot_id
        
        self.state = RobotState.CONNECTED
        self.created_at = time.time()
        now = time.time()
        # 독립적인 타이머: 로봇→서버 시그널, 서버↔서버 시그널, 명령 RPC
        self.last_heartbeats: dict[SessionChannel, float] = {
            SessionChannel.ROBOT_SIGNAL: now,
            SessionChannel.SERVER_SIGNAL: now,
            SessionChannel.COMMAND: now,
        }
        self.robot_addr:str = ""
        
        # robot 쪽도 bidnirectional stream 이 있어야 하고, gateway 쪽도 bidirectional stream이 있어야..
        # 근데 이게 최선인가?
        self.robot_stream = None       # robot → grpc 서버
        self.gateway_stream = None     # gateway → grpc 서버
        self.command_stream = None     # robot control rpc stream (robot rpc test 필요?)
        # Per-robot response queue for gateway/robot signaling
        self.response_queue: asyncio.Queue | None = asyncio.Queue()
        # Per-robot queues for robot-bound requests
        self.robot_signal_queue: asyncio.Queue | None = asyncio.Queue()
        self.command_queue: asyncio.Queue | None = asyncio.Queue()

        
    def touch(self, channel: SessionChannel):
        """Update heartbeat for a specific channel."""
        self.last_heartbeats[channel] = time.time()

    def is_channel_expired(self, channel: SessionChannel, timeout: float, now: typing.Optional[float] = None) -> bool:
        now = now or time.time()
        last = self.last_heartbeats.get(channel, 0)
        return (now - last) > timeout

    def all_channels_expired(self, timeout: float, now: typing.Optional[float] = None) -> bool:
        now = now or time.time()
        return all(self.is_channel_expired(ch, timeout, now) for ch in self.last_heartbeats)

    def mark_offline(self):
        self.state = RobotState.OFFLINE
