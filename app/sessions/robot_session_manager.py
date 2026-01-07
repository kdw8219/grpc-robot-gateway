import asyncio
import time
from app.sessions.robot_session import RobotSession, RobotState, SessionChannel
from logging import Logger 

HEARTBEAT_TIMEOUT = 20.0  # seconds

class RobotSessionManager:
    def __init__(self, logger:Logger):
        self._sessions: dict[str, RobotSession] = {}
        self._lock = asyncio.Lock()
        self.logger = logger

    async def get_or_create(self, robot_id: str, control_stub = None) -> RobotSession:
        async with self._lock:
            if robot_id in self._sessions:
                return self._sessions[robot_id]

            session = RobotSession(robot_id, control_stub)
            self._sessions[robot_id] = session
            return session

    async def get(self, robot_id: str) -> RobotSession | None:
        async with self._lock:
            return self._sessions.get(robot_id)
    
    async def update_heartbeat(self, robot_id: str, channel: SessionChannel) -> bool:
        """Update a specific channel heartbeat. Returns False if the session is missing or already expired."""
        async with self._lock:
            session = self._sessions.get(robot_id)
            if not session:
                self.logger.info("heartbeat: missing session for %s", robot_id)
                return False

            now = time.time()
            if session.is_channel_expired(channel, HEARTBEAT_TIMEOUT, now):
                self.logger.info(
                    "heartbeat expired before update robot_id=%s channel=%s", robot_id, channel.value
                )
                return False

            session.touch(channel)
            return True

    async def mark_offline(self, robot_id: str):
        async with self._lock:
            session = self._sessions.get(robot_id)
            if session:
                self.logger.info(f'robot_id expired : {robot_id}')
                session.mark_offline()

    async def sweep_expired(self):
        now = time.time()
        expired = []
        async with self._lock:
            expired_ids = []
            for robot_id, session in self._sessions.items():
                if session.state != RobotState.CONNECTED:
                    continue

                if session.all_channels_expired(HEARTBEAT_TIMEOUT, now):
                    self.logger.info('robot_id expired!')
                    session.mark_offline()
                    expired.append(session)
                    expired_ids.append(robot_id)
                    # 여기서 event push 가능
            for robot_id in expired_ids:
                self._sessions.pop(robot_id, None)

        for session in expired:
            await self.cleanup_session(session)
        
        # need to add Robot offline event like this.    
        # await self.event_bus.publish(
        #    RobotOfflineEvent(session.robot_id)
        #)

    async def cleanup_session(self, session: RobotSession):
        session.robot_addr = ""

        if session.robot_stream:
            try:
                await session.robot_stream.cancel()
            except Exception:
                pass

        session.robot_stream = None
        session.gateway_stream = None
        
    async def mark_offline_and_cleanup(self, session: RobotSession, robot_id):
        await self.mark_offline(robot_id)
        await self.sweep_expired()

        
    async def is_session_alive(self, robot_id, channel: SessionChannel) -> bool:
        async with self._lock:
            session = self._sessions.get(robot_id)
            if not session:
                return False
            
            if session.robot_stream is None and channel == SessionChannel.ROBOT_SIGNAL:
                return False
            elif session.gateway_stream is None and channel == SessionChannel.SERVER_SIGNAL:
                return False
            elif session.command_stream is None and channel == SessionChannel.COMMAND:
                return False
            
            return not session.is_channel_expired(channel, HEARTBEAT_TIMEOUT)
