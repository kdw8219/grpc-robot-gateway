import asyncio
import time
from app.sessions.robot_session import RobotSession
from app.sessions.robot_session import RobotState
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

    async def update_heartbeat(self, robot_id: str) -> bool:
        self.logger.info('get lock in heartbeat')
        async with self._lock:
            session = self._sessions.get(robot_id)
            if not session:
                self.logger.info('workhere?1')
                return False
            
            if time.time() - session.last_heartbeat > HEARTBEAT_TIMEOUT:
                self.logger.info(f'workhere?2 :{str(time.time())}, {str(session.last_heartbeat)}, {str(HEARTBEAT_TIMEOUT)}')
                return False

            session.touch()
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
            for session in self._sessions.values():
                if (
                    session.state == RobotState.CONNECTED
                    and now - session.last_heartbeat > HEARTBEAT_TIMEOUT
                ):
                    self.logger.info(f'robot_id expired!')
                    session.mark_offline()
                    expired.append(session)
                    # 여기서 event push 가능
                    
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
        
    async def mark_offline_and_cleanup(self, session: RobotSession, robot_id):
        await self.mark_offline(robot_id)
        await self.sweep_expired()