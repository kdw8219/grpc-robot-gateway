import asyncio
import time
from app.sessions.robot_session import RobotSession
from app.sessions.robot_session import RobotState

HEARTBEAT_TIMEOUT = 20.0  # seconds

class RobotSessionManager:
    def __init__(self):
        self._sessions: dict[str, RobotSession] = {}
        self._lock = asyncio.Lock()

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
        async with self._lock:
            session = self._sessions.get(robot_id)
            if not session:
                return False
            
            if time.time() - session.last_heartbeat > HEARTBEAT_TIMEOUT:
                await session.control_channel.close()
                session.control_channel = None
                session.control_stub = None
                return False

            session.touch()
            return True

    async def mark_offline(self, robot_id: str):
        async with self._lock:
            session = self._sessions.get(robot_id)
            if session:
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
                    session.mark_offline()
                    expired.append(session)
                    # 여기서 event push 가능
                    
        for session in expired:
            await self.cleanup_session(session)

    async def cleanup_session(self, session: RobotSession):
        if session.control_channel:
            try:
                await session.control_channel.close()
            except Exception:
                pass

        session.control_channel = None
        session.control_stub = None

        if session.signal_stream:
            try:
                await session.signal_stream.cancel()
            except Exception:
                pass

        session.signal_stream = None