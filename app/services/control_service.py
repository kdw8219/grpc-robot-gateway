from app.core.config import get_settings

settings = get_settings()


class CommandControlServiceError(Exception):
    pass

class CommandControlServiceNotFoundError(Exception): # 404 Not found
    pass

async def command_control_service():
    pass
    