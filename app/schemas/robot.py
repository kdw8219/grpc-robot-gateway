from pydantic import BaseModel, Field
from typing import Optional

class Robot(BaseModel):
    robot_id: Optional[str] = Field(None, description="Robot ID")
    robot_secret: Optional[str] = Field(None, description="Sort of Robot's password")
    model: Optional[str] = Field(None, description="Model name")
    firmware_version: Optional[str] = Field(None, description="Firmware version")
    location: Optional[str] = Field(None, description="Robot's location")