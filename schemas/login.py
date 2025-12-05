from pydantic import BaseModel, Field

class loginOutput(BaseModel):
    access_token:str = Field(..., description="Access Token For Robot Login Service")
    refresh_token:str = Field(..., description="Refresh Token For Robot Login Service")
    