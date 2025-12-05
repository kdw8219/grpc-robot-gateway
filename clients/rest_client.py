import httpx
from core.config import get_settings
from httpx import Response

settings = get_settings()

async def request_post(client: httpx.AsyncClient, url:str, payload:dict, timeout:int):
    response:Response | None = None
    
    try:
        print(url)
        response = await client.post(url, json=payload, timeout= timeout)
        response.raise_for_status()
    
    except httpx.HTTPError as e: #4x, 5x error
        raise httpx.HTTPError(e)
    
    return response

async def request_get(client: httpx.AsyncClient, url:str, timeout:int):
    response:Response | None = None
    
    try:
        print(url)
        response = await client.get(url, timeout= timeout)
        response.raise_for_status()
    
    except httpx.HTTPError as e: #4x, 5x error
        raise httpx.HTTPError(e)
    
    return response