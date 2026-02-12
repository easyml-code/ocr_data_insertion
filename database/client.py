"""
Database client for PostgreSQL operations
"""
from typing import List, Dict, Any, Tuple
from fastapi import HTTPException
from fastapi.concurrency import run_in_threadpool
import psycopg
from psycopg import OperationalError
import traceback

from logs.log import logger
from config import settings


async def get_access_token(email: str, password: str) -> Tuple[str, str]:
    """
    Get authentication tokens
    
    Args:
        email: User email
        password: User password
        
    Returns:
        Tuple of (access_token, refresh_token)
    """
    import requests
    
    def login(payload):
        response = requests.post(
            url=settings.LOGIN_URL,
            json=payload,
            verify=False
        )
        return response.json()
    
    try:
        auth_res = await run_in_threadpool(
            login,
            {"username": email, "password": password}
        )
    except Exception as exc:
        logger.exception("Sign-in failed for email=%s", email)
        raise HTTPException(status_code=500, detail="Authentication service error")
    
    data = auth_res.get("data")
    if not data:
        logger.info("Sign-in failed or no session returned for email=%s", email)
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    session = data.get("auth")
    access_token = session.get("token", None)
    refresh_token = "bjadhfjsdu"  # Placeholder
    
    if not access_token or not refresh_token:
        logger.error("Auth session missing tokens for email=%s: %s", email, auth_res)
        raise HTTPException(status_code=500, detail="Failed to obtain tokens")
    
    logger.info("User signed in: email=%s", email)
    return access_token, refresh_token


async def run_query(
    query: str,
    *,
    retry_on_expire: bool = True
) -> List[Dict[str, Any]]:
    """
    Execute PostgreSQL query
    
    Args:
        query: SQL query to execute
        retry_on_expire: Whether to retry on token expiration (not implemented)
        
    Returns:
        List of dictionaries representing query results
        
    Raises:
        HTTPException: If query execution fails
    """
    
    def _connect_and_exec() -> List[Dict[str, Any]]:
        """Internal function to execute query"""
        conn = None
        cur = None
        try:
            conn = psycopg.connect(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                dbname=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                sslmode="require",
                connect_timeout=5
            )
            
            cur = conn.cursor()
            
            # Execute query
            cur.execute(query)
            
            # Commit for INSERT/UPDATE/DELETE
            if cur.description is None:
                conn.commit()
                return []
            
            # SELECT or INSERT ... RETURNING rows
            rows = cur.fetchall()
            conn.commit()
            
            # Convert rows -> list[dict] using description
            desc = [col.name for col in cur.description] if cur.description else []
            result: List[Dict[str, Any]] = []
            for row in rows:
                row_dict = {desc[i]: row[i] for i in range(len(desc))}
                result.append(row_dict)
            
            return result
            
        except OperationalError as oe:
            logger.error("OperationalError during DB operation: %s", oe)
            raise
        except Exception as exc:
            logger.exception("Error executing query: %s\n%s", exc, traceback.format_exc())
            raise HTTPException(status_code=500, detail="Error executing query")
        
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()
    
    try:
        result = await run_in_threadpool(_connect_and_exec)
        logger.info("run_query success, rows=%d", len(result))
        return result
    
    except Exception as exc:
        logger.exception("Unexpected error in run_query: %s\n%s", exc, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Unexpected error executing query")
