"""
Configuration settings for invoice automation
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings"""
    
    # Database settings
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "invoice_db"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = ""
    
    # Authentication settings
    LOGIN_URL: str = "https://auth.example.com/login"
    
    # Application settings
    APP_NAME: str = "Invoice Automation"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


# Global settings instance
settings = Settings()
