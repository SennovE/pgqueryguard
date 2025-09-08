from pydantic_settings import BaseSettings, SettingsConfigDict

class DefaultSettings(BaseSettings):

    BACKEND_HOST: str
    BACKEND_PORT: int
    PATH_PREFIX : str

settings: DefaultSettings | None = None

def get_settings() -> DefaultSettings:
    global settings
    if settings is None:
        settings = DefaultSettings()
    return settings