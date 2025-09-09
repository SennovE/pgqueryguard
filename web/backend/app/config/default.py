from pydantic_settings import BaseSettings

class DefaultSettings(BaseSettings):
    BACKEND_HOST: str
    BACKEND_PORT: int
    PATH_PREFIX: str

    OPENAI_API_KEY: str | None = None
    DEEPSEEK_API_KEY: str | None = None

settings: DefaultSettings | None = None

def get_settings() -> DefaultSettings:
    global settings
    if settings is None:
        settings = DefaultSettings()
    return settings