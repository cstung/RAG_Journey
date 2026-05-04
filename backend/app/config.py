from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')
    app_name: str = 'LWHN Legal RAG'
    openai_api_key: str = ''
    llm_model: str = 'gpt-4.1'
    translation_model: str = 'gpt-4.1-mini'
    database_url: str = 'sqlite+aiosqlite:///./data/sqlite/lwhn.db'
    secret_key: str = 'dev-secret'

settings = Settings()
