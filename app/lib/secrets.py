import os


def _st_secrets():
    try:
        import streamlit as st

        return getattr(st, "secrets", None)
    except Exception:
        return None


def get_secret(name: str, default: str | None = None) -> str | None:
    s = _st_secrets()
    if s and name in s:
        return str(s[name])
    return os.getenv(name, default)


def get_supabase_config() -> dict:
    return {
        "url": get_secret("SUPABASE_URL"),
        "key": get_secret("SUPABASE_SERVICE_ROLE_KEY") or get_secret("SUPABASE_ANON_KEY"),
    }


def get_azure_openai_config() -> dict:
    return {
        "endpoint": get_secret("AZURE_OPENAI_ENDPOINT"),
        "api_key": get_secret("AZURE_OPENAI_API_KEY"),
    }
