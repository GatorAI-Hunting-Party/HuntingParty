from .secrets import get_azure_openai_config


def get_azure_cfg():
    cfg = get_azure_openai_config()
    if not cfg["endpoint"] or not cfg["api_key"]:
        raise RuntimeError("Missing AZURE_OPENAI_ENDPOINT or AZURE_OPENAI_API_KEY in Streamlit Secrets.")
    return cfg
