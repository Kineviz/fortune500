"""
LLM provider configuration for SEC filing entity extraction.

Supports:
  - vertex   : Google Vertex AI Batch Prediction (recommended for full runs)
  - openrouter: OpenRouter API (good for testing / model comparison)
  - local    : Local model via OpenAI-compatible API (ollama, vllm, lmstudio)
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the pipeline directory
load_dotenv(Path(__file__).parent / ".env")

# ── Provider selection ──────────────────────────────────────────────
# "vertex", "openrouter", or "local"
PROVIDER = os.getenv("LLM_PROVIDER", "vertex")

# ── Model selection ─────────────────────────────────────────────────
# Short name used for output directory and tracking.
# Must be a key in MODEL_REGISTRY below, or a raw model ID for the provider.
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-3-flash")

# Model registry: short_name -> { provider-specific model IDs }
MODEL_REGISTRY = {
    # ── Gemini 3.x (Vertex AI via GenAI SDK, location=global) ─────
    "gemini-3.1-pro": {
        "vertex": "gemini-3.1-pro-preview",
        "openrouter": "google/gemini-3.1-pro-preview",
    },
    "gemini-3-flash": {
        "vertex": "gemini-3-flash-preview",
        "openrouter": "google/gemini-3-flash-preview",
    },
    "gemini-3.1-flash-lite": {
        "vertex": "gemini-3.1-flash-lite-preview",
        "openrouter": "google/gemini-3.1-flash-lite-preview",
    },
    # ── Gemini 2.5 (Vertex AI batch + GenAI SDK) ───────────────────
    "gemini-2.5-pro": {
        "vertex": "gemini-2.5-pro",
        "openrouter": "google/gemini-2.5-pro-preview",
    },
    "gemini-2.5-flash": {
        "vertex": "gemini-2.5-flash",
        "openrouter": "google/gemini-2.5-flash-preview",
    },
    "gemini-2.5-flash-lite": {
        "vertex": "gemini-2.5-flash-lite",
        "openrouter": "google/gemini-2.5-flash-lite-preview",
    },
    # ── Gemini 2.0 (non-thinking, GA) ─────────────────────────────
    "gemini-2.0-flash": {
        "vertex": "gemini-2.0-flash",
        "openrouter": "google/gemini-2.0-flash",
    },
    # ── QWen (OpenRouter / local) ───────────────────────────────────
    "qwen3.5-35b": {
        "openrouter": "qwen/qwen3.5-35b",
        "local": "qwen3.5-35b",
    },
    "qwen3.5-9b": {
        "openrouter": "qwen/qwen3.5-9b",
        "local": "qwen3.5-9b",
    },
}


def get_model_id(model_name: str = None, provider: str = None) -> str:
    """Resolve short model name to provider-specific model ID."""
    model_name = model_name or MODEL_NAME
    provider = provider or PROVIDER
    entry = MODEL_REGISTRY.get(model_name)
    if entry and provider in entry:
        return entry[provider]
    # Fall through: treat model_name as a raw model ID
    return model_name


# ── Vertex AI config ───────────────────────────────────────────────
VERTEX_PROJECT = os.getenv("VERTEX_PROJECT", "")
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "us-central1")
GCS_BUCKET = os.getenv("GCS_BUCKET", "")  # e.g. "gs://my-bucket"
# All pipeline data goes under this prefix — keeps it isolated from
# existing data (e.g. json/, md/) in the same bucket.
GCS_PIPELINE_PREFIX = os.getenv("GCS_PIPELINE_PREFIX", "pipeline_batch")

# ── OpenRouter config ──────────────────────────────────────────────
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# ── Local model config (OpenAI-compatible endpoint) ────────────────
LOCAL_BASE_URL = os.getenv("LOCAL_LLM_URL", "http://192.168.1.100:1234/v1")

# ── Shared LLM parameters ─────────────────────────────────────────
TEMPERATURE = 0.2
MAX_TOKENS = 8192

# ── Data paths ─────────────────────────────────────────────────────
DATA_DIR = os.getenv(
    "DATA_DIR",
    os.path.join(os.path.dirname(__file__), "..", "data_2021-2025"),
)
OUTPUT_BASE = os.getenv(
    "OUTPUT_DIR",
    os.path.join(os.path.dirname(__file__), "output"),
)
KUZU_DB_PATH = os.getenv(
    "KUZU_DB_PATH",
    os.path.join(os.path.dirname(__file__), "sec_filings_db"),
)


def get_output_dir(model_name: str = None) -> str:
    """Return model-specific output directory: output/{model_name}/"""
    model_name = model_name or MODEL_NAME
    return os.path.join(OUTPUT_BASE, model_name)


def get_kuzu_db_path(model_name: str = None) -> str:
    """Return model-specific KuzuDB path: sec_filings_db/{model_name}/"""
    model_name = model_name or MODEL_NAME
    return os.path.join(KUZU_DB_PATH, model_name)


# ── Extraction settings ───────────────────────────────────────────
TARGET_SECTIONS = ["Item 1.", "Item 1A.", "Item 7."]
MAX_CONTENT_CHARS = 100_000
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "5"))

# ── Extraction prompt (mirrors fortune500/05_extraction.sql) ──────
EXTRACTION_PROMPT = """Analyze the following text from a 10-K filing (Section: {section_id}). \
Extract insights for the following questions and return ONLY valid JSON matching this EXACT schema:
{{
  "markets": {{
    "entering": [{{"market": "Name", "evidence": "Details...", "reference": "Original text..."}}],
    "exiting": [{{"market": "Name", "evidence": "Details...", "reference": "Original text..."}}],
    "expanding": [{{"market": "Name", "details": "Details...", "reference": "Original text..."}}]
  }},
  "risks_opportunities": {{
    "emerging_risks": [{{"risk": "Name", "description": "Details...", "reference": "Original text..."}}],
    "emerging_opportunities": [{{"opportunity": "Name", "description": "Details...", "reference": "Original text..."}}]
  }},
  "competitors": [{{"name": "Name", "relationship": "Details...", "reference": "Original text..."}}]
}}

Do NOT use markdown code blocks. Return raw JSON only.
Text:
{content}"""
