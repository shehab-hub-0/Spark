import os

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://superset:superset@postgres:5432/superset"
)
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "changeme")

# Use Redis for rate limiting (already running)
RATELIMIT_STORAGE_URI = "redis://redis:6379/1"

# Disable example data in production
SUPERSET_LOAD_EXAMPLES = False
