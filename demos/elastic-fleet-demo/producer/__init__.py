"""Elastic Fleet producer: a user-run terminal simulator for Snowpipe Streaming Elastic channels."""

import os

CONTRACT_VERSION = 1

# Target table. Override with environment variables; defaults match sql/setup.sql.
DB = os.environ.get("ELASTIC_DEMO_DATABASE", "ELASTIC_DEMO_DB")
SCHEMA = os.environ.get("ELASTIC_DEMO_SCHEMA", "FLEET")
TABLE = os.environ.get("ELASTIC_DEMO_TABLE", "ROUTER_TELEMETRY")
