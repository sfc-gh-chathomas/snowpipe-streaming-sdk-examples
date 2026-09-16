import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import elastic_step1_quickstart as quickstart


def test_connection_properties_uses_profile_without_pat(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_PAT", raising=False)

    assert quickstart.connection_properties() is None


def test_connection_properties_builds_pat_settings(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_PAT", "token")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "account")
    monkeypatch.setenv("SNOWFLAKE_URL", "https://account.snowflakecomputing.com")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "role")

    assert quickstart.connection_properties() == {
        "authorization_type": "PAT",
        "personal_access_token": "token",
        "account": "account",
        "url": "https://account.snowflakecomputing.com",
        "role": "role",
    }


def test_connection_properties_requires_account_and_url(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_PAT", "token")
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    monkeypatch.delenv("SNOWFLAKE_URL", raising=False)

    with pytest.raises(ValueError, match="SNOWFLAKE_ACCOUNT"):
        quickstart.connection_properties()
