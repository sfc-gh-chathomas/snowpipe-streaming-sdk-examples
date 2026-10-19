-- Elastic Fleet demo: authentication for the producer (run once, after sql/setup.sql).
--
-- The producer authenticates with a programmatic access token (PAT). This script
-- creates a dedicated service user that can only insert into the demo table:
--
--   role    ELASTIC_DEMO_PRODUCER   USAGE on database/schema, INSERT on the table
--   user    ELASTIC_DEMO_SVC        TYPE = SERVICE (no password, no interactive login)
--   policy  ELASTIC_DEMO_PRODUCER_POLICY
--           Service users can only generate or use a PAT while subject to a
--           network policy, so this one allows the IP address(es) the producer runs from.
--
-- Run as a role that can create roles, users and network policies (for example
-- SECURITYADMIN for the role/user/grants and a role with CREATE NETWORK POLICY).
-- Replace <PRODUCER_PUBLIC_IP> before running. Find it with: curl -s https://checkip.amazonaws.com
-- If your account already applies a network policy to all users, make sure it also
-- allows that address and skip the policy section.

-- 1. Least-privilege role for streaming into the demo table.
CREATE ROLE IF NOT EXISTS ELASTIC_DEMO_PRODUCER
  COMMENT = 'Elastic Fleet demo: INSERT into ROUTER_TELEMETRY via Snowpipe Streaming';
GRANT USAGE ON DATABASE ELASTIC_DEMO_DB TO ROLE ELASTIC_DEMO_PRODUCER;
GRANT USAGE ON SCHEMA ELASTIC_DEMO_DB.FLEET TO ROLE ELASTIC_DEMO_PRODUCER;
GRANT INSERT ON TABLE ELASTIC_DEMO_DB.FLEET.ROUTER_TELEMETRY TO ROLE ELASTIC_DEMO_PRODUCER;
-- The default pipe (ROUTER_TELEMETRY-STREAMING) needs no extra privilege.

-- 2. Dedicated service user (no password; it can only authenticate with its PAT).
CREATE USER IF NOT EXISTS ELASTIC_DEMO_SVC
  TYPE = SERVICE
  DEFAULT_ROLE = ELASTIC_DEMO_PRODUCER
  COMMENT = 'Elastic Fleet demo producer (PAT only)';
GRANT ROLE ELASTIC_DEMO_PRODUCER TO USER ELASTIC_DEMO_SVC;

-- 3. Network policy for the producer's address.
CREATE NETWORK RULE IF NOT EXISTS ELASTIC_DEMO_DB.FLEET.ELASTIC_DEMO_PRODUCER_IPS
  MODE = INGRESS TYPE = IPV4
  VALUE_LIST = ('<PRODUCER_PUBLIC_IP>/32')
  COMMENT = 'Where the Elastic Fleet demo producer runs';
CREATE NETWORK POLICY IF NOT EXISTS ELASTIC_DEMO_PRODUCER_POLICY
  ALLOWED_NETWORK_RULE_LIST = ('ELASTIC_DEMO_DB.FLEET.ELASTIC_DEMO_PRODUCER_IPS')
  COMMENT = 'Elastic Fleet demo producer';
ALTER USER ELASTIC_DEMO_SVC SET NETWORK_POLICY = ELASTIC_DEMO_PRODUCER_POLICY;

-- 4. The token. Its secret appears ONCE in the result (token_secret column):
--    copy it straight into the producer's hidden prompt or ELASTIC_DEMO_PAT (see README).
--    Never paste it into files, chat, tickets, or on screen.
ALTER USER ELASTIC_DEMO_SVC ADD PROGRAMMATIC ACCESS TOKEN ELASTIC_DEMO_PRODUCER_PAT
  ROLE_RESTRICTION = 'ELASTIC_DEMO_PRODUCER'
  DAYS_TO_EXPIRY = 7
  COMMENT = 'Elastic Fleet demo producer';

-- Check: name, role restriction, expiry, status.
SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER ELASTIC_DEMO_SVC;

-- Rotate / remove when done:
-- ALTER USER ELASTIC_DEMO_SVC REMOVE PROGRAMMATIC ACCESS TOKEN ELASTIC_DEMO_PRODUCER_PAT;
