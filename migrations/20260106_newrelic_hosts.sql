-- New Relic per-host credentials
-- Apply with migration/service role.

begin;

-- Store a host-specific New Relic ingest key (or license key) and app name used by the agent.
alter table public.bot_hosts
  add column if not exists newrelic_key text,
  add column if not exists newrelic_app_name text;

commit;
