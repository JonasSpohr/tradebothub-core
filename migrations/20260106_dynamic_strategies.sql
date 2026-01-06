-- Dynamic strategies + polling tier (5s) support
-- Apply with service role / migration user.

begin;

-- Allow new strategy keys (e.g., scalping) on bot_versions
alter table public.bot_versions drop constraint if exists bot_versions_strategy_check;

-- Add fast_5s tier to subscriptions
alter table public.subscriptions drop constraint if exists subscriptions_polling_tier_check;
alter table public.subscriptions
  add constraint subscriptions_polling_tier_check
  check (polling_tier = any (array['standard','fast_30s','ultra_15s','fast_5s']::text[]));

-- Add fast_5s tier to deploy queue
alter table public.bot_deploy_queue drop constraint if exists bot_deploy_queue_requested_polling_tier_check;
alter table public.bot_deploy_queue
  add constraint bot_deploy_queue_requested_polling_tier_check
  check (requested_polling_tier = any (array['standard','fast_30s','ultra_15s','fast_5s']::text[]));

commit;
