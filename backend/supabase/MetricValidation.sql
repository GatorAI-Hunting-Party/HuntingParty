-- Rules and flags
create table if not exists public.validator_rules (
  metric_name      text primary key,
  unit             text,
  min_value        numeric,
  max_value        numeric,
  required_nonzero boolean default false,
  pct_0_100        boolean default false,
  notes            text
);

create table if not exists public.flags (
  flag_id    bigserial primary key,
  metric_id  bigint references public.investor_metrics(metric_id) on delete cascade,
  flag_type  text,              -- 'range_violation','incoherent','missing','unit_mismatch'
  severity   text check (severity in ('info','warn','high')) default 'warn',
  message    text,
  created_at timestamptz default now()
);

-- Seed initial ranges (edit later without code)
insert into public.validator_rules(metric_name, unit, min_value, max_value, required_nonzero, pct_0_100, notes) values
  ('cap_rate_pct','%', 2.0, 12.0, true, false, 'Typical cap rates'),
  ('noi_usd','USD', 1000, 1000000000, true, false, 'NOI should be positive'),
  ('price_per_sf_usd','$/sf', 50, 1000, true, false, 'Broad bounds for $/sf'),
  ('price_per_unit_usd','$/unit', 20000, 1000000, true, false, 'Broad bounds for $/unit'),
  ('price_per_acre_usd','$/acre', 100000, 100000000, true, false, 'Broad bounds for $/acre'),
  ('occupancy_pct','%', 0, 100, false, true, 'Occupancy 0–100'),
  ('avg_rent_usd_month','USD/month', 300, 6000, true, false, 'Typical monthly rents')
on conflict (metric_name) do nothing;

-- Trigger: validate each OM metric as it’s inserted
create or replace function public.fn_validate_investor_metric()
returns trigger as $$
declare r record;
begin
  select * into r from public.validator_rules where metric_name = new.metric_name;
  if new.value_num is null then
    insert into public.flags(metric_id, flag_type, severity, message)
    values (new.metric_id, 'missing', 'warn', new.metric_name || ': value missing');
    return new;
  end if;

  if r.required_nonzero and new.value_num = 0 then
    insert into public.flags(metric_id, flag_type, severity, message)
    values (new.metric_id, 'incoherent', 'high', new.metric_name || ': must be nonzero');
  end if;

  if (r.pct_0_100 = true or lower(coalesce(new.unit,'')) = '%')
     and (new.value_num < 0 or new.value_num > 100) then
    insert into public.flags(metric_id, flag_type, severity, message)
    values (new.metric_id, 'incoherent', 'high', new.metric_name || ': percent outside 0–100');
  end if;

  if r.min_value is not null and new.value_num < r.min_value then
    insert into public.flags(metric_id, flag_type, severity, message)
    values (new.metric_id, 'range_violation', 'warn', new.metric_name || ': below min ' || r.min_value);
  end if;

  if r.max_value is not null and new.value_num > r.max_value then
    insert into public.flags(metric_id, flag_type, severity, message)
    values (new.metric_id, 'range_violation', 'warn', new.metric_name || ': above max ' || r.max_value);
  end if;

  return new;
end;
$$ language plpgsql;

drop trigger if exists trg_validate_investor_metric on public.investor_metrics;
create trigger trg_validate_investor_metric
after insert on public.investor_metrics
for each row execute function public.fn_validate_investor_metric();