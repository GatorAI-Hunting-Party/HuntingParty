-- 1) Create base tables in public schema
create table if not exists public.market_metrics (
  market_id    bigserial primary key,
  provider     text,                  -- 'CREXi' or 'Realtor.com'
  geography    text,
  asset_type   text,
  metric_name  text not null,         -- e.g., 'rentable_sqft','cap_rate_pct','avg_rent_usd_month'
  unit_type    text,                  -- optional: '1BR','2BR','3BR','4BR' (for rents)
  period_start date,
  period_end   date,
  value        numeric not null,
  unit         text,                  -- 'sqft','%','USD','USD/month','$/sf','$/unit','$/acre'
  filtered     boolean default true,  -- set true for the subset used in the demo
  source_uri   text,
  created_at   timestamptz default now()
);

create table if not exists public.investor_metrics (
  metric_id      bigserial primary key,
  deal_id        text,
  geography      text,
  asset_type     text,
  metric_name    text not null,       -- same naming as market_metrics
  unit_type      text,                -- optional for rents
  value_num      numeric,
  unit           text,
  period_label   text,                -- 'T12','Pro Forma Y1', etc.
  source_uri     text,
  created_at     timestamptz default now()
);

-- Helpful indexes
create index if not exists mm_idx on public.market_metrics (geography, asset_type, metric_name, unit_type, provider);
create index if not exists im_idx on public.investor_metrics (deal_id, geography, asset_type, metric_name, unit_type);

-- 2) Create the comparison view (OM vs CREXi vs Realtor), backing your 3-column table [1]
create or replace view public.v_deal_all_metrics as
with market as (
  select geography, asset_type, metric_name, unit_type,
         max(case when provider = 'CREXi' then value end)       as crexi_value,
         max(case when provider = 'Realtor.com' then value end) as realtor_value,
         max(unit) as unit
  from public.market_metrics
  where filtered is true
  group by geography, asset_type, metric_name, unit_type
)
select i.deal_id,
       i.metric_name,
       coalesce(i.unit_type, '') as unit_type,
       i.unit,
       i.value_num                           as om_value,
       m.crexi_value                         as crexi_filtered_avg,
       m.realtor_value                       as realtor_filtered_avg,
       case
         when m.crexi_value   is not null then round(i.value_num - m.crexi_value,   2)
         when m.realtor_value is not null then round(i.value_num - m.realtor_value, 2)
         else null
       end                                    as deviation,
       case
         when m.crexi_value is null and m.realtor_value is null then 'no market match'
         when m.crexi_value is not null and i.value_num > m.crexi_value then '+ above CREXi'
         when m.crexi_value is not null and i.value_num < m.crexi_value then '- below CREXi'
         when m.realtor_value is not null and i.value_num > m.realtor_value then '+ above Realtor'
         when m.realtor_value is not null and i.value_num < m.realtor_value then '- below Realtor'
         else '≈ matches market'
       end                                    as comment
from public.investor_metrics i
left join market m
  on m.geography   = i.geography
 and m.asset_type  = i.asset_type
 and m.metric_name = i.metric_name
 and coalesce(m.unit_type, '') = coalesce(i.unit_type, '');


 -- OM values for one deal
insert into public.investor_metrics(deal_id, geography, asset_type, metric_name, unit, value_num)
values
  ('DEMO1','Tampa, FL','Multifamily','rentable_sqft','sqft',108750),
  ('DEMO1','Tampa, FL','Multifamily','cap_rate_pct','%',5.4),
  ('DEMO1','Tampa, FL','Multifamily','noi_usd','USD',1890000),
  ('DEMO1','Tampa, FL','Multifamily','price_per_sf_usd','$/sf',315),
  ('DEMO1','Tampa, FL','Multifamily','price_per_unit_usd','$/unit',285417),
  ('DEMO1','Tampa, FL','Multifamily','price_per_acre_usd','$/acre',18513514),
  ('DEMO1','Tampa, FL','Multifamily','avg_rent_usd_month','USD/month',1650) -- 1BR example
;

-- Market medians (filtered)
insert into public.market_metrics(provider, geography, asset_type, metric_name, unit, value, filtered)
values
  ('CREXi','Tampa, FL','Multifamily','rentable_sqft','sqft',107296,true),
  ('CREXi','Tampa, FL','Multifamily','cap_rate_pct','%',5.4,true),
  ('CREXi','Tampa, FL','Multifamily','noi_usd','USD',1761406,true),
  ('CREXi','Tampa, FL','Multifamily','price_per_sf_usd','$/sf',314,true),
  ('CREXi','Tampa, FL','Multifamily','price_per_unit_usd','$/unit',277144,true),
  ('CREXi','Tampa, FL','Multifamily','price_per_acre_usd','$/acre',18860538,true),
  ('Realtor.com','Tampa, FL','Multifamily','avg_rent_usd_month','USD/month',1525,true)
;

-- Query the view
select * from public.v_deal_all_metrics
where deal_id = 'DEMO1'
order by metric_name, unit_type;
