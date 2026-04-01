{{
    config(
        materialized = 'table'
    )
}}

/*
  One row per (peer_group, fiscal_year, metric).

  Groups by calendar year (YEAR(period_end)) rather than exact period_end so
  that companies with different fiscal year-end dates land in the same cohort.
  NVDA (Jan), AMD (Dec), and TSM (Dec) all become fiscal_year = 2025 and are
  benchmarked together, giving meaningful peer group sizes.

  Annual filings only (is_annual = true). Percentiles use PERCENTILE_CONT
  which interpolates between values, giving a smooth median for small groups.
*/

with base as (

    select
        ticker,
        period_end,
        gross_margin_pct,
        operating_margin_pct,
        net_margin_pct,
        r_and_d_pct,

        case ticker
            when 'NVDA'  then 'GPU & Compute'
            when 'AMD'   then 'GPU & Compute'
            when 'INTC'  then 'GPU & Compute'
            when 'QCOM'  then 'GPU & Compute'
            when 'MRVL'  then 'GPU & Compute'
            when 'ARM'   then 'GPU & Compute'
            when 'CRWV'  then 'GPU & Compute'
            when 'SMCI'  then 'GPU & Compute'

            when 'TSM'   then 'Foundry & Equipment'
            when 'AMAT'  then 'Foundry & Equipment'
            when 'LRCX'  then 'Foundry & Equipment'
            when 'KLAC'  then 'Foundry & Equipment'
            when 'TER'   then 'Foundry & Equipment'
            when 'ENTG'  then 'Foundry & Equipment'
            when 'ONTO'  then 'Foundry & Equipment'
            when 'ASML'  then 'Foundry & Equipment'

            when 'MU'    then 'Memory'
            when 'WDC'   then 'Memory'
            when 'STX'   then 'Memory'

            when 'ANET'  then 'Networking'
            when 'CSCO'  then 'Networking'
            when 'CIEN'  then 'Networking'
            when 'INFN'  then 'Networking'

            when 'MSFT'  then 'Hyperscalers'
            when 'GOOGL' then 'Hyperscalers'
            when 'AMZN'  then 'Hyperscalers'
            when 'META'  then 'Hyperscalers'
            when 'ORCL'  then 'Hyperscalers'

            when 'SNOW'  then 'AI Software'
            when 'DDOG'  then 'AI Software'
            when 'MDB'   then 'AI Software'
            when 'NET'   then 'AI Software'
            when 'CFLT'  then 'AI Software'
            when 'GTLB'  then 'AI Software'

            when 'AVGO'  then 'Custom Silicon'
            when 'MCHP'  then 'Custom Silicon'
            when 'SWKS'  then 'Custom Silicon'
            when 'QRVO'  then 'Custom Silicon'
            when 'MTSI'  then 'Custom Silicon'

            else 'Other'
        end                                             as peer_group

    from {{ ref('fct_company_metrics') }}
    where is_annual = true

),

unpivoted as (

    -- Unpivot four margin columns to long format so a single aggregation
    -- block computes all metrics uniformly.

    select ticker, peer_group, year(period_end) as fiscal_year,
           'gross_margin_pct'     as metric,
           gross_margin_pct       as value
    from base

    union all

    select ticker, peer_group, year(period_end) as fiscal_year,
           'operating_margin_pct' as metric,
           operating_margin_pct   as value
    from base

    union all

    select ticker, peer_group, year(period_end) as fiscal_year,
           'net_margin_pct'       as metric,
           net_margin_pct         as value
    from base

    union all

    select ticker, peer_group, year(period_end) as fiscal_year,
           'r_and_d_pct'          as metric,
           r_and_d_pct            as value
    from base

),

aggregated as (

    select
        peer_group,
        fiscal_year,
        metric,
        count(*)                                                         as n_companies,
        percentile_cont(0.25) within group (order by value)             as p25,
        percentile_cont(0.50) within group (order by value)             as median,
        percentile_cont(0.75) within group (order by value)             as p75,
        avg(value)                                                       as mean,
        stddev_pop(value)                                                as stddev

    from unpivoted
    where value is not null
    group by peer_group, fiscal_year, metric

)

select * from aggregated
