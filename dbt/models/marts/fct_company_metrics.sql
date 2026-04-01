{{
    config(
        materialized = 'table'
    )
}}

/*
  One row per (ticker, period_end, form).

  Pivots stg_xbrl_facts from long (one row per concept) to wide (one column
  per metric).  Adds margin ratios and sequential QoQ change columns via LAG()
  partitioned by (ticker, form) — so 10-Q rows compare to the prior quarter
  and 10-K rows compare to the prior fiscal year.
*/

with pivoted as (

    select
        ticker,
        period_end,
        form,
        is_annual,

        -- ── income statement ──────────────────────────────────────────────────
        max(case when concept = 'Revenues'
            then value end)                                     as revenue,

        max(case when concept = 'GrossProfit'
            then value end)                                     as gross_profit,

        max(case when concept = 'OperatingIncomeLoss'
            then value end)                                     as operating_income,

        max(case when concept = 'NetIncomeLoss'
            then value end)                                     as net_income,

        -- ── expense detail ────────────────────────────────────────────────────
        max(case when concept = 'ResearchAndDevelopmentExpense'
            then value end)                                     as r_and_d,

        max(case when concept = 'CapitalExpendituresIncurringObligation'
            then value end)                                     as capex,

        -- ── balance sheet (period-end snapshot) ───────────────────────────────
        max(case when concept = 'Assets'
            then value end)                                     as assets,

        max(case when concept = 'CashAndCashEquivalentsAtCarryingValue'
            then value end)                                     as cash

    from {{ ref('stg_xbrl_facts') }}
    group by ticker, period_end, form, is_annual

),

with_margins as (

    select
        ticker,
        period_end,
        form,
        is_annual,

        revenue,
        gross_profit,
        operating_income,
        net_income,
        r_and_d,
        capex,
        assets,
        cash,

        -- ── margin ratios (NULL when revenue is NULL or zero) ─────────────────
        gross_profit     / nullif(revenue, 0) * 100     as gross_margin_pct,
        operating_income / nullif(revenue, 0) * 100     as operating_margin_pct,
        net_income       / nullif(revenue, 0) * 100     as net_margin_pct,
        r_and_d          / nullif(revenue, 0) * 100     as r_and_d_pct

    from pivoted

),

with_qoq as (

    select
        *,

        -- ── sequential period-over-period changes ─────────────────────────────
        -- Partitioned by (ticker, form): 10-Q rows lag to the prior quarter;
        -- 10-K rows lag to the prior fiscal year.
        revenue - lag(revenue) over (
            partition by ticker, form
            order by period_end
        )                                               as revenue_qoq_chg,

        gross_margin_pct - lag(gross_margin_pct) over (
            partition by ticker, form
            order by period_end
        )                                               as gross_margin_qoq_chg

    from with_margins

)

select * from with_qoq
