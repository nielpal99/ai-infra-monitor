{{
    config(
        materialized = 'view'
    )
}}

/*
  Staging model for raw XBRL financial facts.

  Filters raw_xbrl_facts down to:
    - 8 canonical income statement / balance sheet concepts
    - USD-denominated values only
    - Annual (10-K, 20-F) and quarterly (10-Q) filings only

  Deduplicates on (ticker, concept, period_end, form), keeping the most
  recently filed row.  This handles cases where a company restates a period
  in a later filing — the latest filed date wins.

  Note: the capex concept is stored as 'CapitalExpendituresIncurringObligation'
  in raw_xbrl_facts, matching the CONCEPT_MAP key in ingestion/xbrl_parser.py.
*/

with source as (

    select * from {{ source('staging', 'raw_xbrl_facts') }}

),

filtered as (

    select
        ticker,
        concept,
        value::float          as value,
        unit,
        period_end::date      as period_end,
        period_start::date    as period_start,
        form,
        filed::date           as filed,
        fy,
        fp,
        accession,
        loaded_at
    from source
    where concept in (
        'Revenues',
        'GrossProfit',
        'OperatingIncomeLoss',
        'NetIncomeLoss',
        'ResearchAndDevelopmentExpense',
        'CapitalExpendituresIncurringObligation',   -- capex
        'Assets',
        'CashAndCashEquivalentsAtCarryingValue'
    )
      and unit in ('USD', 'USD/shares')
      -- 10-K / 10-Q for domestic filers; 20-F for foreign filers (TSM, ASML, ARM)
      and form in ('10-K', '10-Q', '20-F')

),

deduped as (

    select
        *,
        row_number() over (
            partition by ticker, concept, period_end, form
            order by filed desc
        ) as _row_num
    from filtered

)

select
    ticker,
    concept,
    value,
    unit,
    period_end,
    period_start,
    form,
    filed,
    fy,
    fp,
    accession,
    -- true for annual filings regardless of domestic vs foreign filer type
    (form in ('10-K', '20-F'))  as is_annual,
    loaded_at
from deduped
where _row_num = 1
