
    
    

with all_values as (

    select
        concept as value_field,
        count(*) as n_records

    from financial_intelligence.staging.stg_xbrl_facts
    group by concept

)

select *
from all_values
where value_field not in (
    'Revenues','GrossProfit','OperatingIncomeLoss','NetIncomeLoss','ResearchAndDevelopmentExpense','CapitalExpendituresIncurringObligation','Assets','CashAndCashEquivalentsAtCarryingValue'
)


