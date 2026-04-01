
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test