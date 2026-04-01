
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_annual
from financial_intelligence.staging.stg_xbrl_facts
where is_annual is null



  
  
      
    ) dbt_internal_test