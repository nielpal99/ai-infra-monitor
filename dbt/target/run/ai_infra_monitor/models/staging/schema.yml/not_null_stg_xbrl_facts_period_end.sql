
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period_end
from financial_intelligence.staging.stg_xbrl_facts
where period_end is null



  
  
      
    ) dbt_internal_test