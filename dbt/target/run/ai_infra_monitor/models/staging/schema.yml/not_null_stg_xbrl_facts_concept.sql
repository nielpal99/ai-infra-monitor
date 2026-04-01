
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select concept
from financial_intelligence.staging.stg_xbrl_facts
where concept is null



  
  
      
    ) dbt_internal_test