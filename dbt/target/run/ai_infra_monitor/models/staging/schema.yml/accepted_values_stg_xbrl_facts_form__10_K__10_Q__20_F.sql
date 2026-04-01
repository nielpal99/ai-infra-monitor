
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        form as value_field,
        count(*) as n_records

    from financial_intelligence.staging.stg_xbrl_facts
    group by form

)

select *
from all_values
where value_field not in (
    '10-K','10-Q','20-F'
)



  
  
      
    ) dbt_internal_test