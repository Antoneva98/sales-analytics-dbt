-- Агенти зі знижками вище середнього рівня
with overall_average_discount as (
    select round(avg(discount_amount), 2) as avg_discount
    from {{ ref('fct_sales') }}
    where discount_amount > 0
),

agent_sales as (
    -- Розбиваємо список агентів на окремі рядки
    select 
        referenceid,
        trim(agent.value) as individual_agent,
        discount_amount
    from {{ ref('fct_sales') }},
    lateral split_to_table(sales_agents_list, ',') as agent
    where trim(agent.value) != 'N/A' 
    and sales_agents_list != 'N/A'
    and discount_amount > 0
),

agent_average_discounts as (
    select 
        individual_agent as agent_name,
        count(*) as sales_with_discounts,
        round(avg(discount_amount), 2) as average_discount
    from agent_sales
    group by individual_agent
),

agents_above_average as (
    select 
        aad.agent_name,
        aad.sales_with_discounts,
        aad.average_discount,
        oad.avg_discount as overall_average_discount,
        round(aad.average_discount - oad.avg_discount, 2) as difference_from_average
    from agent_average_discounts aad
    cross join overall_average_discount oad
    where aad.average_discount > oad.avg_discount
)

select 
    agent_name,
    sales_with_discounts,
    average_discount,
    overall_average_discount,
    difference_from_average
from agents_above_average
order by difference_from_average desc;
