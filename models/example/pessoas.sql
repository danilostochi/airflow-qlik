{{ config(
    materialized = 'table',
    schema = 'dados'
) }}

select
    1 as id,
    'João' as nome,
    '2025-06-29'::date as data_criacao
