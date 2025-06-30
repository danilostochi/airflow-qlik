{{ config(
    materialized = 'view'
) }}

select
    id_filial
from dados.fct_fat_varejo
