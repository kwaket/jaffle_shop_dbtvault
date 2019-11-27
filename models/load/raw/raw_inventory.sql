{{ config(materialized='view', enabled=true, tags='raw')}}

select
  a.PS_PARTKEY as PARTKEY
, a.PS_SUPPKEY as SUPPLIERKEY
, a.PS_AVAILQTY as AVAILQTY
, a.PS_SUPPLYCOST as SUPPLYCOST
, a.PS_COMMENT as PART_SUPPLY_COMMENT
, b.S_NAME as SUPPLIER_NAME
, b.S_ADDRESS as SUPPLIER_ADDRESS
, b.S_NATIONKEY as SUPPLIER_NATION_KEY
, b.S_PHONE as SUPPLIER_PHONE
, b.S_ACCTBAL as SUPPLIER_ACCTBAL
, b.S_COMMENT as SUPPLIER_COMMENT
, c.P_NAME as PART_NAME
, c.P_MFGR as PART_MFGR
, c.P_BRAND as PART_BRAND
, c.P_TYPE as PART_TYPE
, c.P_SIZE as PART_SIZE
, c.P_CONTAINER as PART_CONTAINER
, c.P_RETAILPRICE as PART_RETAILPRICE
, c.P_COMMENT as PART_COMMENT
, d.N_NAME as SUPPLIER_NATION_NAME
, d.N_COMMENT as SUPPLIER_NATION_COMMENT
, d.N_REGIONKEY as SUPPLIER_REGION_KEY
, e.R_NAME as SUPPLIER_REGION_NAME
, e.R_COMMENT as SUPPLIER_REGION_COMMENT
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.PARTSUPP as a
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.SUPPLIER as b on a.PS_SUPPKEY=b.S_SUPPKEY
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.PART as c on a.PS_PARTKEY=c.P_PARTKEY
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.NATION as d on b.S_NATIONKEY=d.N_NATIONKEY
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.REGION as e on d.N_REGIONKEY=e.R_REGIONKEY
join {{ ref('raw_orders') }} as f on a.PS_PARTKEY = f.PARTKEY and a.PS_SUPPKEY=f.SUPPLIERKEY
order by a.PS_PARTKEY, a.PS_SUPPKEY