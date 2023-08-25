from typing import List

import awswrangler as wr
import boto3
import numpy as np
import pandas as pd

import datetime
import pytz

eastern = pytz.timezone('US/Eastern')

boto3_session = boto3.Session(
    region_name='us-east-1'
)

query = """
WITH
  single_published_catalogs AS (
   SELECT
     sca.catalog_item_id
   , sca.inventory_item_id
   FROM
     ((
      SELECT DISTINCT catalog_item_id
      FROM
        (gogotech_launch.sc_catalog_item_publish_statuses_for_sub_stores pu
      LEFT JOIN gogotech_launch.sc_sub_stores ss ON (pu.sub_store_id = ss.sub_store_id))
      WHERE (ss.Store_id IN (5, 30))
   )  pub
   INNER JOIN (
      SELECT
        catalog_item_id
      , CAST(max(inventory_item_id) AS int) inventory_item_id
      FROM
        gogotech_launch.ord_catalog_item_inventory_mappings
      GROUP BY catalog_item_id
      HAVING (count(*) = 1)
   )  sca ON (pub.catalog_item_id = sca.catalog_item_id))
)
, price AS (
   SELECT
     price_amt current_product_price
   , discount_amt discount
   , catalog_item_id
   FROM
     (
      SELECT
        price.*
      , "row_number"() OVER (PARTITION BY price.catalog_item_id ORDER BY price.store_id ASC) row_number
      FROM
        "gogotech_launch"."pl_price_list_catalog_item_assignments" price
      WHERE (store_id IN (5, 30))
   ) 
   WHERE (row_number = 1)
) 
, product_condition AS (
   SELECT
     PD.catalog_item_id
   , AV.name attribute_name
   , (CASE WHEN (AV.attribute_value_id = 18) THEN 'New' ELSE 'Refurbished' END) legacy_condition
   FROM
     ((gogotech_launch.pc_catalog_item_attribute_values PD
   LEFT JOIN gogotech_launch.pc_attribute_values AV ON (PD.attribute_value_id = AV.attribute_value_id))
   LEFT JOIN gogotech_launch.pc_attributes A ON (AV.attribute_id = A.attribute_id))
   WHERE (A.attribute_id = 1)
) 
, catalog_asin AS (
   SELECT
     cma.catalog_item_id
   , array_join(array_agg(DISTINCT psu.pseudo_product_id), ', ') asin
   FROM
     (gogotech_launch.ca_catalog_item_meta_assignments cma
   INNER JOIN gogotech_launch.ca_pseudo_product_identifiers psu ON (psu.ca_meta_assignment_id = cma.ca_meta_assignment_id))
   WHERE ((psu.product_identifier_type_id = 2) AND (cma.is_archived = false))
   GROUP BY catalog_item_id
) 
, genuine_catalogs AS (
   SELECT
     sc.inventory_item_id
   , sc.catalog_item_id
   , pc."identifier"
   , pc.model_id
   , pcc.legacy_condition
   , pcc.attribute_name
   , pr.current_product_price
   , pr.discount
   , inv.mpn
   , inv.identifier legacy_sku
   , inv.upc
   , inv.barcode
   , inq.syosset_qty
   , inq.tucson_qty
   , inq.live_fba_qty
   , pb.name brand
   , inv.weighted_avg_cost wac
   , IF((inv.web_map_price > 0E0), inv.web_map_price, IF((inv.stock_type IN (8, 10, 12, 14, 17, 18)), pr.current_product_price, null)) web_map_price
   , cs.asin
   FROM
     (((((((single_published_catalogs sc
   LEFT JOIN gogotech_launch.pc_catalog_items pc ON (sc.catalog_item_id = pc.catalog_item_id))
   LEFT JOIN gogotech_launch.inv_inventory_items inv ON (sc.inventory_item_id = inv.inventory_item_id))
   LEFT JOIN gogotech_launch.pc_brands pb ON (inv.brand_id = pb.brand_id))
   LEFT JOIN gogotech_reporting.vw_inventory_quantities inq ON (sc.inventory_item_id = inq.inventory_item_id))
   LEFT JOIN product_condition pcc ON (pc.catalog_item_id = pcc.catalog_item_id))
   LEFT JOIN price pr ON (sc.catalog_item_id = pr.catalog_item_id))
   LEFT JOIN catalog_asin cs ON (sc.catalog_item_id = cs.catalog_item_id))
   WHERE ((NOT (lower(pc."identifier") LIKE '%test%')) AND (NOT (lower(pc."identifier") LIKE '%discount%')) AND (NOT (lower(pc."identifier") LIKE '%upgrade%')) AND (NOT (lower(pc.identifier) LIKE '%special%')) AND (NOT (lower(pc.identifier) LIKE '%do not use%')) AND (NOT (lower(pc.identifier) LIKE '%del')))
) 
, product_with_catalog_in_avinash_sheet as (
        select
        am.brightpearl_sku
          , am.brightpearl_id
          , am.akeneo_sku
          , am.mpn nextgen_mpn
          , am.upc nextgen_upc
          , nb.label_value nextgen_brand
          , am.condition nextgen_condition
          , am.catalogid as catalog_item_id
          , am.legacyinventoryid inventory_item_id
          , inv.identifier legacy_sku
          , am.modelid model_id
          , inv.mpn legacy_mpn
          , inv.upc legacy_upc
          , inv.barcode
          , pb.name legacy_brand
          , pcc.legacy_condition
          , pr.current_product_price
          , pr.discount
          , pcc.attribute_name
          , inq.syosset_qty
          , inq.tucson_qty
          , inq.live_fba_qty
          , inv.weighted_avg_cost wac
          , IF((inv.web_map_price > 0E0), inv.web_map_price, IF((inv.stock_type IN (8, 10, 12, 14, 17, 18)), pr.current_product_price, null)) web_map_price
          , cs.asin
          , false is_bundle
        from (select * from products_fnd.products where bundle = False and legacyinventoryid is not null) am
        LEFT JOIN price pr ON cast(am.catalogid as varchar) = cast(pr.catalog_item_id as varchar)
        LEFT JOIN gogotech_launch.inv_inventory_items inv ON cast(inv.inventory_item_id as varchar) = cast(am.legacyinventoryid as varchar)
        LEFT JOIN gogotech_launch.pc_brands pb ON (inv.brand_id = pb.brand_id)
        LEFT JOIN gogotech_reporting.vw_inventory_quantities inq ON cast(am.legacyinventoryid as varchar) = cast(inq.inventory_item_id as varchar)
        LEFT JOIN products_fnd.brands nb ON (am.brand = nb.code)
        LEFT JOIN product_condition pcc ON cast(am.catalogid as varchar) = cast(pcc.catalog_item_id as varchar)
        LEFT JOIN catalog_asin cs ON cast(am.catalogid as varchar) = cast(cs.catalog_item_id as varchar)
    )

, avinash_mappings AS (
   SELECT *
   FROM
     (
      SELECT
        *, row_number() OVER (PARTITION BY brightpearl_sku ORDER BY current_product_price ASC) rank from product_with_catalog_in_avinash_sheet 
   ) 
   WHERE (rank = 1)
) 


, legacy_details AS (
   SELECT
     gc.catalog_item_id
   , gc.inventory_item_id
   , gc.identifier catalog_name
   , gc.legacy_sku
   , gc.model_id
   , gc.mpn
   , lower(trim(BOTH FROM REGEXP_REPLACE(gc.mpn, '[^a-zA-Z0-9]', ''))) trimed_mpn
   , gc.upc
   , gc.barcode
   , (CASE WHEN (gc.upc <> '0') THEN trim(BOTH '0' FROM gc.upc) ELSE gc.upc END) trimed_upc
   , gc.brand
   , REGEXP_REPLACE(lower(gc.brand), '[^a-zA-Z0-9]', '') trimed_brand
   , gc.attribute_name
   , gc.legacy_condition
   , gc.current_product_price
   , gc.discount
   , gc.syosset_qty
   , gc.tucson_qty
   , gc.live_fba_qty
   , gc.wac
   , gc.web_map_price
   , gc.asin
   FROM
     genuine_catalogs gc
) 
, nextgen_details AS (
   SELECT
     prod.brightpearl_sku
   , prod.brightpearl_id
   , product_id akeneo_sku
   , prod.condition nextgen_condition
   , prod.retail_fos_price
   , kv1['MPN'] mpn
   , lower(trim(BOTH FROM REGEXP_REPLACE(kv1['MPN'], '[^a-zA-Z0-9]', ''))) trimed_mpn
   , kv1['UPC'] upc
   , (CASE WHEN (kv1['UPC'] <> '0') THEN trim(BOTH '0' FROM kv1['UPC']) ELSE kv1['UPC'] END) trimed_upc
   , kv1['Brand'] brand
   , REGEXP_REPLACE(lower(kv1['Brand']), '[^a-zA-Z0-9]', '') trimed_brand
   , REGEXP_REPLACE(lower(legacy_brand), '[^a-zA-Z0-9]', '') leg_trimed_brand
   , kv1['Catalog_item_id'] ake_catalog_item_id
   FROM
     (((
      SELECT
        product_id
      , map_agg(attribute_name, attribute_value) kv1
      FROM
        products_fnd.product_attributes
      WHERE (attribute_name IN ('MPN', 'UPC', 'Brand'))
      GROUP BY product_id
   )  akeneo
   INNER JOIN (select * from products_fnd.products where legacyinventoryid is null) prod ON (akeneo.product_id = prod.akeneo_sku))
   LEFT JOIN gogotech_nextgen.nextgen_to_legacy_brand_mappings ma ON (akeneo.kv1['Brand'] = ma.nextgen_brand))
   WHERE (prod.bundle = false) and prod.brightpearl_status <> 'DISCONTINUED'
) 
, mapped_directly AS (
   WITH
     mapped AS (
      SELECT
        nd.brightpearl_sku
      , nd.brightpearl_id
      , nd.akeneo_sku
      , nd.mpn nextgen_mpn
      , nd.upc nextgen_upc
      , nd.brand nextgen_brand
      , nd.nextgen_condition
      , cast(ld.catalog_item_id as varchar) catalog_item_id
      , cast(ld.inventory_item_id as varchar) inventory_item_id
      , ld.legacy_sku
      , cast(ld.model_id as varchar) model_id
      , ld.mpn legacy_mpn
      , ld.upc legacy_upc
      , ld.barcode
      , ld.brand legacy_brand
      , ld.legacy_condition
      , ld.current_product_price
      , ld.discount
      , ld.attribute_name
      , ld.syosset_qty
      , ld.tucson_qty
      , ld.live_fba_qty
      , ld.wac
      , ld.web_map_price
      , ld.asin
      , false is_bundle
      FROM
        (nextgen_details nd
      INNER JOIN legacy_details ld ON ((nd.trimed_mpn = ld.trimed_mpn and (nd.trimed_brand = ld.trimed_brand or nd.leg_trimed_brand = ld.trimed_brand) AND nd.nextgen_condition = ld.legacy_condition) OR (nd.trimed_upc = ld.trimed_upc and nd.nextgen_condition = ld.legacy_condition)) )
   ) 
   SELECT *
   FROM
     (
      SELECT
        *
      , row_number() OVER (PARTITION BY brightpearl_sku ORDER BY current_product_price ASC) rank
      FROM
        mapped
   ) 
   WHERE (rank = 1)
) 
, mapped_products AS (
   SELECT *
   FROM
     avinash_mappings
UNION
      SELECT DISTINCT *
      FROM
     mapped_directly
)
   
, bundle_mapped as (
    with bundle_mapping as (
        SELECT
        m.brightpearl_sku
      , m.brightpearl_id
      , m.akeneo_sku
      , m.mpn nextgen_mpn
      , m.upc nextgen_upc
      , m.brand nextgen_brand
      , m.condition nextgen_condition
      , m.catalogid catalog_item_id
      , null inventory_item_id
      , m.modelid model_id
      , pc.identifier legacy_sku
      , null legacy_mpn
      , null legacy_upc
      , null barcode
      , null legacy_brand
      , null legacy_condition
      , pr.current_product_price
      , pr.discount
      , null attribute_name
      , null syosset_qty
      , null tucson_qty
      , null live_fba_qty
      , null wac
      , null web_map_price
      , null asin
      , true is_bundle
        from (select distinct brightpearl_id,akeneo_sku,mpn,upc,brand,condition,catalogid, brightpearl_sku, modelid from products_fnd.products where bundle and catalogid is not null and brightpearl_sku is not null) as m
        left join gogotech_launch.pc_catalog_items as pc on m.catalogid = cast(pc.catalog_item_id as varchar)
        left join price as pr on (m.catalogid = cast(pr.catalog_item_id as varchar))
        where m.brightpearl_sku not in (select distinct brightpearl_sku from mapped_products)
    )
    
    SELECT *
   FROM
     (
      SELECT
        *, row_number() OVER (PARTITION BY brightpearl_sku ORDER BY current_product_price ASC) rank
      FROM
        bundle_mapping
   ) 
   WHERE (rank = 1)
)

SELECT
  brightpearl_sku
, brightpearl_id
, akeneo_sku
, nextgen_mpn
, nextgen_upc
, nextgen_brand
, nextgen_condition
, catalog_item_id
, inventory_item_id
, legacy_sku
, model_id
, legacy_mpn
, legacy_upc
, barcode
, legacy_brand
, legacy_condition
, current_product_price
, discount
, attribute_name
, syosset_qty
, tucson_qty
, live_fba_qty
, wac
, web_map_price
, asin
, is_bundle
FROM
  mapped_products
UNION SELECT
  brightpearl_sku
, brightpearl_id
, akeneo_sku
, nextgen_mpn
, nextgen_upc
, nextgen_brand
, nextgen_condition
, cast(catalog_item_id as varchar) catalog_item_id
, cast(inventory_item_id as varchar) inventory_item_id
, legacy_sku
, model_id
, legacy_mpn
, legacy_upc
, barcode
, legacy_brand
, legacy_condition
, current_product_price
, discount
, attribute_name
, syosset_qty
, tucson_qty
, live_fba_qty
, wac
, web_map_price
, asin
, is_bundle
FROM
  bundle_mapped
"""

file_path = f"s3://gogotech-datalake/foundation/nextgen_to_legacy_sku_mapping"

def save_df(data, db, tb, file_path_, partition_cols=None):
    if partition_cols is None:
        partition_cols = []

    try:
        return wr.s3.to_parquet(
            data,
            dataset=True,
            mode='overwrite_partitions',
            database=db,
            boto3_session=boto3_session,
            table=tb,
            path=file_path_,
            partition_cols=partition_cols,
            catalog_versioning=True,
            schema_evolution=True,
        )
    except:
        glue = boto3_session.client('glue')
        print(glue.delete_table(DatabaseName=db, Name=tb))

        return wr.s3.to_parquet(
            data,
            dataset=True,
            mode='overwrite',
            database=db,
            boto3_session=boto3_session,
            table=tb,
            path=file_path_,
            partition_cols=partition_cols,
            catalog_versioning=True,
            schema_evolution=True,
        )

if __name__ == '__main__':
    mapping_df = wr.athena.read_sql_query(
        keep_files=False,
        sql=query,
        database="gogotech_nextgen",
        workgroup="primary",
        boto3_session=boto3_session
    )
    
    save_df(
        data = mapping_df,
        db = "gogotech_nextgen",
        tb = "nextgen_to_legacy_sku_mappings",
        file_path_ = file_path
        )







