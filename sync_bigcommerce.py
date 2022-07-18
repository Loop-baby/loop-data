from http import client
import json
import requests as rq
import time

from retrying import retry
from datetime import datetime
from email.utils import format_datetime, formatdate, parsedate_to_datetime

from packages.schema import Schema
from packages.config import Config
from packages.s3_resource import S3
from packages.redshift_resource import Redshift


class BigCommerce:
    def __init__(self, access_token, store_hash, client_id, client_secret):
        self.store_hash = store_hash
        self.access_token = access_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.headers = {
            "X-Auth-Token": self.access_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        self.urls = {
            "customers": f'https://api.bigcommerce.com/stores/{self.store_hash}/v3/customers', 
            "orders": f'https://api.bigcommerce.com/stores/{self.store_hash}/v2/orders',
            "products": f'https://api.bigcommerce.com/stores/{self.store_hash}/v3/catalog/products',
            "refunds": f'https://api.bigcommerce.com/stores/{self.store_hash}/v3/orders/payment_actions/refunds',
            "line_items": f'https://api.bigcommerce.com/stores/{self.store_hash}/v2/orders/order_id_here/products',
            "shipping_addresses": f'https://api.bigcommerce.com/stores{store_hash}/v2/orders/order_id_here/shipping_addresses'
        }

    def check_limits(self, response):
        limit = response.headers['x-rate-limit-requests-left']
        if int(limit) < 10:
            print(f"Request limit nearing sleeping: 30 seconds")
            time.sleep(30)

    def get_orders(self, table, filter_ms):
        """
        Have to separate this from the other pulls due to it being a v2 object. v3 is not available for orders.
        V3 has a simple pagination system, v2 requires enough changes to put it in a separate function
        """
        all_data = []
        url = self.urls[table]
        params = {
            "min_date_modified": format_datetime(filter_ms),
        }

        response = rq.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        self.check_limits(response)
        data = response.json()
        all_data.append(data)

        while len(data) == 50:
            new_min_date_modified = max(parsedate_to_datetime(i["date_modified"] for i in data))
            params = {
                "min_date_modified": format_datetime(new_min_date_modified)
            }

            response = rq.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            self.check_limits(response)
            data = response.json()
            all_data.append(data)

        all_data = [item for sublist in all_data for item in sublist]

        return all_data  


    def get_dependent_data(self, table, order_data):
        """
        Orders does not contain line items or shipping addresses and there is not a way to pull them all at once so this needs to be called
        for each new order to get the line items and shipping addresses
        """

        all_data = []
        order_ids = [val[0] for val in order_data]

        for order_id in order_ids:
            url = self.urls[table].replace('order_id_here', order_id)
            response = rq.get(url, headers=self.headers)
            response.raise_for_status()
            self.check_limits(response)
            data = response.json()
            all_data.append(data)
        
        all_data = [item for sublist in all_data for item in sublist]

        return all_data


    def get_data(self, table, filter_ms, audit_col):
        if table != "refunds":
            params = {
                f"{audit_col[0]}:min": filter_ms.date().isoformat()
            }

        else:
            params = {}

        url = self.urls[table]
        all_data = []
        current_page = 1
        total_pages = 5

        while current_page <= total_pages:
            response = rq.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            self.check_limits(response)
            data = response.json()
            # import ipdb;ipdb.set_trace()
            current_page = data['meta']['pagination']['current_page']
            total_pages = data['meta']['pagination']['total_pages']
            all_data.append(data['data'])
            params["page"] = current_page + 1

        all_data = [item for sublist in all_data for item in sublist]

        return all_data           


class RedshiftBigcommerceSyncer:
    def __init__(self, s3, redshift, bigcommerce, target_schema):
        self.s3 = s3
        self.redshift = redshift
        self.bigcommerce = bigcommerce
        self.schema = Schema(target_schema)
        self.extract_ts = datetime.utcnow()
        self.s3_prefix = target_schema

        self.redshift.createSchema(self.schema)

        self.tables = self.schema.getSchemaTables()

    def get_last_ts(self, table_name):
        audit_cols = self.schema.schema_def[table_name].get('audit_cols')
        if not audit_cols:
            return None, None
        audit_col_str = audit_cols[0] if len(audit_cols) == 1 else f'GREATEST({",".join(audit_cols)})'
        query = """
            SELECT MAX({})
            FROM {}.{}
        """.format(
            audit_col_str,
            self.schema.name,
            table_name
        )
        max_extract = self.redshift.executeQuery(query)[0]
        
        if max_extract[0]:
            ts = max_extract[0]#.isoformat()
        else:
            ts = datetime(2019, 1, 1)#.isoformat()
        return ts, audit_cols

    def sync(self):
        for table in self.tables:
            if table != 'line_items':
                self.sync_table(table)

    def sync_table(self, table_name, dependent_data=None, init=False):
        self.redshift.createTable(self.schema, table_name)        
        if init:
            self.redshift.truncate(self.schema, table_name)

        s3_path = f"{self.s3_prefix}/{table_name}/{datetime.utcnow().isoformat()}"
        field_names = self.schema.getTableFields(table_name)

        filter_ms, audit_col = self.get_last_ts(table_name)


        if table_name == 'orders':
            data = self.bigcommerce.get_orders(table_name, filter_ms)
            dependent_tables = {'line_items', 'shipping_addresses'}
        elif table_name in ('line_items', 'shipping_addresses'):
            data = self.bigcommerce.get_dependent_data(table_name, filter_ms)
            dependent_table = {}
        else:
            data = self.bigcommerce.get_data(table_name, filter_ms, audit_col)
            dependent_table = {}

        clean_data = [{k: v for k, v in row.items() if k in field_names} for row in data]

        self.s3.stream_dict_writer(clean_data, field_names, s3_path)

        s3_url = 's3://' + self.s3.bucket + '/' + s3_path
        self.redshift.upsertFromS3(self.schema, table_name, s3_url)

        if dependent_tables != {}:
            call_dependent_tables(dependent_tables, clean_data)

    def call_dependent_tables(self, dependent_tables, clean_data):
        for dependent_table in dependent_tables:
            self.sync_table(dependent_table, dependent_data=clean_data)

if __name__ == '__main__':
    TARGET_SCHEMA = 'bigcommerce'
    config = Config('config/config.yaml').getConfigObject()
    redshift_client = Redshift(**config['Resources']['redshift'])
    s3_client = S3(**config['Resources']['s3'])
    bigcommerce_client = BigCommerce(**config['Resources']['bigcommerce'])
    syncer = RedshiftBigcommerceSyncer(s3_client, redshift_client, bigcommerce_client, TARGET_SCHEMA)
    syncer.sync()