import os
import hubspot
from datetime import datetime

from packages.schema import Schema
from packages.config import Config
from packages.s3_resource import S3
from packages.redshift_resource import Redshift


class Hubspot:
    def __init__(self, api_key):
        self.client = hubspot.HubSpot(api_key=api_key)

    def _flatten_properties(self, data):
        new_data = []
        for record in data:
            dict_record = record.to_dict()
            for k, v in dict_record['properties'].items():
                dict_record[k] = v
            del dict_record['properties']
            new_data.append(dict_record)
        return new_data

    def get_pipelines(self):
        return self.client.crm.pipelines.pipelines_api.get_all('deals').to_dict()['results']

    def get_contacts(self):
        properties = [x['name'] for x in self.get_object_properties('contacts')]
        return self._flatten_properties(self.client.crm.contacts.get_all(properties=properties))

    def get_deals(self):
        properties = [x['name'] for x in self.get_object_properties('deals')]
        return self._flatten_properties(self.client.crm.deals.get_all(properties=properties))

    def get_companies(self):
        return self.client.crm.companies.get_all()

    def get_object_properties(self, object_name):
        return self.client.crm.properties.core_api.get_all(object_name).to_dict()['results']

    def get_object_data(self, object_name):
        object_map = {
            'contacts': self.get_contacts,
            'deals': self.get_deals
        }
        if object_map.get(object_name):
            return object_map[object_name]()
        else:
            raise ValueError(f"unsupported object {object_name}")


class RedshiftHubspotSyncer:
    def __init__(self, s3, redshift, hubspot, target_schema):
        self.s3 = s3
        self.redshift = redshift
        self.hubspot = hubspot
        self.schema = Schema(target_schema)
        self.extract_ts = datetime.utcnow()
        self.s3_prefix = target_schema
        self.objects = [
            'contacts',
            'deals'
        ]
        self.redshift.createSchema(self.schema)

    def get_last_ts(self, table_name):
        ...

    def sync_table(self, table_name):
        s3_path = f"{self.s3_prefix}/{table_name}/{datetime.utcnow().isoformat()}"
        field_names = self.schema.getTableFields(table_name)
        data = self.hubspot.get_object_data(table_name)
        clean_data = [{k: v for k, v in row.items() if k in field_names} for row in data]
        self.s3.stream_dict_writer(clean_data, field_names, s3_path)
        # TODO: Just doing fulls for now
        self.redshift.createTable(self.schema, table_name)
        self.redshift.truncate(self.schema, table_name)
        self.redshift.appendFromS3(self.schema, table_name, 's3://' + self.s3.bucket + '/' + s3_path)

    def sync(self):
        for object in self.objects:
            self.sync_table(object)


if __name__ == '__main__':
    TARGET_SCHEMA = 'hubspot'
    config = Config('config/config.yaml').getConfigObject()
    redshift_client = Redshift(**config['Resources']['redshift'])
    s3_client = S3(**config['Resources']['s3'])
    hubspot_client = Hubspot(**config['Resources']['hubspot'])
    syncer = RedshiftHubspotSyncer(s3_client, redshift_client, hubspot_client, TARGET_SCHEMA)
    syncer.sync()
