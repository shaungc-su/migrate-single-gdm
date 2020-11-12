import pathlib
import json
from custom_utils.relation_linkage import LINKAGE_TRANSFORM


class ObjectStoreManager:
    def __init__(self, gdm_rid):
        self.filename = f'gdm_related_objects_{gdm_rid}.json'
        self.filepath = pathlib.Path(self.filename)
        if self.filepath.exists():
            with self.filepath.open('r') as f:
                self.store = json.load(f)
                return
        
        self.store = {}
    
    def save(self):
        with self.filepath.open('w') as f:
            json.dump(self.store, f, indent=2)

    def exist(self, item_type, rid_or_pk):
        if item_type in self.store:
            return rid_or_pk in self.store[item_type]['byPK'] or rid_or_pk in self.store[item_type]['byRid']
        
        return False

    def insert(self, parent):
        if not parent['item_type'] in self.store:
            self.store[parent['item_type']] = {
                'byPK': {},
                'byRid': {}
            }
        
        self.store[parent['item_type']]['byRid'][parent['rid']] = parent
        if parent['item_type'] in LINKAGE_TRANSFORM:
            self.store[parent['item_type']]['byPK'][parent[LINKAGE_TRANSFORM[parent['item_type']]]] = parent

    def get(self, item_type, rid_or_pk):
        try:
            item = self.store[item_type]['byPK'][rid_or_pk]
        except KeyError as error:
            item = self.store[item_type]['byRid'][rid_or_pk]
        
        return item
    
    def getAll(self, prioritized_schema_list):
        '''
        :param list prioritized_schema_list: if provided, will use the objects of those schema(s) first in the final `all_objects` list
        '''
        if prioritized_schema_list:
            prioritized_schema_name_set = set(prioritized_schema_list)
            remain_schema_name_set = set(self.store.keys()) - prioritized_schema_name_set
            all_objects = []

            # construct the list
            ordered_schema_name_list = [*prioritized_schema_list, *remain_schema_name_set]

            # filter out schema which doesn't exist in data
            ordered_schema_name_list = list(filter(lambda schema_name: schema_name in self.store, ordered_schema_name_list))

            for schema_name in ordered_schema_name_list:
                all_objects = [*all_objects, *[value for value in self.store[schema_name]['byRid'].values()]]
            
            return all_objects

        return [ value for schemaName in self.store.keys() for value in self.store[schemaName]['byRid'].values() ]