
__version__ = '0.0.1'
__author__ = 'Willem Bult'
__email__ = 'willem.bult@gmail.com'
__doc__ = '''
dynamo datastore implementation.

Tested with:
* boto 2.9.9

'''
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.types import NUMBER, STRING
from boto.exception import JSONResponseError

from copy import deepcopy

import time
import datastore.core
import json
from datastore.core import Key, Namespace
from bson import json_util
from decimal import *

class Doc(object):
    '''Document key constants for datastore documents.'''
    _id = '_id'
    key = 'key'
    hashkey = '_partition'
    value = 'val'
    wrapped = '_wrapped'

class DynamoDatastore(datastore.Datastore):
    '''Represents a AWS DynamoDB database as a datastore.

    Hello World:

      >>> import datastore.dynamo
      >>> import boto
      >>>
      >>> conn = boto.dynamodb2.connect_to_region('us-west-2', aws_access_key_id='key',aws_secret_access_key='key')
      >>> ds = datastore.dynamo.DynamoDatastore(conn)
      >>>
      >>> hello = datastore.Key('hello')
      >>> ds.put(hello, 'world')
      >>> ds.contains(hello)
      True
      >>> ds.get(hello)
      'world'
      >>> ds.delete(hello)
      >>> ds.get(hello)
      None

    '''
    @staticmethod
    def _table_has_range_key(key):
        return '.' in key.name

    @staticmethod
    def _table_name_for_key(key):
        '''Returns the name of the table to house objects with `key`.
        Users can override this function to enforce their own table naming.
        '''
        # use the second to last namespace as range key
        table_path = key.path

        name = str(table_path)[1:]        # remove first slash.
        name = name.replace(':', '_')   # no : allowed in collection names, use _
        name = name.replace('/', '_')   # no / allowed in collection names, use _
        name = name or '_'              # if collection name is empty, use _

        return name

    @staticmethod
    def _should_pickle(key, val):
        return not key in [Doc.key, Doc.hashkey, Doc.wrapped, Doc._id]

    @staticmethod
    def _wrap_value(value):
        # We want to preserve data types as much as possible, so that querying remains intuitive
        # i.e. 2 < 10 whereas '10' < '2'
        if isinstance(value, basestring):
            return value
        elif type(value) in [int, long, float]:
            return value
        else:
            return '__json__=' + json.dumps(value, default=json_util.default)

    @staticmethod
    def _wrap(table, key, value):
        '''Returns a value to insert. Non-documents are wrapped in a document.'''
        
        if not isinstance(value, dict):
            wrapped = {Doc.value:DynamoDatastore._wrap_value(value), Doc.wrapped:True}
        else:
            wrapped = dict( (k, DynamoDatastore._wrap_value(v)) for (k,v) in value.iteritems() if DynamoDatastore._should_pickle(k,v) )
        
        if table.hash_key == Doc.hashkey:
            pk = table.primary_key_from_key(key)
            wrapped[Doc.hashkey] = pk[table.hash_key]
        table.validate_key_for_value(key, wrapped)
        
        wrapped[Doc.key] = str(key)

        return wrapped

    @staticmethod
    def _unwrap_value(value):
        if isinstance(value, basestring):
            if value[0:9] == '__json__=':
                return json.loads(value[9:], object_hook=json_util.object_hook)
            else:
                return value
        elif isinstance(value, Decimal):
            if int(value) == value:
                return int(value)
            elif long(value) == value:
                return long(value)
            else:
                return float(value)
        return value

    @staticmethod
    def _unwrap(value):
        '''Returns a value to return. Wrapped-documents are unwrapped.'''

        if value is not None and Doc.wrapped in value and value[Doc.wrapped]:
          return DynamoDatastore._unwrap_value(value[Doc.value])

        if isinstance(value, dict):
            if Doc._id in value:
                del value[Doc._id]
            if Doc.hashkey in value:
                del value[Doc.hashkey]

        for k,v in value.iteritems():
            if DynamoDatastore._should_pickle(k,v):
                value[k] = DynamoDatastore._unwrap_value(v)

        return value

    def __init__(self, conn, prefix=""):
        self.conn = conn
        self.prefix = prefix

        # Tables
        self._tables = {}

    def _create_table(self, name, range_key=False):
        if range_key:
            schema = [
                HashKey(Doc.hashkey, data_type=STRING),
                RangeKey(Doc.key, data_type=STRING)
            ]
        else:
            schema = [
                HashKey(Doc.key, data_type=STRING) # by default, use single index
            ]

        Table.create(name, schema=schema, connection=self.conn)


    def _table(self, key):
        '''Returns the `table` corresponding to `key`.'''
        name = self.prefix + self._table_name_for_key(key)

        if not self._tables.get(name, None):
            # Let boto figure out the schema, so we don't have to worry about it
            # This comes at the cost of an extra call
            table = DynamoTable(name, connection=self.conn)

            # If we don't know yet for sure this table exists, check
            if not table.exists():
                self._create_table(name, range_key=DynamoDatastore._table_has_range_key(key))

            while not table.ready:
                time.sleep(1)
                table.prepare()

            self._tables[name] = table

        return self._tables[name]

    def get(self, key):
        '''Return the object named by key.'''
        table = self._table(key)
        item = table.get_item(**table.primary_key_from_key(key))
        return self._unwrap(item._data) if item and item._data != {} else None

    def put(self, key, value):
        '''Stores the object.'''
        table = self._table(key)

        value = self._wrap(table, key, value)
        item = Item(table, data=value)
        item.save(overwrite=True)

    def delete(self, key):
        '''Removes the object.'''
        table = self._table(key)
        table.delete_item(**table.primary_key_from_key(key))

    def contains(self, key):
        '''Returns whether the object is in this datastore.'''
        return self.get(key) is not None

    def query(self, query):
        '''Returns a sequence of objects matching criteria expressed in `query`'''
        table = self._table(query.key.child('_'))
        return DynamoQuery.translate(table, query)

class DynamoTable(Table):
    KEY_SEPARATOR = '.'

    def exists(self):
        try:
            self.prepare()
            return True
        except JSONResponseError, e:
            return False

    def prepare(self):
        status = self.describe()

        if status['Table']['TableStatus'] == 'ACTIVE':             
            self._name = status['Table']['TableName']
            
            def key_by_type_from_schema(schema, type):
                return next((s['AttributeName'] for s in schema if s['KeyType'] == type), None)
            def extract_index(idx):
                return (key_by_type_from_schema(idx['KeySchema'], 'RANGE'), idx['IndexName'])
            def data_type_for_attribute(attr):
                dtype = attr['AttributeType']
                dtype_map = {'N': Decimal, 'S': str}
                if dtype in dtype_map:
                    return dtype_map[dtype]
                else:raise Exception('Unsupported datatype %s for attribute %s in underlying DynamoDB table %s' % (dtype, attr['AttributeName'], self._name))
            def data_type_by_attribute(definitions):
                return dict( (attr['AttributeName'], data_type_for_attribute(attr)) for attr in definitions)

            self._datatypes = data_type_by_attribute(status['Table']['AttributeDefinitions'])
            self._hash_key = key_by_type_from_schema(status['Table']['KeySchema'], 'HASH')
            self._range_key = key_by_type_from_schema(status['Table']['KeySchema'], 'RANGE')
            self._indices = dict(extract_index(idx) for idx in status['Table'].get('LocalSecondaryIndexes', []))
            self._ready = True

    @property
    def name(self):
        return self._name if hasattr(self, '_name') else None

    @property
    def ready(self):
        return hasattr(self, '_ready') and self._ready

    @property
    def datatypes(self):
        return self._datatypes if hasattr(self, '_datatypes') else None

    @property
    def indices(self):
        return self._indices if hasattr(self, '_indices') else []

    @property
    def hash_key(self):
        return self._hash_key if hasattr(self, '_hash_key') else None

    @property
    def range_key(self):
        return self._range_key if hasattr(self, '_range_key') else None

    @property
    def keys(self):
        return [k for k in [self.hash_key, self.range_key] if k is not None]

    def validate_key_for_value(self, key, value):
        '''Verifies that the key is in valid format for the specified table.
        When the underlying table uses a range key or a non-default hash key, there
        are certain limitations on the Key format so that we can always deduce the
        Dynamo primary key from the datastore Key
        '''
        valid = True

        if self.range_key:
            hash_val = value.get(self.hash_key, None)
            if not hash_val:
                raise Exception('Underlying DynamoDB table requires the hash key "%s" to be present in the value dictionary' % self.hash_key)

            if self.range_key == Doc.key:
                # PK is (hash_key, Key)
                # Key name needs to be hash_key.rest_of_key
                if type(value) != dict or not key.name.startswith(str(hash_val)):
                    raise Exception('Underlying DynamoDB table requires key name to be %s.[...], was %s while %s == %s' % (self.hash_key, key.name, self.hash_key, str(hash_val)))
            else:
                # PK is (hash_key, range_key) != (hash_key, Key)
                # Key name needs to be hash_key.range_key
                if type(value) != dict or key.name != str(hash_val) + self.KEY_SEPARATOR + str(value.get(self.range_key, '')):
                    raise Exception('Underlying DynamoDB table requires key name to be %s.%s was %s' % (self.hash_key, self.range_key, key.name))
        elif self.hash_key != Doc.key:
            # PK is (hash_key) != (Key)
            # Key name then has to be hash_key
            if type(value) != dict or key.name != str(value.get(self.hash_key, '')):
                raise Exception('Underlying DynamoDB table requires key name to be %s, was %s' % (self.hash_key, key.name))

    def primary_key_from_key(self, key):
        '''Returns the Dynamo primary key for the datastore key,
        depending on the schema of the underlying Dynamo table.
        '''
        hash_key, range_key = None, None

        if self.range_key:
            # When a range key is specified, the hash key is everything before the separator
            hash_key = key.name.split(self.KEY_SEPARATOR)[0]
            if self.range_key == Doc.key:
                # The range key is the full Key
                range_key = str(key)
            else:
                # The range key is the part after the separator
                range_key = key.name.split(self.KEY_SEPARATOR)[1]
        elif self.hash_key != Doc.key:
            # The entire key name
            hash_key = key.name
        else:
            hash_key = str(key)

        # Cast to correct types
        primary_key = {self.hash_key: self.datatypes[self.hash_key](hash_key)}
        if self.range_key:
            primary_key[self.range_key] = self.datatypes[self.range_key](range_key)

        return primary_key


class DynamoCursor(datastore.Cursor):
    def __init__(self, query, iterable):
        super(DynamoCursor, self).__init__(query, iterable)
        self._orig_iterable = self._iterable
        self._iterable = self.unwrap_gen(self._iterable)

    def unwrap_gen(self, iterable):
        for item in iterable:
            yield DynamoDatastore._unwrap(item._data)

    @property
    def last_key(self):
        return self._orig_iterable._last_key_seen

    '''
    
    def next(self):
        next = super(DynamoCursor, self).next()
        if next is not StopIteration:
            next = DynamoDatastore._unwrap(next._data)
        return next
    '''

class DynamoQuery(object):
    '''Translates queries from datastore queries to dynamodb queries.'''
    operators = { '>':'gt', '>=':'ge', '=':'eq', '!=':'ne', '<=':'le', '<':'lt' }

    @classmethod
    def offset_key(self, table, key):
        # Allow passing just the key as a string or Key as well
        if type(key) is dict:
            return key

        if type(key) is str or isinstance(key, datastore.Key):
            return table.primary_key_from_key(key)

    @classmethod
    def index_for_query(cls, table, query):
        filter_fields = [f.field for f in query.filters]
        for (field, idx) in table.indices.iteritems():
            if field in filter_fields:
                return idx
        return None

    @classmethod
    def can_use_query(cls, table, query):
        for f in query.filters:
            if f.field == table.hash_key and f.op == '=':
                return True
        return False

    @classmethod
    def translate(cls, table, query):
        '''Translate given datastore `query` to a mongodb query on `table`.'''

        # If we're looking at a specific hash key, we can query instead of scan
        if cls.can_use_query(table, query):
            kwargs = cls.query_arguments(table, query, full_scan=False)
            idx = cls.index_for_query(table, query)
            if idx:
                kwargs['index'] = idx

            datastore_cursor = table.query(**kwargs)
        else:
            kwargs = cls.query_arguments(table, query, full_scan=True)
            datastore_cursor = table.scan(**kwargs)
        
        # create datastore Cursor with query and iterable of results
        cursor = DynamoCursor(query, datastore_cursor)
        cursor.apply_filter()
        cursor.apply_order()
        return cursor

    @classmethod
    def query_arguments(cls, table, query, full_scan=True):
        # must call find
        if full_scan:
            applicable_filters = query.filters
        else:
            applicable_filters = [f for f in query.filters if f.field in [table.hash_key, table.range_key]]

        kwargs = cls.filters(applicable_filters)

        if query.limit:
            kwargs['limit'] = query.limit
        if len(query.orders) > 0:
            raise Exception('DynamoDatastore does not support query ordering')
        if query.offset:
            raise Exception('DynamoDatastore does not support query offset counts. Use offset_key instead.')
        if query.offset_key:
            kwargs['exclusive_start_key'] = cls.offset_key(table, query.offset_key)

        return kwargs

    @classmethod
    def filter(cls, filter):
        '''Transform given `filter` into a dynamodb filter tuple.'''
        return '%s__%s' % (filter.field, cls.operators[filter.op]), DynamoDatastore._wrap_value(filter.value)

    @classmethod
    def filters(cls, filters):
        '''Transform given `filters` into a dynamodb filter dictionary.'''
        return dict([cls.filter(f) for f in filters])
