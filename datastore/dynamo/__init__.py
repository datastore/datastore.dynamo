
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
from boto.dynamodb2.types import NUMBER
from boto.exception import JSONResponseError

import time
import datastore.core
from bson import json_util
import json
#import datastore.core


class Doc(object):
    '''Document key constants for datastore documents.'''
    _id = '_id'
    key = 'key'
    rangekey = 'rangekey'
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

    def __init__(self, conn, prefix=""):
        self.conn = conn
        self._indexed = {}
        self.prefix = prefix

    def _tableNamed(self, name):
        '''Returns the `table` named `name`.'''

        # Let boto figure out the schema, so we don't have to worry about it
        # This comes at the cost of an extra call
        table = Table(name, connection=self.conn)
        
        # If we don't know yet for sure this table exists, check
        if self._indexed.get(name, None) is None:
            try:
                status = table.describe()
            except JSONResponseError, e:
                # Create if it doesn't exist yet
                table = Table.create(name, schema=[
                    HashKey(Doc.key),
                    RangeKey(Doc.rangekey, data_type=NUMBER)
                ], connection=self.conn)
                status = table.describe()

            # Wait for it to be active
            while status['Table']['TableStatus'] != 'ACTIVE':
                time.sleep(1)
                status = table.describe()

            self._indexed[name] = True

        return table

    @staticmethod
    def _tableNameForKey(key):
        '''Returns the name of the table to house objects with `key`.
        Users can override this function to enforce their own table naming.
        '''
        name = str(key.path)[1:]        # remove first slash.
        name = name.replace(':', '_')   # no : allowed in collection names, use _
        name = name.replace('/', '.')   # no / allowed in collection names, use .
        name = name or '_'              # if collection name is empty, use _
        return name

    def _table(self, key):
        '''Returns the `table` corresponding to `key`.'''
        return self._tableNamed(self.prefix + self._tableNameForKey(key))

    def _item(self, key):
        # can't use get_item without knowing the range key
        q = {'%s__eq'%Doc.key:str(key), 'limit': 1}
        return next(self._table(key).query(**q), None)

    @staticmethod
    def _should_pickle(key, val):
        return not key in [Doc.key, Doc.rangekey, Doc.value, Doc.wrapped, Doc._id]

    @staticmethod
    def _wrap(key, value, existing=None):
        '''Returns a value to insert. Non-documents are wrapped in a document.'''
        if not isinstance(value, dict) or Doc.key not in value or value[Doc.key] != key:
            value = { Doc.key:key, Doc.value:val, Doc.wrapped:True}

        if Doc._id in value:
            del value[Doc._id]

        value[Doc.rangekey] = existing._data['rangekey'] if existing else time.time()

        for k,v in value.iteritems():
            if DynamoDatastore._should_pickle(k,v):
                value[k] = json.dumps(v, default=json_util.default)

        return value

    @staticmethod
    def _unwrap(value):
        '''Returns a value to return. Wrapped-documents are unwrapped.'''
        if value is not None and Doc.wrapped in value and value[Doc.wrapped]:
          return value[Doc.value]

        if isinstance(value, dict) and Doc._id in value:
            del value[Doc._id]
        if isinstance(value, dict) and Doc.rangekey in value:
            del value[Doc.rangekey]

        for k,v in value.iteritems():
            if DynamoDatastore._should_pickle(k,v):
                value[k] = json.loads(v, object_hook=json_util.object_hook)

        return value

    def get(self, key):
        '''Return the object named by key.'''
        item = self._item(key)
        return self._unwrap(item._data) if item else None

    def put(self, key, value):
        '''Stores the object.'''
        # Need to get the old version to get the range key value
        existing = self._item(key)
        value = self._wrap(str(key), value, existing)

        item = Item(self._table(key), data=value)
        item.save(overwrite=True)

    def delete(self, key):
        '''Removes the object.'''
        item = self._item(key)
        if item:
            item.delete()

    def contains(self, key):
        '''Returns whether the object is in this datastore.'''
        try:
            q = {'%s__eq'%Doc.key:str(key)}
            return self._table(key).query_count(**q) > 0
        except JSONResponseError, e:
            return False

    def query(self, query):
        '''Returns a sequence of objects matching criteria expressed in `query`'''
        table = self._table(query.key.child('_'))
        return DynamoQuery.translate(table, query)

class DynamoCursor(datastore.Cursor):
    @property
    def last_key(self):
        return self._iterable._last_key_seen

    def __iter__(self):
        return super(DynamoCursor, self).__iter__()

    def next(self):
        next = super(DynamoCursor, self).next()
        if next is not StopIteration:
            next = DynamoDatastore._unwrap(next._data)
        return next

class DynamoQuery(object):
    '''Translates queries from datastore queries to dynamodb queries.'''
    operators = { '>':'gt', '>=':'ge', '=':'eq', '!=':'ne', '<=':'le', '<':'lt' }

    @classmethod
    def offset_key(self, table, key):
        # Allow passing just the keyas a string or Key as well
        if type(key) is dict:
            return key

        if type(key) is str or isinstance(key, datastore.Key):
            item = next(table.query(**{'%s__eq' % Doc.key: str(key), 'limit':1}), None)
            if not item:
                raise Exception('Item for offset_key not found')
            return {Doc.key: item._data[Doc.key], Doc.rangekey: item._data[Doc.rangekey]}

    @classmethod
    def translate(self, table, query):
        '''Translate given datastore `query` to a mongodb query on `table`.'''

        # must call find
        kwargs = self.filters(query.filters)

        if query.limit:
            kwargs['limit'] = query.limit
        if len(query.orders) > 0:
            raise Exception('DynamoDatastore does not support query ordering')
        if query.offset:
            raise Exception('DynamoDatastore does not support query offset counts. Use offset_key instead.')
        if query.offset_key:
            kwargs['exclusive_start_key'] = self.offset_key(table, query.offset_key)

        # TODO: Use query on index instead of scan when filtering on an indexed field
        cursor = table.scan(**kwargs)

        # create datastore Cursor with query and iterable of results
        datastore_cursor = DynamoCursor(query, cursor)
        return datastore_cursor

    @classmethod
    def filter(cls, filter):
        '''Transform given `filter` into a dynamodb filter tuple.'''
        return '%s__%s' % (filter.field, cls.operators[filter.op]), filter.value

    @classmethod
    def filters(cls, filters):
        '''Transform given `filters` into a dynamodb filter dictionary.'''
        return dict([cls.filter(f) for f in filters])
