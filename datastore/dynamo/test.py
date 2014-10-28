# NOTE: make sure you set aws dynamo information

import unittest
import logging
import boto
import mock

from . import *
from datastore.core.test.test_basic import TestDatastore
from datastore.core.query import Query
from datastore.core.key import Key

from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, AllIndex, GlobalAllIndex
from boto.dynamodb2.types import NUMBER, STRING
from math import floor

aws_access_key = '<aws access key>'
aws_secret_key = '<aws secret key>'
aws_region = 'us-west-2'

try:
  from test_settings import *
except:
  pass

class TestDynamoDatastore(TestDatastore):

  SIMPLE_TABLE = str(TestDatastore.pkey)[1:]
  INDEXED_TABLE = 'testIndexTable'
  RANGEKEY_TABLE = 'testRangekeyTable'

  def _delete_keys_from_table(self, name):
    try:
      table = boto.dynamodb2.table.Table(name, connection=self.conn)
      items = list(table.scan())
      for item in items:
        item.delete()
    except:
      pass

  def setUp(self):
    logging.getLogger('boto').setLevel(logging.CRITICAL)

    err = 'Use a real DynamoDB %s. Add datastore/dynamo/test_settings.py.'
    assert aws_access_key != '<aws access key>', err % 'access key.'
    assert aws_secret_key != '<aws secret key>', err % 'secret key.'
    self.conn = boto.dynamodb2.connect_to_region(aws_region, aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)
    
    # Create an indexed table 
    table = Table(self.INDEXED_TABLE, connection=self.conn)
    try:
      status = table.describe()
    except:
      table = Table.create(self.INDEXED_TABLE, schema=[
          HashKey('department', data_type=STRING),
          RangeKey('name', data_type=STRING)
      ], indexes=[
        AllIndex('ScoreIndex', parts=[
          HashKey('department'),
          RangeKey('score', data_type=NUMBER)
        ])
      ], global_indexes=[
        GlobalAllIndex('GroupIndex', parts=[
          HashKey('group'),
          RangeKey('age', data_type=NUMBER)
        ])
      ], connection=self.conn)

    # make sure we're clean :)
    self._delete_keys_from_table(self.SIMPLE_TABLE) 
    self._delete_keys_from_table(self.INDEXED_TABLE) 
    self._delete_keys_from_table(self.RANGEKEY_TABLE) 

  def tearDown(self):
    # clean up after ourselves :]
    self._delete_keys_from_table(self.SIMPLE_TABLE) 
    self._delete_keys_from_table(self.INDEXED_TABLE)  
    self._delete_keys_from_table(self.RANGEKEY_TABLE)

    #boto.dynamodb2.table.Table(self.SIMPLE_TABLE, connection=self.conn).delete()
    #boto.dynamodb2.table.Table(self.INDEXED_TABLE, connection=self.conn).delete()
    #boto.dynamodb2.table.Table(self.RANGEKEY_TABLE, connection=self.conn).delete()
    del self.conn
  
  def test_dynamo(self):
    self.ds = DynamoDatastore(self.conn)      
    self.subtest_simple([self.ds], numelems=20)

  def test_dict_query(self):
    self.ds = DynamoDatastore(self.conn)
    pkey = '/' + self.SIMPLE_TABLE
    key = Key(pkey + '/abc')
    test_dict = {'key': str(key), 'a': 3, 'b': {'1':2,'2':3}, 'c': True, 'd': False, 'e': '1.0', 'f': None}
    
    self.ds.put(key, test_dict)

    res = self.ds.get(key)
    assert res == test_dict
    assert res['c'] is True
    assert res['d'] is False

    res = self.ds.query(Query(Key(pkey)).filter('b','=',test_dict['b']))
    first = next(res, None)
    assert first is not None
    assert first == test_dict
  
  def test_rangekey_table(self):
    self.ds = DynamoDatastore(self.conn)
    pkey = '/' + self.RANGEKEY_TABLE
    key = Key(pkey + '/hash1.abc')
    test_dict = {'key': str(key), 'a': 3, 'b': {'1':2,'2':3}}
    self.ds.put(key, test_dict)

    res = self.ds.get(key)
    assert res == test_dict

  def test_indexed_table(self):
    # Assume that a table with indexes exists before we begin. 
    # The driver should see the indexes and use them
    self.ds = DynamoDatastore(self.conn)
    pkey = Key('/' + self.INDEXED_TABLE)

    johnny_key = pkey.child('sales.Johnny')
    johnny = {
      'key': str(johnny_key),
      'department': 'sales',
      'name': 'Johnny',
      'group': 'managers',
      'age': 20,
      'score': 1500
    }
    tom_key = pkey.child('sales.Tom')
    tom = {
      'key': str(tom_key),
      'department': 'sales',
      'name': 'Tom',
      'group': 'employees',
      'age': 30,
      'score': 1000
    }
    barbara_key = pkey.child('online_marketing.Barbara')
    barbara = {
      'key': str(barbara_key),
      'department': 'online_marketing',
      'name': 'Barbara',
      'group': 'managers',
      'age': 40,
      'score': 500
    }
    self.ds.put(johnny_key, johnny)
    self.ds.put(tom_key, tom)
    self.ds.put(barbara_key, barbara)

    query = Query(pkey).filter('age','>',20)
    # since the query does not contain any of the hash keys (department, username), 
    # we'll have to do a scan instead of a query
    with mock.patch.object(Table, 'query', return_value=None) as mock_method:
      res = list(self.ds.query(query))
      assert not mock_method.called
      assert res == [tom, barbara] or res == [barbara, tom]

    # From here on out we don't want to use scan anymore
    with mock.patch.object(Table, 'scan', return_value=None) as mock_method:
      assert self.ds.get(johnny_key) == johnny
      assert self.ds.get(tom_key) == tom
      assert self.ds.get(barbara_key) == barbara

      # Filter on hash key exclusively
      q = Query(pkey).filter('department', '=', 'sales')
      table = self.ds._table(q.key.child('_'))
      idx = DynamoQuery.index_for_query(table, q)
      assert idx.name is None, idx.name 
      assert idx.hash_key == 'department', idx.hash_key
      assert idx.range_key == 'name', idx.range_key
      res = list(self.ds.query(q))
      assert res == [tom, johnny] or res == [johnny, tom]
      
      # Filter on hash and range key
      q = Query(pkey).filter('department', '=', 'sales').filter('name','=','Johnny')
      res = list(self.ds.query(q))
      assert res == [johnny]

      # Filter on hash and range key with comparison
      q = Query(pkey).filter('department', '=', 'sales').filter('name','>','Jason')
      res = list(self.ds.query(q))
      assert res == [johnny, tom] or res == [tom, johnny]

      # Filter on a hash key and an arbitrary other non-indexed key
      q = Query(pkey).filter('department', '=', 'sales').filter('age','>',25)
      res = list(self.ds.query(q))
      assert res == [tom]

      # Filter on a hash key, secondary index, and non-indexed
      q = Query(pkey).filter('department', '=', 'sales').filter('score','>=',25).filter('age','>=',25)
      idx = DynamoQuery.index_for_query(table, q)
      assert idx.name == 'ScoreIndex', idx.name 
      assert idx.hash_key == 'department', idx.hash_key
      assert idx.range_key == 'score', idx.range_key
      args = DynamoQuery.query_arguments(table, q, index=idx)
      assert args == {'score__gte': 25, 'department__eq': 'sales'} # age is not indexed, not in query filters
      res = list(self.ds.query(q))
      assert res == [tom]
      
      # Filter on hash and secondary index key
      q = Query(pkey).filter('department', '=', 'sales').filter('score','>',500)
      res = list(self.ds.query(q))
      assert res == [tom, johnny] or res == [johnny, tom]

      # Filter on global hash key exclusively
      q = Query(pkey).filter('group', '=', 'managers')
      idx = DynamoQuery.index_for_query(table, q)
      assert idx.name == 'GroupIndex', idx.name 
      assert idx.hash_key == 'group', idx.hash_key
      assert idx.range_key == 'age', idx.range_key
      res = list(self.ds.query(q))
      assert res == [johnny, barbara] or res == [barbara, johnny], res
      
      # Filter on global hash and range key
      q = Query(pkey).filter('group', '=', 'managers').filter('age','=',40)
      res = list(self.ds.query(q))
      assert res == [barbara]

      # Filter on global hash and range key with comparison
      q = Query(pkey).filter('group', '=', 'managers').filter('age','<',30)
      res = list(self.ds.query(q))
      assert res == [johnny]

      # Filter on global hash key and an arbitrary other non-indexed key
      q = Query(pkey).filter('group', '=', 'managers').filter('score','>',500)
      res = list(self.ds.query(q))
      assert res == [johnny]

      # Update a secondary index range key
      tom['score'] = 400
      self.ds.put(tom_key, tom)

      # Query using secondary index again, verify updated values
      q = Query(pkey).filter('department', '=', 'sales').filter('score','>',500)
      res = list(self.ds.query(q))
      assert res == [johnny] or res == [johnny]

      # Update a global index rang key
      johnny['age'] = 35
      self.ds.put(johnny_key, johnny)
      
      # Query using global index again, verify updated values
      q = Query(pkey).filter('group', '=', 'managers').filter('age','>',30)
      res = list(self.ds.query(q))
      assert res == [johnny, barbara] or res == [barbara, johnny]

      assert not mock_method.called

      # Try to create an item with the key separator in the hash
      bad_key = pkey.child('datastore.dynamo.Bad')
      bad_item = {
        'key': str(bad_key),
        'department': 'datastore.dynamo',
        'name': 'Bad',
        'group': 'employees',
        'age': 30,
        'score': 1000
      }
      try:
        self.ds.put(bad_key, bad_item)
        assert False, "should have not allowed, bad key"
      except ValueError:
        pass


  def subtest_queries(self):
    for value in range(0, self.numelems):
      key = self.pkey.child(value)
      for sn in self.stores:
        sn.put(key, value)

    k = self.pkey
    n = int(self.numelems)
    
    self.check_query(Query(k), n, slice(0, n))
    self.check_query(Query(k, limit=n), n, slice(0, n))

    # Check pagination (last_key from last result as offset_key)
    result = self.check_query(Query(k, limit=n/2), n, slice(0, n/2))
    self.check_query(Query(k, offset_key=result.last_key), n, slice(n/2, n))
    
    # Check offset with manual offset_key 
    res = list(self.ds.query(Query(k)))
    k_ndiv2 = '%s/%d' % (k, res[int(n/2)-1]) # key name is equal to the value
    self.check_query(Query(k, offset_key=k_ndiv2), n, slice(n/2, n))
    self.check_query(Query(k, offset_key=Key(k_ndiv2)), n, slice(n/2, n))
    del k
    del n

if __name__ == '__main__':
  unittest.main()
