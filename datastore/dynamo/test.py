# NOTE: make sure you set aws dynamo information

import unittest
import logging
import boto

from . import DynamoDatastore
from datastore.core.test.test_basic import TestDatastore
from datastore.core.query import Query
from datastore.core.key import Key

from math import floor

aws_access_key = '<aws access key>'
aws_secret_key = '<aws secret key>'

class TestDynamoDatastore(TestDatastore):

  def _deletekeys(self):
    try:
      table = boto.dynamodb2.table.Table('dfadasfdsafdas', connection=self.conn)
      items = list(table.scan())
      for item in items:
        item.delete()
    except:
      pass

  def setUp(self):
    logging.getLogger('boto').setLevel(logging.CRITICAL)

    err = 'Use a real DynamoDB %s. Edit datastore/dynamo/test.py.'
    assert aws_access_key != '<aws access key>', err % 'access key.'
    assert aws_secret_key != '<aws secret key>', err % 'secret key.'
    self.conn = boto.dynamodb2.connect_to_region('us-west-2', aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)
    self._deletekeys() # make sure we're clean :)

  def tearDown(self):
    self._deletekeys() # clean up after ourselves :]
    boto.dynamodb2.table.Table(str(self.pkey)[1:], connection=self.conn).delete()
    del self.conn

  def test_dynamo(self):
    self.ds = DynamoDatastore(self.conn)
    self.subtest_simple([self.ds], numelems=20)

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
