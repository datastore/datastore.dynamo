# datastore-dynamo

## datastore implementation for AWS DynamoDB

See [datastore](https://github.com/datastore/datastore).


### Install

From pypi (using pip):

    sudo pip install datastore.dynamo

From pypi (using setuptools):

    sudo easy_install datastore.dynamo

From source:

    git clone https://github.com/RentMethod/datastore.dynamo/
    cd datastore.dynamo
    sudo python setup.py install


### License

datastore.dynamo is under the MIT License.

### Contact

datastore.dynamo is written by [Willem Bult](https://github.com/willembult).
It is largely based on [datastore.mongo](https://github.com/datastore/datastore.mongo) written by [Juan Batiz-Benet](https://github.com/jbenet).

Project Homepage:
[https://github.com/RentMethod/datastore.dynamo](https://github.com/RentMethod/datastore.dynamo)


### Hello World

    
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
