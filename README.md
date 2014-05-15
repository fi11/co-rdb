Rethinkdb driver wrapper for [Co](https://github.com/visionmedia/co) and helpers for testing.

## Installation

```
$ npm install rethinkdb
$ npm install co-rdb
```

## Example

```js
var rdb = require('co-rdb');
var r = require('rethinkdb');

co(function *() {
    var conf = {
        db: 'someDbName', // default test
        host: '1.1.1.1', // default localhost
        port: '8888', // default 28015
        tables: {
            firstTableName: 'primaryKey',
            secondTableName: '',  // default id
            tableNameWithSecondaryIndex: ['primaryKey', 'secondaryIndex']
        }
    };

    var conn = yield rdb.conn(opt); // create connection
    yield rdb.setup(conf); // create db and tables

    yield rdb.run(r.table('test).insert({ data: 'awesome data' }), conn) // run db query
    yield rdb.run(r.table('test), conn) // run db query and apply toArray for cursor

    yield rdb.clear(['firstTableName', 'secondTableName'], conn) // clear tables
    yield rdb.clear(conn) // clear all tables

})();
```

## Running tests

```
$ make test
```

## Authors

  - [Pavel Silin](https://github.com/fi11)

# License

  MIT
