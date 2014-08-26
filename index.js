var rdb;

try { rdb = require('rethinkdb') } catch (err) {}

function getConnection(options) {
    return function(fn) {
        if (!rdb) fn(new Error('Can`t resolve rethinkdb module'));
        else return rdb.connect(options, fn);
    }
}

function runQuery(query, conn) {
    return function(fn) {
        return query.run(conn, function(err, resultOrCursor) {
            if (err)
                fn(err);
            else if (resultOrCursor && resultOrCursor.toArray)
                resultOrCursor.toArray(function(err, res) { if (err) fn(err); else fn(null, res); });
            else
                fn(null, resultOrCursor);
        });
    };
}

function *setup(conf, conn) {
    if (!conn)
        conn = yield getConnection({ host: conf.host || 'localhost', port: conf.port || 28015, db: conf.db || 'test' });

    if (conf.force)
        try { yield runQuery(rdb.dbDrop(conf.db || 'test'), conn) } catch (err) {}

    try { yield runQuery(rdb.dbCreate(conf.db || 'test'), conn) } catch (err) {}

    var tables = conf.tables || {};
    var queries = [];

    Object.keys(tables || {}).forEach(function(name) {
        var table = tables[name] || {};
        var indexParams = { pk: table.pk || 'id', sk: table.sk || null };

        queries.push(function *() {
            yield runQuery(rdb.tableCreate(name, { primaryKey: indexParams.pk }), conn);

            //TODO: add sk as array
            if (indexParams.sk) {
                yield runQuery(rdb.table(name).indexCreate(indexParams.sk), conn);
            }
        });
    });
    
    try {
        yield queries;
    } catch (err) {
        console.log(err);
    }

    return true;
}

function *clearTables(tables, conn) {
    if (arguments.length === 1) {
        conn = tables;
        tables = yield runQuery(rdb.tableList(), conn);
    }

    while(tables.length) {
        var name = tables.pop();
        try { yield runQuery(rdb.table(name).delete(), conn) } catch (err) {}
    }

    return true;
}

exports.run = runQuery;
exports.conn = getConnection;
exports.setup = setup;
exports.clear = clearTables;
