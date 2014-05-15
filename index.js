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

    for (var name in tables) {
        var indexes = [].concat(tables[name]);

        try { yield runQuery(rdb.tableCreate(name, { primaryKey: indexes[0] || 'id' }), conn); } catch (err) {}

        if (indexes[1]) {
            try { yield runQuery(rdb.table(name).indexCreate(indexes[1]), conn); } catch (err) {}
        }
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
