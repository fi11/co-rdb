var expect = require('chai').expect;
var co = require('co');
var rdb = require('../index');
var r = require('rethinkdb');

var opt = { host:'localhost', port: 28015, db: 'test' };
var conn;

before(function(done) {
    r.connect(opt)
        .then(function(connection) {
            conn = connection;

            return r.dbDrop('test').run(conn)
                .then(function() {
                    return r.dbCreate('test').run(conn);
                })
                .then(function() {
                    return r.tableCreate('t1').run(conn);
                })
                .finally(function() {
                    return r.table('t1').insert({ id: 1, cnt: 'test1' }).run(conn).then(function() {
                        return r.table('t1').insert({ id: 2, cnt: 'test2' }).run(conn);
                    });
                })
        })
        .finally(function() {
            done();
        });
});

describe('Connection', function() {
    it('Should create connection', function(done) {
        co(function *() {
            var conn = yield rdb.conn(opt);

            r.dbList().run(conn, function(err, res) {
                expect(err).to.be.null;
                done();
            });
        })();
    });
});

describe('Run query', function() {
    it('Should run query', function(done) {
        co(function *(){
            var res = yield rdb.run(r.table('t1').get(1), conn);

            expect(res).to.eql({ id: 1, cnt: 'test1' });
        })(done);
    });

    it('Should run query with conn as context', function(done) {
        co(function *(){
            var res = yield rdb.run.call(conn, r.table('t1').get(1));

            expect(res).to.eql({ id: 1, cnt: 'test1' });
        })(done);
    });

    it('Should apply toArray for cursor', function(done) {
        co(function *(){
            var res = yield rdb.run(r.table('t1'), conn);

            expect(res.map(function(i) { return i.cnt }))
                .to.include.members(['test1', 'test2']);
        })(done);
    });
});

describe('Pool', function() {
    it('Should have acquire property', function(done) {
        co(function *(){
            var pool = rdb.createPool(opt);

            expect(pool.acquire).to.exist;
        })(done);
    });

    it('Should run query with pool', function(done) {
        co(function *(){
            var pool = rdb.createPool(opt);

            var res = yield rdb.run(r.table('t1').get(1), pool);

            expect(res).to.eql({ id: 1, cnt: 'test1' });
        })(done);
    });

    it('Should run query with pool as context', function(done) {
        co(function *(){
            var pool = rdb.createPool(opt);

            var res = yield rdb.run.call(pool, (r.table('t1').get(1)));

            expect(res).to.eql({ id: 1, cnt: 'test1' });
        })(done);
    });
});

describe('Setup db', function() {
    before(function(done) {
        co(function *(){
            try { yield rdb.run(r.dbDrop('test'), conn); } catch(err) {}
        })(done);
    });

    it('Should create table t2 with id as primary key', function(done) {
        co(function *(){
            var conf = { db: 'test', tables: { t2: {} } };

            yield rdb.setup(conf, conn);
            var info = yield rdb.run(r.table('t2').info(), conn);

            expect(info.primary_key).to.equal('id');
        })(done);
    });

    it('Should create table t3 with pk as primary key', function(done) {
        co(function *(){
            var conf = { db: 'test', tables: { t3: { pk: 'pk' } } };

            yield rdb.setup(conf, conn);
            var info = yield rdb.run(r.table('t3').info(), conn);

            expect(info.primary_key).to.equal('pk');
        })(done);
    });

    it('Should create table t4 with sk as secondary index', function(done) {
        co(function *(){
            var conf = { db: 'test', tables: { t4: { sk: 'sk' } } };

            yield rdb.setup(conf, conn);
            var info = yield rdb.run(r.table('t4').info(), conn);

            expect(info.indexes).to.eql(['sk']);
        })(done);
    });

    it('Should create table t5 with pk and sk as secondary index', function(done) {
        co(function *(){
            var conf = { db: 'test', tables: { t5: { pk: 'pk', sk: 'sk' } } };

            yield rdb.setup(conf, conn);
            var info = yield rdb.run(r.table('t5').info(), conn);

            expect(info.primary_key).to.equal('pk');
            expect(info.indexes).to.eql(['sk']);
        })(done);
    });

    it('Should create t6 and t7 tables', function(done) {
        co(function *(){
            var conf = { db: 'test', tables: { t6: {}, t7: {} } };

            yield rdb.setup(conf, conn);
            var info1 = yield rdb.run(r.table('t6').info(), conn);
            var info2 = yield rdb.run(r.table('t6').info(), conn);

            expect(info1.primary_key).to.equal('id');
            expect(info2.primary_key).to.equal('id');
        })(done);
    });

    it('Should create table t8 with multi secondary index', function(done) {
        co(function *(){
            var conf = { db: 'test', tables: { t8: { sk: { name: 'sk', multi: true } } } };

            yield rdb.setup(conf, conn);
            var info = yield rdb.run(r.table('t8').indexStatus(), conn);

            expect(info[0]).to.eql({ index: 'sk', ready: true } );
        })(done);
    });

    it('Should create table t9 with multi compound index', function(done) {
        co(function *(){
            var conf = {
                db: 'test',
                tables: { t9: { sk: { name: 'foo', fields:  [r.row("bar"), r.row("baz")] } } }
            };

            yield rdb.setup(conf, conn);
            var info = yield rdb.run(r.table('t9').indexStatus(), conn);

            expect(info[0]).to.eql({ index: 'foo', ready: true } );
        })(done);
    });
});

describe('Clear table', function() {
    var conf = {
        db: 'test',
        tables: { one: {}, two: {}, three: {} }
    };

    beforeEach(function(done) {
        co(function *(){
            try { yield rdb.run(r.dbDrop('test'), conn); } catch(err) {}
        })(done);
    });

    it('Should clear only tables from list', function(done) {
        co(function *(){
            yield rdb.setup(conf, conn);

            var queries = [
                rdb.run(r.table('one').insert({ data: 'test '}), conn),
                rdb.run(r.table('two').insert({ data: 'test '}), conn),
                rdb.run(r.table('three').insert({ id: 3, data: 'test'}), conn)
            ];

            yield queries;

            yield rdb.clear(['one', 'two'], conn);

            queries = {
                one: rdb.run(r.table('one'), conn),
                two: rdb.run(r.table('two'), conn),
                three: rdb.run(r.table('three'), conn)
            };

            var res = yield queries;

            expect([].concat(res.one, res.two, res.three)).to.eql([{ data: 'test', id: 3 }]);
        })(done);
    });

    it('Should clear all tables', function(done) {
        co(function *(){
            yield rdb.setup(conf, conn);

            var queries = [
                rdb.run(r.table('one').insert({ data: 'test '}), conn),
                rdb.run(r.table('two').insert({ data: 'test '}), conn),
                rdb.run(r.table('three').insert({ id: 3, data: 'test'}), conn)
            ];

            yield queries;
            yield rdb.clear(conn);

            queries = {
                one: rdb.run(r.table('one'), conn),
                two: rdb.run(r.table('two'), conn),
                three: rdb.run(r.table('three'), conn)
            };

            var res = yield queries;

            expect([].concat(res.one, res.two, res.three)).to.eql([]);
        })(done);
    });
});
