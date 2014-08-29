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
    var conf = {
        db: 'test',
        tables: { t2: {}, t3: { pk: 'pk' }, t4: { sk: 'sk' }, t5: { pk: 'pk', sk: 'sk' } }
    };

    beforeEach(function(done) {
        co(function *(){
            var queries = [
                rdb.run(r.tableDrop('t2'), conn),
                rdb.run(r.tableDrop('t3'), conn),
                rdb.run(r.tableDrop('t4'), conn),
                rdb.run(r.tableDrop('t5'), conn)
            ];

            try { yield queries } catch(err) {}
        })(done);
    });

    it('Should create table t2 with id as primary key', function(done) {
        co(function *(){
            yield rdb.setup(conf, conn);
            var info = yield rdb.run(r.table('t2').info(), conn);

            expect(info.primary_key).to.equal('id');
        })(done);
    });

    it('Should create table t3 with pk as primary key', function(done) {
        co(function *(){
            yield rdb.setup(conf, conn);
            var info = yield rdb.run(r.table('t3').info(), conn);

            expect(info.primary_key).to.equal('pk');
        })(done);
    });

    it('Should create table t4 with sk as secondary index', function(done) {
        co(function *(){
            yield rdb.setup(conf, conn);
            var info = yield rdb.run(r.table('t4').info(), conn);

            expect(info.indexes).to.eql(['sk']);
        })(done);
    });

    it('Should create table t5 with pk and sk as secondary index', function(done) {
        co(function *(){
            yield rdb.setup(conf, conn);
            var info = yield rdb.run(r.table('t5').info(), conn);

            expect(info.primary_key).to.equal('pk');
            expect(info.indexes).to.eql(['sk']);
        })(done);
    });
});

describe('Clear table', function() {
    var conf = {
        db: 'test',
        tables: { one: '', two: '', three: '' }
    };

    beforeEach(function(done) {
        co(function *(){
            var queries = [
                rdb.run(r.tableDrop('one'), conn),
                rdb.run(r.tableDrop('two'), conn),
                rdb.run(r.tableDrop('three'), conn)
            ];

            try { yield queries; } catch(err) {}
        })(done);
    });

    it('Should clear only tables from list', function(done) {
        co(function *(){
            yield rdb.setup(conf, conn);

            yield rdb.run(r.table('one').insert({ data: 'test1 '}), conn);
            yield rdb.run(r.table('two').insert({ data: 'test1 '}), conn);
            yield rdb.run(r.table('three').insert({ id: 3, data: 'test1'}), conn);

            yield rdb.clear(['one', 'two'], conn);

            var one = yield rdb.run(r.table('one'), conn);
            var two = yield rdb.run(r.table('two'), conn);
            var three = yield rdb.run(r.table('three'), conn);

            expect([].concat(one, two, three)).to.eql([{ data: 'test1', id: 3 }]);
        })(done);
    });

    it('Should clear all tables', function(done) {
        co(function *(){
            yield rdb.setup(conf, conn);

            yield rdb.run(r.table('one').insert({ data: 'test1 '}), conn);
            yield rdb.run(r.table('two').insert({ data: 'test1 '}), conn);
            yield rdb.run(r.table('three').insert({ id: 3, data: 'test1'}), conn);

            yield rdb.clear(conn);

            var one = yield rdb.run(r.table('one'), conn);
            var two = yield rdb.run(r.table('two'), conn);
            var three = yield rdb.run(r.table('three'), conn);

            expect([].concat(one, two, three)).to.eql([]);
        })(done);
    });
});
