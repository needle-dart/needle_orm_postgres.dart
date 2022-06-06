import 'dart:async';
import 'dart:convert';
import 'package:logging/logging.dart';
import 'package:pool/pool.dart';
import 'package:postgres/postgres.dart';
import 'package:needle_orm/needle_orm.dart';

/// A [QueryExecutor] that queries a PostgreSQL database.
class PostgreSqlDataSource extends DataSource {
  final PostgreSQLExecutionContext _connection;

  /// An optional [Logger] to print information to. A default logger will be used
  /// if not set
  late Logger logger;

  PostgreSqlDataSource(this._connection, {Logger? logger})
      : super(DatabaseType.PostgreSQL, '10.0') {
    this.logger = logger ?? Logger('PostgreSqlDataSource');
  }

  /// The underlying connection.
  PostgreSQLExecutionContext get connection => _connection;

  /// Closes the connection.
  @override
  Future<void> close() async {
    if (_connection is PostgreSQLConnection) {
      return (_connection as PostgreSQLConnection).close();
    } else {
      return Future.value();
    }
  }

  @override
  Future<List<List>> query(String sql, Map<String, dynamic> substitutionValues,
      {List<String> returningFields = const [], String? tableName}) {
    if (returningFields.isNotEmpty) {
      var fields = returningFields.join(', ');
      var returning = 'RETURNING $fields';
      sql = '$sql $returning';
    }

    logger.fine('Query: $sql');
    logger.fine('Params: $substitutionValues');

    // expand List first
    var param = <String, dynamic>{};
    substitutionValues.forEach((key, value) {
      if (value is List) {
        var newKeys = [];
        for (var i = 0; i < value.length; i++) {
          var key2 = '${key}_$i';
          param[key2] = value[i];
          newKeys.add('@$key2');
        }

        var strReplace = "(${newKeys.join(',')})";
        sql = sql.replaceAll('@$key ',
            strReplace); // '@$key ' means all key must be followed by a ' ' to prevent mis-replace!
      } else {
        param[key] = value;
      }
    });

    return _connection.query(sql, substitutionValues: param);
  }

  @override
  Future<T> transaction<T>(FutureOr<T> Function(DataSource) f) async {
    if (_connection is! PostgreSQLConnection) {
      return await f(this);
    }

    var conn = _connection as PostgreSQLConnection;
    T? returnValue;

    var txResult = await conn.transaction((ctx) async {
      try {
        logger.fine('Entering transaction');
        var tx = PostgreSqlDataSource(ctx, logger: logger);
        returnValue = await f(tx);

        return returnValue;
      } catch (e) {
        ctx.cancelTransaction(reason: e.toString());
        rethrow;
      } finally {
        logger.fine('Exiting transaction');
      }
    });

    if (txResult is PostgreSQLRollback) {
      //if (txResult.reason == null) {
      //  throw StateError('The transaction was cancelled.');
      //} else {
      throw StateError(
          'The transaction was cancelled with reason "${txResult.reason}".');
      //}
    } else {
      return returnValue!;
    }
  }
}

/// A [QueryExecutor] that manages a pool of PostgreSQL connections.
class PostgreSqlDataSourcePool extends DataSource {
  /// The maximum amount of concurrent connections.
  final int size;

  /// Creates a new [PostgreSQLConnection], on demand.
  ///
  /// The created connection should **not** be open.
  final PostgreSQLConnection Function() connectionFactory;

  /// An optional [Logger] to print information to.
  late Logger logger;

  final List<PostgreSqlDataSource> _connections = [];
  int _index = 0;
  late final Pool _pool;
  final _connMutex = Pool(1);

  PostgreSqlDataSourcePool(this.size, this.connectionFactory, {Logger? logger})
      : super(DatabaseType.PostgreSQL, '10.0') {
    _pool = Pool(size);
    if (logger != null) {
      this.logger = logger;
    } else {
      this.logger = Logger('PostgreSqlDataSourcePool');
    }

    assert(size > 0, 'Connection pool cannot be empty.');
  }

  /// Closes all connections.
  @override
  Future<void> close() async {
    await _pool.close();
    await _connMutex.close();
    Future.wait(_connections.map((c) => c.close()));
  }

  Future _open() async {
    if (_connections.isEmpty) {
      _connections.addAll(await Future.wait(List.generate(size, (_) async {
        logger.fine('Spawning connections...');
        var conn = connectionFactory();
        await conn.open();
        //return conn
        //    .open()
        //    .then((_) => PostgreSqlExecutor(conn, logger: logger));
        return PostgreSqlDataSource(conn, logger: logger);
      })));
    }
  }

  Future<PostgreSqlDataSource> _next() {
    return _connMutex.withResource(() async {
      await _open();
      if (_index >= size) _index = 0;
      return _connections[_index++];
    });
  }

  @override
  Future<List<List>> query(String sql, Map<String, dynamic> substitutionValues,
      {List<String> returningFields = const [], String? tableName}) {
    return _pool.withResource(() async {
      var executor = await _next();
      return executor.query(sql, substitutionValues,
          returningFields: returningFields, tableName: tableName);
    });
  }

  @override
  Future<T> transaction<T>(FutureOr<T> Function(DataSource) f) {
    return _pool.withResource(() async {
      var executor = await _next();
      return executor.transaction(f);
    });
  }
}
