import 'dart:async';
import 'dart:convert';
import 'package:logging/logging.dart';
import 'package:postgres_pool/postgres_pool.dart';
import 'package:needle_orm/needle_orm.dart';

import 'postgres.dart';

/// A [QueryExecutor] that uses `package:postgres_pool` for connetions pooling.
class PostgreSqlPoolDataSource extends DataSource {
  final PgPool _pool;

  /// An optional [Logger] to print information to.
  late Logger logger;

  PostgreSqlPoolDataSource(this._pool, {Logger? logger})
      : super(DatabaseType.PostgreSQL, '10.0') {
    this.logger = logger ?? Logger('PostgreSqlPoolDataSource');
  }

  /// The underlying connection pooling.
  PgPool get pool => _pool;

  /// Closes all the connections in the pool.
  Future<dynamic> close() {
    return _pool.close();
  }

  /// Run query.
  @override
  Future<PostgreSQLResult> execute(
      String tableName, String sql, Map<String, dynamic> substitutionValues,
      [List<String> returningFields = const []]) {
    if (returningFields.isNotEmpty) {
      var fields = returningFields.join(', ');
      var returning = 'RETURNING $fields';
      sql = '$sql $returning';
    }

    logger.fine('Query: $sql');
    logger.fine('Params: $substitutionValues');

    // Convert List into String  @TODO seems not work
    var param = <String, dynamic>{};
    substitutionValues.forEach((key, value) {
      if (value is List) {
        param[key] = jsonEncode(value);
        logger.fine('\t json: $key => ${param[key]}');
      } else {
        param[key] = value;
      }
    });

    return _pool.run<PostgreSQLResult>((pgContext) async {
      return await pgContext.query(sql, substitutionValues: param);
    });
  }

  /// Run query in a transaction.
  @override
  Future<T> transaction<T>(FutureOr<T> Function(DataSource) f) async {
    return _pool.runTx((pgContext) async {
      var exec = PostgreSqlDataSource(pgContext, logger: logger);
      return await f(exec);
    });
  }
}