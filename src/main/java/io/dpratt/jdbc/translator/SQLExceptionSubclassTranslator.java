package io.dpratt.jdbc.translator;

import io.dpratt.jdbc.*;

import java.sql.*;

/**
 * {@link SQLExceptionTranslator} implementation which analyzes the specific
 * {@link java.sql.SQLException} subclass thrown by the JDBC driver.
 *
 * <p>Falls back to a standard {@link SQLStateSQLExceptionTranslator} if the JDBC
 * driver does not actually expose JDBC 4 compliant {@code SQLException} subclasses.
 *
 * @author Thomas Risberg
 * @author Juergen Hoeller
 * @since 2.5
 * @see java.sql.SQLTransientException
 * @see java.sql.SQLTransientException
 * @see java.sql.SQLRecoverableException
 */
public class SQLExceptionSubclassTranslator extends AbstractFallbackSQLExceptionTranslator {

    public SQLExceptionSubclassTranslator() {
        super(new SQLStateSQLExceptionTranslator());
    }

    @Override
    protected DataAccessException doTranslate(String task, String sql, SQLException ex) {
        if (ex instanceof SQLTransientException) {
            if (ex instanceof SQLTransientConnectionException) {
                return new TransientDataAccessResourceException(buildMessage(task, sql, ex), ex);
            }
            else if (ex instanceof SQLTransactionRollbackException) {
                return new ConcurrencyFailureException(buildMessage(task, sql, ex), ex);
            }
            else if (ex instanceof SQLTimeoutException) {
                return new QueryTimeoutException(buildMessage(task, sql, ex), ex);
            }
        }
        else if (ex instanceof SQLNonTransientException) {
            if (ex instanceof SQLNonTransientConnectionException) {
                return new DataAccessResourceFailureException(buildMessage(task, sql, ex), ex);
            }
            else if (ex instanceof SQLDataException) {
                return new DataIntegrityViolationException(buildMessage(task, sql, ex), ex);
            }
            else if (ex instanceof SQLIntegrityConstraintViolationException) {
                return new DataIntegrityViolationException(buildMessage(task, sql, ex), ex);
            }
            else if (ex instanceof SQLInvalidAuthorizationSpecException) {
                return new PermissionDeniedDataAccessException(buildMessage(task, sql, ex), ex);
            }
            else if (ex instanceof SQLSyntaxErrorException) {
                return new BadSqlGrammarException(task, sql, ex);
            }
            else if (ex instanceof SQLFeatureNotSupportedException) {
                return new InvalidDataAccessApiUsageException(buildMessage(task, sql, ex), ex);
            }
        }
        else if (ex instanceof SQLRecoverableException) {
            return new RecoverableDataAccessException(buildMessage(task, sql, ex), ex);
        }

        // Fallback to Spring's own SQL state translation...
        return null;
    }

}
