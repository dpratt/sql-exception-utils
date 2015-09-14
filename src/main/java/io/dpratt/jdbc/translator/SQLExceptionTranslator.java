package io.dpratt.jdbc.translator;

import io.dpratt.jdbc.DataAccessException;

import java.sql.SQLException;

/**
 * Strategy interface for translating between {@link java.sql.SQLException SQLExceptions}
 * and Spring's data access strategy-agnostic {@link DataAccessException}
 * hierarchy.
 *
 * <p>Implementations can be generic (for example, using
 * {@link java.sql.SQLException#getSQLState() SQLState} codes for JDBC) or wholly
 * proprietary (for example, using Oracle error codes) for greater precision.
 *
 * @author Rod Johnson
 * @author Juergen Hoeller
 * @see DataAccessException
 */
public interface SQLExceptionTranslator {

    /**
     * Translate the given {@link java.sql.SQLException} into a generic {@link DataAccessException}.
     * <p>The returned DataAccessException is supposed to contain the original
     * {@code SQLException} as root cause. However, client code may not generally
     * rely on this due to DataAccessExceptions possibly being caused by other resource
     * APIs as well. That said, a {@code getRootCause() instanceof SQLException}
     * check (and subsequent cast) is considered reliable when expecting JDBC-based
     * access to have happened.
     * @param task readable text describing the task being attempted
     * @param sql SQL query or update that caused the problem (may be {@code null})
     * @param ex the offending {@code SQLException}
     * @return the DataAccessException, wrapping the {@code SQLException}
     * @see DataAccessException#getCause()
     */
    DataAccessException translate(String task, String sql, SQLException ex);

}
