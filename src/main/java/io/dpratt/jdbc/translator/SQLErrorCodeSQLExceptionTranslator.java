package io.dpratt.jdbc.translator;

import io.dpratt.jdbc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.Arrays;

/**
* Implementation of {@link SQLExceptionTranslator} that analyzes vendor-specific error codes.
* More precise than an implementation based on SQL state, but heavily vendor-specific.
*
* <p>This class applies the following matching rules:
* <ul>
* <li>Try custom translation implemented by any subclass. Note that this class is
* concrete and is typically used itself, in which case this rule doesn't apply.
* <li>Apply error code matching. Error codes are obtained from the SQLErrorCodesFactory
* by default. This factory loads a "sql-error-codes.xml" file from the class path,
* defining error code mappings for database names from database metadata.
* <li>Fallback to a fallback translator. {@link SQLStateSQLExceptionTranslator} is the
* default fallback translator, analyzing the exception's SQL state only. On Java 6
* which introduces its own {@code SQLException} subclass hierarchy, we will
* use {@link SQLExceptionSubclassTranslator} by default, which in turns falls back
* to Spring's own SQL state translation when not encountering specific subclasses.
* </ul>
*
* <p>The configuration file named "sql-error-codes.xml" is by default read from
* this package. It can be overridden through a file of the same name in the root
* of the class path (e.g. in the "/WEB-INF/classes" directory), as long as the
* Spring JDBC package is loaded from the same ClassLoader.
*
* @author Rod Johnson
* @author Thomas Risberg
* @author Juergen Hoeller
* @see SQLErrorCodesFactory
* @see SQLStateSQLExceptionTranslator
*/
public class SQLErrorCodeSQLExceptionTranslator extends AbstractFallbackSQLExceptionTranslator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLErrorCodeSQLExceptionTranslator.class);


    /** Error codes used by this translator */
    private final SQLErrorCodes sqlErrorCodes;


    /**
     * Create a SQL error code translator for the given database product name.
     * Invoking this constructor will avoid obtaining a Connection from the
     * DataSource to get the metadata.
     * @param dbName the database product name that identifies the error codes entry
     * @see SQLErrorCodesFactory
     * @see java.sql.DatabaseMetaData#getDatabaseProductName()
     */
    public SQLErrorCodeSQLExceptionTranslator(String dbName) {
        this(SQLErrorCodesFactory.getInstance().getErrorCodes(dbName));
    }

    /**
     * Create a SQLErrorCode translator given these error codes.
     * Does not require a database metadata lookup to be performed using a connection.
     * @param sec error codes
     */
    public SQLErrorCodeSQLExceptionTranslator(SQLErrorCodes sec) {
        super(new SQLExceptionSubclassTranslator());
        this.sqlErrorCodes = sec;
    }

    /**
     * Return the error codes used by this translator.
     * Usually determined via a DataSource.
     * @return the error codes instance
     */
    public SQLErrorCodes getSqlErrorCodes() {
        return this.sqlErrorCodes;
    }


    @Override
    protected DataAccessException doTranslate(String task, String sql, SQLException ex) {
        SQLException sqlEx = ex;
        if (sqlEx instanceof BatchUpdateException && sqlEx.getNextException() != null) {
            SQLException nestedSqlEx = sqlEx.getNextException();
            if (nestedSqlEx.getErrorCode() > 0 || nestedSqlEx.getSQLState() != null) {
                LOGGER.debug("Using nested SQLException from the BatchUpdateException");
                sqlEx = nestedSqlEx;
            }
        }

        // Check SQLErrorCodes with corresponding error code, if available.
        if (this.sqlErrorCodes != null) {
            String errorCode;
            if (this.sqlErrorCodes.isUseSqlStateForTranslation()) {
                errorCode = sqlEx.getSQLState();
            }
            else {
                // Try to find SQLException with actual error code, looping through the causes.
                // E.g. applicable to java.sql.DataTruncation as of JDK 1.6.
                SQLException current = sqlEx;
                while (current.getErrorCode() == 0 && current.getCause() instanceof SQLException) {
                    current = (SQLException) current.getCause();
                }
                errorCode = Integer.toString(current.getErrorCode());
            }

            if (errorCode != null) {
                // Next, look for grouped error codes.
                if (Arrays.binarySearch(this.sqlErrorCodes.getBadSqlGrammarCodes(), errorCode) >= 0) {
                    logTranslation(task, sql, sqlEx, false);
                    return new BadSqlGrammarException(task, sql, sqlEx);
                }
                else if (Arrays.binarySearch(this.sqlErrorCodes.getInvalidResultSetAccessCodes(), errorCode) >= 0) {
                    logTranslation(task, sql, sqlEx, false);
                    return new InvalidResultSetAccessException(task, sql, sqlEx);
                }
                else if (Arrays.binarySearch(this.sqlErrorCodes.getDuplicateKeyCodes(), errorCode) >= 0) {
                    logTranslation(task, sql, sqlEx, false);
                    return new DuplicateKeyException(buildMessage(task, sql, sqlEx), sqlEx);
                }
                else if (Arrays.binarySearch(this.sqlErrorCodes.getDataIntegrityViolationCodes(), errorCode) >= 0) {
                    logTranslation(task, sql, sqlEx, false);
                    return new DataIntegrityViolationException(buildMessage(task, sql, sqlEx), sqlEx);
                }
                else if (Arrays.binarySearch(this.sqlErrorCodes.getPermissionDeniedCodes(), errorCode) >= 0) {
                    logTranslation(task, sql, sqlEx, false);
                    return new PermissionDeniedDataAccessException(buildMessage(task, sql, sqlEx), sqlEx);
                }
                else if (Arrays.binarySearch(this.sqlErrorCodes.getDataAccessResourceFailureCodes(), errorCode) >= 0) {
                    logTranslation(task, sql, sqlEx, false);
                    return new DataAccessResourceFailureException(buildMessage(task, sql, sqlEx), sqlEx);
                }
                else if (Arrays.binarySearch(this.sqlErrorCodes.getTransientDataAccessResourceCodes(), errorCode) >= 0) {
                    logTranslation(task, sql, sqlEx, false);
                    return new TransientDataAccessResourceException(buildMessage(task, sql, sqlEx), sqlEx);
                }
                else if (Arrays.binarySearch(this.sqlErrorCodes.getCannotAcquireLockCodes(), errorCode) >= 0) {
                    logTranslation(task, sql, sqlEx, false);
                    return new CannotAcquireLockException(buildMessage(task, sql, sqlEx), sqlEx);
                }
                else if (Arrays.binarySearch(this.sqlErrorCodes.getDeadlockLoserCodes(), errorCode) >= 0) {
                    logTranslation(task, sql, sqlEx, false);
                    return new DeadlockLoserDataAccessException(buildMessage(task, sql, sqlEx), sqlEx);
                }
                else if (Arrays.binarySearch(this.sqlErrorCodes.getCannotSerializeTransactionCodes(), errorCode) >= 0) {
                    logTranslation(task, sql, sqlEx, false);
                    return new CannotSerializeTransactionException(buildMessage(task, sql, sqlEx), sqlEx);
                }
            }
        }

        // We couldn't identify it more precisely - let's hand it over to the SQLState fallback translator.
        if (LOGGER.isDebugEnabled()) {
            String codes;
            if (this.sqlErrorCodes != null && this.sqlErrorCodes.isUseSqlStateForTranslation()) {
                codes = "SQL state '" + sqlEx.getSQLState() + "', error code '" + sqlEx.getErrorCode();
            }
            else {
                codes = "Error code '" + sqlEx.getErrorCode() + "'";
            }
            LOGGER.debug("Unable to translate SQLException with " + codes + ", will now try the fallback translator");
        }

        return null;
    }

    private void logTranslation(String task, String sql, SQLException sqlEx, boolean custom) {
        if (LOGGER.isDebugEnabled()) {
            String intro = custom ? "Custom translation of" : "Translating";
            LOGGER.debug(intro + " SQLException with SQL state '" + sqlEx.getSQLState() +
                    "', error code '" + sqlEx.getErrorCode() + "', message [" + sqlEx.getMessage() +
                    "]; SQL was [" + sql + "] for task [" + task + "]");
        }
    }

}
