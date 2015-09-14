package io.dpratt.jdbc.translator;

import io.dpratt.jdbc.DataAccessException;
import io.dpratt.jdbc.UncategorizedSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;


/**
 * Base class for {@link SQLExceptionTranslator} implementations that allow for
 * fallback to some other {@link SQLExceptionTranslator}.
 *
 * @author Juergen Hoeller
 * @since 2.5.6
 */
public abstract class AbstractFallbackSQLExceptionTranslator implements SQLExceptionTranslator {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractFallbackSQLExceptionTranslator.class);

    private final SQLExceptionTranslator fallbackTranslator;

    public AbstractFallbackSQLExceptionTranslator() {
        this(null);
    }

    public AbstractFallbackSQLExceptionTranslator(SQLExceptionTranslator fallback) {
        this.fallbackTranslator = fallback;
    }

    /**
     * Return the fallback exception translator, if any.
     * @return the translator
     */
    public SQLExceptionTranslator getFallbackTranslator() {
        return this.fallbackTranslator;
    }


    /**
     * Pre-checks the arguments, calls {@link #doTranslate}, and invokes the
     * {@link #getFallbackTranslator() fallback translator} if necessary.
     */
    @Override
    public DataAccessException translate(String task, String sql, SQLException ex) {
        if(ex == null) {
            throw new IllegalArgumentException("Cannot translate a null SQLException");
        }
        if (task == null) {
            task = "";
        }
        if (sql == null) {
            sql = "";
        }

        DataAccessException dex = doTranslate(task, sql, ex);
        if (dex != null) {
            // Specific exception match found.
            return dex;
        }
        // Looking for a fallback...
        SQLExceptionTranslator fallback = getFallbackTranslator();
        if (fallback != null) {
            return fallback.translate(task, sql, ex);
        }
        // We couldn't identify it more precisely.
        return new UncategorizedSQLException(task, sql, ex);
    }

    /**
     * Template method for actually translating the given exception.
     * <p>The passed-in arguments will have been pre-checked. Furthermore, this method
     * is allowed to return {@code null} to indicate that no exception match has
     * been found and that fallback translation should kick in.
     * @param task readable text describing the task being attempted
     * @param sql SQL query or update that caused the problem (may be {@code null})
     * @param ex the offending {@code SQLException}
     * @return the DataAccessException, wrapping the {@code SQLException};
     * or {@code null} if no exception match found
     */
    protected abstract DataAccessException doTranslate(String task, String sql, SQLException ex);


    /**
     * Build a message {@code String} for the given {@link java.sql.SQLException}.
     * <p>To be called by translator subclasses when creating an instance of a generic
     * {@link DataAccessException} class.
     * @param task readable text describing the task being attempted
     * @param sql the SQL statement that caused the problem (may be {@code null})
     * @param ex the offending {@code SQLException}
     * @return the message {@code String} to use
     */
    protected String buildMessage(String task, String sql, SQLException ex) {
        return task + "; SQL [" + sql + "]; " + ex.getMessage();
    }

}
