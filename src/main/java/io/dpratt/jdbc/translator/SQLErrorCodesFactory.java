package io.dpratt.jdbc.translator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating {@link SQLErrorCodes} based on the
 * "databaseProductName" taken from the {@link java.sql.DatabaseMetaData}.
 * <p>Returns {@code SQLErrorCodes} populated with vendor codes
 * defined in a configuration file named "sql-error-codes.xml".
 * Reads the default file in this package if not overridden by a file in
 * the root of the class path (for example in the "/WEB-INF/classes" directory).
 *
 * @author Thomas Risberg
 * @author Rod Johnson
 * @author Juergen Hoeller
 * @see java.sql.DatabaseMetaData#getDatabaseProductName()
 */
public class SQLErrorCodesFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLErrorCodesFactory.class);

    private static final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();


    /**
     * The name of custom SQL error codes file, loading from the root
     * of the class path (e.g. from the "/WEB-INF/classes" directory).
     */
    public static final String SQL_ERROR_CODE_OVERRIDE_PATH = "sql-error-codes.xml";

    /**
     * The name of default SQL error code files, loading from the class path.
     */
    public static final String SQL_ERROR_CODE_DEFAULT_PATH = "io/dpratt/jdbc/translator/sql-error-codes.xml";

    /**
     * Keep track of a single instance so we can return it to classes that request it.
     */
    private static final SQLErrorCodesFactory instance = new SQLErrorCodesFactory();


    /**
     * Return the singleton instance.
     * @return the instance
     */
    public static SQLErrorCodesFactory getInstance() {
        return instance;
    }

    /**
     * Map to hold error codes for all databases defined in the config file.
     * Key is the database product name, value is the SQLErrorCodes instance.
     */
    private final Map<String, SQLErrorCodes> errorCodesMap;

    private SQLErrorCodesFactory() {
        Map<String, SQLErrorCodes> defaultCodes = parseResource(SQL_ERROR_CODE_DEFAULT_PATH);
        if(defaultCodes.isEmpty()) {
            LOGGER.error("Unable to parse default SQL codes mapping file.");
        }
        defaultCodes.putAll(parseResource(SQL_ERROR_CODE_OVERRIDE_PATH));
        errorCodesMap = defaultCodes;
    }

    /**
     * Return the {@link SQLErrorCodes} instance for the given database.
     * <p>No need for a database metadata lookup.
     *
     * @param dbName the database name (must not be {@code null})
     * @return the {@code SQLErrorCodes} instance for the given database
     * @throws IllegalArgumentException if the supplied database name is {@code null}
     */
    public SQLErrorCodes getErrorCodes(String dbName) {

        SQLErrorCodes sec = this.errorCodesMap.get(dbName);
        if (sec != null) {
            return sec;
        } else {
            for (SQLErrorCodes candidate : this.errorCodesMap.values()) {
                if (PatternMatchUtils.simpleMatch(candidate.getDatabaseProductNames(), dbName)) {
                    return candidate;
                }
            }

            // Could not find the database among the defined ones.
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("SQL error codes for '" + dbName + "' not found");
            }
            return SQLErrorCodes.EMPTY;
        }
    }

    private Map<String, SQLErrorCodes> parseResource(String resource) {

        InputStream in = getClass().getClassLoader().getResourceAsStream(resource);
        if (in == null) {
            return Collections.emptyMap();
        }

        try {
            Map<String, SQLErrorCodes> retval = new HashMap<String, SQLErrorCodes>();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(in);

            NodeList nodes = doc.getDocumentElement().getElementsByTagName("bean");
            for (int i = 0; i < nodes.getLength(); i++) {
                SQLErrorCodes parsed = SQLErrorCodes.fromXmlNode(nodes.item(i));
                retval.put(parsed.getDatabaseProductName(), parsed);
            }
            return retval;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SAXException e) {
            throw new RuntimeException(e);
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                LOGGER.error("Could not close resource stream.", e);
            }
        }


    }


}
