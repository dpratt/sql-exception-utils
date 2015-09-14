package io.dpratt.jdbc.translator;

/*
 * Copyright 2002-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * JavaBean for holding JDBC error codes for a particular database.
 * Instances of this class are normally loaded through a bean factory.
 * <p>Used by Spring's {@link SQLErrorCodeSQLExceptionTranslator}.
 * The file "sql-error-codes.xml" in this package contains default
 * {@code SQLErrorCodes} instances for various databases.
 *
 * @author Thomas Risberg
 * @author Juergen Hoeller
 * @see SQLErrorCodesFactory
 * @see SQLErrorCodeSQLExceptionTranslator
 */
public class SQLErrorCodes {

    private final String[] databaseProductNames;
    private final boolean useSqlStateForTranslation;

    private final String[] badSqlGrammarCodes;
    private final String[] invalidResultSetAccessCodes;
    private final String[] duplicateKeyCodes;
    private final String[] dataIntegrityViolationCodes;
    private final String[] permissionDeniedCodes;
    private final String[] dataAccessResourceFailureCodes;
    private final String[] transientDataAccessResourceCodes;
    private final String[] cannotAcquireLockCodes;
    private final String[] deadlockLoserCodes;
    private final String[] cannotSerializeTransactionCodes;

    public SQLErrorCodes(
            boolean useSqlStateForTranslation,
            String[] databaseProductNames,
            String[] badSqlGrammarCodes,
            String[] invalidResultSetAccessCodes,
            String[] duplicateKeyCodes,
            String[] dataIntegrityViolationCodes,
            String[] permissionDeniedCodes,
            String[] dataAccessResourceFailureCodes,
            String[] transientDataAccessResourceCodes,
            String[] cannotAcquireLockCodes,
            String[] deadlockLoserCodes,
            String[] cannotSerializeTransactionCodes) {

        this.databaseProductNames = databaseProductNames;
        this.useSqlStateForTranslation = useSqlStateForTranslation;

        this.badSqlGrammarCodes = sortStringArray(badSqlGrammarCodes);
        this.invalidResultSetAccessCodes = sortStringArray(invalidResultSetAccessCodes);
        this.duplicateKeyCodes = duplicateKeyCodes;
        this.dataIntegrityViolationCodes = sortStringArray(dataIntegrityViolationCodes);
        this.permissionDeniedCodes = sortStringArray(permissionDeniedCodes);
        this.dataAccessResourceFailureCodes = sortStringArray(dataAccessResourceFailureCodes);
        this.transientDataAccessResourceCodes = sortStringArray(transientDataAccessResourceCodes);
        this.cannotAcquireLockCodes = sortStringArray(cannotAcquireLockCodes);
        this.deadlockLoserCodes = sortStringArray(deadlockLoserCodes);
        this.cannotSerializeTransactionCodes = sortStringArray(cannotSerializeTransactionCodes);

    }

    public String getDatabaseProductName() {
        return (this.databaseProductNames != null && this.databaseProductNames.length > 0 ?
                this.databaseProductNames[0] : null);
    }

    public String[] getDatabaseProductNames() {
        return this.databaseProductNames;
    }

    public boolean isUseSqlStateForTranslation() {
        return this.useSqlStateForTranslation;
    }

    public String[] getBadSqlGrammarCodes() {
        return this.badSqlGrammarCodes;
    }


    public String[] getInvalidResultSetAccessCodes() {
        return this.invalidResultSetAccessCodes;
    }

    public String[] getDuplicateKeyCodes() {
        return duplicateKeyCodes;
    }

    public String[] getDataIntegrityViolationCodes() {
        return this.dataIntegrityViolationCodes;
    }

    public String[] getPermissionDeniedCodes() {
        return this.permissionDeniedCodes;
    }

    public String[] getDataAccessResourceFailureCodes() {
        return this.dataAccessResourceFailureCodes;
    }

    public String[] getTransientDataAccessResourceCodes() {
        return this.transientDataAccessResourceCodes;
    }

    public String[] getCannotAcquireLockCodes() {
        return this.cannotAcquireLockCodes;
    }

    public String[] getDeadlockLoserCodes() {
        return this.deadlockLoserCodes;
    }

    public String[] getCannotSerializeTransactionCodes() {
        return this.cannotSerializeTransactionCodes;
    }


    public static SQLErrorCodes fromXmlNode(Node node) {

        try {
            if(!(node instanceof Element)) {
                throw new IllegalArgumentException("Cannot parse from a non-Element node.");
            }
            Element elem = (Element)node;
            String primaryProductName = elem.getAttribute("id");
            List<String> productNames = new ArrayList<String>();
            productNames.add(0, primaryProductName);
            productNames.addAll(Arrays.asList(parseStringArray(getProperty(node, "databaseProductNames"))));

            boolean useSqlState = Boolean.valueOf(getProperty(node, "useSqlStateForTranslation"));

            String[] badSqlGrammarCodes = parseStringArray(getProperty(node, "badSqlGrammarCodes"));
            String[] invalidResultSetAccessCodes = parseStringArray(getProperty(node, "invalidResultSetAccessCodes"));
            String[] duplicateKeyCodes = parseStringArray(getProperty(node, "duplicateKeyCodes"));
            String[] dataIntegrityViolationCodes = parseStringArray(getProperty(node, "dataIntegrityViolationCodes"));
            String[] permissionDeniedCodes = parseStringArray(getProperty(node, "permissionDeniedCodes"));
            String[] dataAccessResourceFailureCodes = parseStringArray(getProperty(node, "dataAccessResourceFailureCodes"));
            String[] transientDataAccessResourceCodes = parseStringArray(getProperty(node, "transientDataAccessResourceCodes"));
            String[] cannotAcquireLockCodes = parseStringArray(getProperty(node, "cannotAcquireLockCodes"));
            String[] deadlockLoserCodes = parseStringArray(getProperty(node, "deadlockLoserCodes"));
            String[] cannotSerializeTransactionCodes = parseStringArray(getProperty(node, "cannotSerializeTransactionCodes"));

            return new SQLErrorCodes(
                    useSqlState,
                    productNames.toArray(new String[productNames.size()]),
                    badSqlGrammarCodes,
                    invalidResultSetAccessCodes,
                    duplicateKeyCodes,
                    dataIntegrityViolationCodes,
                    permissionDeniedCodes,
                    dataAccessResourceFailureCodes,
                    transientDataAccessResourceCodes,
                    cannotAcquireLockCodes,
                    deadlockLoserCodes,
                    cannotSerializeTransactionCodes
            );

        } catch(XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }

    public static final SQLErrorCodes EMPTY = new SQLErrorCodes(
            false,
            new String[0],
            new String[0],
            new String[0],
            new String[0],
            new String[0],
            new String[0],
            new String[0],
            new String[0],
            new String[0],
            new String[0],
            new String[0]
    );

    /**
     * Turn given source String array into sorted array.
     *
     * @param array the source array
     * @return the sorted array (never {@code null})
     */
    private static String[] sortStringArray(String[] array) {
        if ((array == null || array.length == 0)) {
            return new String[0];
        }
        Arrays.sort(array);
        return array;
    }

    private static final XPathFactory xPathFactory = XPathFactory.newInstance();
    private static String getProperty(Node parent, String propName) throws XPathExpressionException {
        XPath xPath = xPathFactory.newXPath();
        String path = "//property[@name='" + propName + "']/value/text()";
        return (String)xPath.evaluate(path, parent, XPathConstants.STRING);
    }

    private static final Pattern COMMA_PATTERN = Pattern.compile(",");

    private static String[] parseStringArray(String input) {
        if(input.trim().isEmpty()) {
            return new String[0];
        } else {
            return COMMA_PATTERN.split(input);
        }
    }


}
