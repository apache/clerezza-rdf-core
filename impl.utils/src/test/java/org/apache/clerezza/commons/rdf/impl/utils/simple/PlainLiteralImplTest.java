/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.clerezza.commons.rdf.impl.utils.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.clerezza.commons.rdf.impl.utils.PlainLiteralImpl;
import org.apache.clerezza.commons.rdf.Language;
import org.apache.clerezza.commons.rdf.Literal;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 *
 * @author reto
 *
 */
@RunWith(JUnitPlatform.class)
public class PlainLiteralImplTest {

    
    @Test public void plainLiteralEquality() {
        String stringValue = "some text";
        Literal literal1 = new PlainLiteralImpl(stringValue);
        Literal literal2 = new PlainLiteralImpl(stringValue);        
        assertEquals(literal1, literal2);
        assertEquals(literal1.hashCode(), literal2.hashCode());
        Literal literal3 = new PlainLiteralImpl("something else");
        assertFalse(literal1.equals(literal3));
    }
    
    @Test public void languageLiteralEquality() {
        String stringValue = "some text";
        Language lang = new Language("en-ca");
        Literal literal1 = new PlainLiteralImpl(stringValue, lang);
        Literal literal2 = new PlainLiteralImpl(stringValue, lang);        
        assertEquals(literal1, literal2);
        assertEquals(literal1.hashCode(), literal2.hashCode());
        Language lang2 = new Language("de");
        Literal literal3 = new PlainLiteralImpl(stringValue, lang2);
        assertFalse(literal1.equals(literal3));
        Literal literal4 = new PlainLiteralImpl(stringValue, null);
        assertFalse(literal3.equals(literal4));
        assertFalse(literal4.equals(literal3));
    }

    /**
     * hashCode of the lexical form plus the hashCode of the locale
     */
    @Test public void checkHashCode() {
        String stringValue = "some text";
        Language language = new Language("en");
        Literal literal = new PlainLiteralImpl(stringValue, language);
        assertEquals(literal.getDataType().hashCode() + stringValue.hashCode() + language.hashCode(), literal.hashCode());
    }

}
