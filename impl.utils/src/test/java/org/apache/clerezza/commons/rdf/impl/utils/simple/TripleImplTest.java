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

import org.apache.clerezza.commons.rdf.BlankNodeOrIRI;
import org.apache.clerezza.commons.rdf.RDFTerm;
import org.apache.clerezza.commons.rdf.Triple;
import org.apache.clerezza.commons.rdf.IRI;
import org.apache.clerezza.commons.rdf.impl.utils.PlainLiteralImpl;
import org.apache.clerezza.commons.rdf.impl.utils.TripleImpl;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 *
 * @author reto
 *
 */
@RunWith(JUnitPlatform.class)
public class TripleImplTest {

    @Test public void tripleEquality() {
        BlankNodeOrIRI subject = new IRI("http://example.org/");
        IRI predicate = new IRI("http://example.org/property");
        RDFTerm object = new PlainLiteralImpl("property value");
        Triple triple1 = new TripleImpl(subject, predicate, object);
        Triple triple2 = new TripleImpl(subject, predicate, object);
        assertEquals(triple1.hashCode(), triple2.hashCode());
        assertEquals(triple1, triple2);
    }

}
