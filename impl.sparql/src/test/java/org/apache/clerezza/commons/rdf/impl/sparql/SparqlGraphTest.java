/*
 * Copyright 2015 The Apache Software Foundation.
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
package org.apache.clerezza.commons.rdf.impl.sparql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import java.io.IOException;
import java.net.ServerSocket;
import org.apache.jena.fuseki.EmbeddedFusekiServer;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.clerezza.commons.rdf.Graph;
import org.apache.clerezza.commons.rdf.IRI;
import org.apache.clerezza.commons.rdf.Language;
import org.apache.clerezza.commons.rdf.Literal;
import org.apache.clerezza.commons.rdf.RDFTerm;
import org.apache.clerezza.commons.rdf.Triple;
import org.apache.clerezza.commons.rdf.impl.utils.PlainLiteralImpl;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 *
 * @author reto
 */
@RunWith(JUnitPlatform.class)
public class SparqlGraphTest {

    final static int serverPort = findFreePort();
    static EmbeddedFusekiServer server;

    @BeforeAll
    public static void prepare() throws IOException {
        final String serviceURI = "http://localhost:" + serverPort + "/ds/data";
        final DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(serviceURI);
        final InputStream in = SparqlGraphTest.class.getResourceAsStream("grounded.ttl");
        final Model m = ModelFactory.createDefaultModel();
        String base = "http://example.org/";
        m.read(in, base, "TURTLE");
        server = EmbeddedFusekiServer.memTDB(serverPort, "/ds");//dataSet.getAbsolutePath());
        server.start();
        System.out.println("Started fuseki on port " + serverPort);
        accessor.putModel(m);
    }

    @AfterAll
    public static void cleanup() {
        server.stop();
    }

    @Test
    public void graphSize() {
        final Graph graph = new SparqlGraph("http://localhost:" + serverPort + "/ds/query");
        assertEquals(8, graph.size(), "Graph not of the exepected size");
    }

    @Test
    public void filter1() {
        final Graph graph = new SparqlGraph("http://localhost:" + serverPort + "/ds/query");
        final IRI spiderman = new IRI("http://example.org/#spiderman");
        final IRI greenGoblin = new IRI("http://example.org/#green-goblin");
        final IRI enemyOf = new IRI("http://www.perceive.net/schemas/relationship/enemyOf");
        final IRI foafName = new IRI("http://xmlns.com/foaf/0.1/name");
        {
            final Iterator<Triple> iter = graph.filter(spiderman, null, greenGoblin);
            assertTrue(iter.hasNext());
            assertEquals(enemyOf, iter.next().getPredicate());
            assertFalse(iter.hasNext());
        }
        {
            final Iterator<Triple> iter = graph.filter(spiderman, foafName, null);
            Set<Literal> names = new HashSet<>();
            for (int i = 0; i < 2; i++) {
                assertTrue(iter.hasNext());
                RDFTerm name = iter.next().getObject();
                assertTrue(name instanceof Literal);
                names.add((Literal)name);
            }
            assertFalse(iter.hasNext());
            assertTrue(names.contains(new PlainLiteralImpl("Spiderman")));
            assertTrue(names.contains(new PlainLiteralImpl("Человек-паук", new Language("ru"))));
        }
    }

    public static int findFreePort() {
        int port = 0;
        try (ServerSocket server = new ServerSocket(0);) {
            port = server.getLocalPort();
        } catch (Exception e) {
            throw new RuntimeException("unable to find a free port");
        }
        return port;
    }

}
