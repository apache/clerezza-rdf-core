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
import java.util.Iterator;
import org.apache.clerezza.commons.rdf.BlankNode;
import org.apache.clerezza.commons.rdf.BlankNodeOrIRI;
import org.apache.clerezza.commons.rdf.Graph;
import org.apache.clerezza.commons.rdf.IRI;
import org.apache.clerezza.commons.rdf.RDFTerm;
import org.apache.clerezza.commons.rdf.Triple;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

/**
 *
 * @author reto
 */
public class BNodeTest {

    final static int serverPort = findFreePort();
    static EmbeddedFusekiServer server;

    @BeforeAll
    public static void prepare() throws IOException {
        final String serviceURI = "http://localhost:" + serverPort + "/ds/data";
        final DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(serviceURI);
        final InputStream in = BNodeTest.class.getResourceAsStream("simple-bnode.ttl");
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
        assertEquals(3, graph.size(), "Graph not of the expected size");
    }

    /* Filtering with a Bode that cannot be in graph
    */
    @Test
    public void filterAlienBNode() {
        final Graph graph = new SparqlGraph("http://localhost:" + serverPort + "/ds/query");
        
        final BlankNode blankNode = new BlankNode();
        final Iterator<Triple> iter = graph.filter(blankNode, null, null);
        assertFalse(iter.hasNext());
    }
    
    @Test
    public void bNodeIdentity() {
        final Graph graph = new SparqlGraph("http://localhost:" + serverPort + "/ds/query");
        
        final IRI foafPerson = new IRI("http://xmlns.com/foaf/0.1/Person");
        final IRI foafName = new IRI("http://xmlns.com/foaf/0.1/name");
        final IRI foafKnows = new IRI("http://xmlns.com/foaf/0.1/knows");
        final IRI rdfType = new IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

        final Iterator<Triple> iter = graph.filter(null, foafName, null);
        assertTrue(iter.hasNext());
        final BlankNodeOrIRI namedThing = iter.next().getSubject();
        assertTrue(namedThing instanceof BlankNode);
        
        final Iterator<Triple> iter2 = graph.filter(null, rdfType, foafPerson);
        assertTrue(iter2.hasNext());
        final BlankNodeOrIRI person = iter2.next().getSubject();
        assertTrue(person instanceof BlankNode);
        assertEquals(namedThing, person);
        
        final Iterator<Triple> iter3 = graph.filter(null, foafKnows, null);
        assertTrue(iter3.hasNext());
        final RDFTerm knownThing = iter3.next().getObject();
        assertTrue(knownThing instanceof BlankNode);
        assertEquals(knownThing, person);
        assertEquals(namedThing, knownThing);
    }
    
    @Test
    public void filter1() {
        final Graph graph = new SparqlGraph("http://localhost:" + serverPort + "/ds/query");
        
        final IRI foafPerson = new IRI("http://xmlns.com/foaf/0.1/Person");
        final IRI foafName = new IRI("http://xmlns.com/foaf/0.1/name");
        final IRI rdfType = new IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

        final Iterator<Triple> iter = graph.filter(null, foafName, null);
        assertTrue(iter.hasNext());
        final BlankNodeOrIRI person = iter.next().getSubject();
        assertTrue(person instanceof BlankNode);
        
        final Iterator<Triple> iter2 = graph.filter(person, rdfType, null);
        assertTrue(iter2.hasNext());
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
