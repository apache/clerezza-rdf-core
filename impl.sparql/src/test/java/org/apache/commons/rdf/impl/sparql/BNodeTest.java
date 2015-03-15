/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.commons.rdf.impl.sparql;

import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import org.apache.jena.fuseki.EmbeddedFusekiServer;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.rdf.BlankNode;
import org.apache.commons.rdf.BlankNodeOrIri;
import org.apache.commons.rdf.Graph;
import org.apache.commons.rdf.Iri;
import org.apache.commons.rdf.Language;
import org.apache.commons.rdf.Literal;
import org.apache.commons.rdf.RdfTerm;
import org.apache.commons.rdf.Triple;
import org.apache.commons.rdf.impl.utils.PlainLiteralImpl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author reto
 */
public class BNodeTest {

    final static int serverPort = findFreePort();
    static EmbeddedFusekiServer server;

    @BeforeClass
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

    @AfterClass
    public static void cleanup() {
        server.stop();
    }

    @Test
    public void graphSize() {
        final Graph graph = new SparqlGraph("http://localhost:" + serverPort + "/ds/query");
        Assert.assertEquals("Graph not of the exepected size", 3, graph.size());
    }

    /* Filtering with a Bode that cannot be in graph
    */
    @Test
    public void filterAlienBNode() {
        final Graph graph = new SparqlGraph("http://localhost:" + serverPort + "/ds/query");
        
        final BlankNode blankNode = new BlankNode();
        final Iterator<Triple> iter = graph.filter(blankNode, null, null);
        Assert.assertFalse(iter.hasNext());
    }
    
    @Test
    public void bNodeIdentity() {
        final Graph graph = new SparqlGraph("http://localhost:" + serverPort + "/ds/query");
        
        final Iri foafPerson = new Iri("http://xmlns.com/foaf/0.1/Person");
        final Iri foafName = new Iri("http://xmlns.com/foaf/0.1/name");
        final Iri foafKnows = new Iri("http://xmlns.com/foaf/0.1/knows");
        final Iri rdfType = new Iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

        final Iterator<Triple> iter = graph.filter(null, foafName, null);
        Assert.assertTrue(iter.hasNext());
        final BlankNodeOrIri namedThing = iter.next().getSubject();
        Assert.assertTrue(namedThing instanceof BlankNode);
        
        final Iterator<Triple> iter2 = graph.filter(null, rdfType, foafPerson);
        Assert.assertTrue(iter2.hasNext());
        final BlankNodeOrIri person = iter2.next().getSubject();
        Assert.assertTrue(person instanceof BlankNode);
        Assert.assertEquals(namedThing, person);
        
        final Iterator<Triple> iter3 = graph.filter(null, foafKnows, null);
        Assert.assertTrue(iter3.hasNext());
        final RdfTerm knownThing = iter3.next().getObject();
        Assert.assertTrue(knownThing instanceof BlankNode);
        Assert.assertEquals(knownThing, person);
        Assert.assertEquals(namedThing, knownThing);
    }
    
    @Ignore
    @Test
    public void filter1() {
        final Graph graph = new SparqlGraph("http://localhost:" + serverPort + "/ds/query");
        
        final Iri foafPerson = new Iri("http://xmlns.com/foaf/0.1/Person");
        final Iri foafName = new Iri("http://xmlns.com/foaf/0.1/name");
        final Iri rdfType = new Iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");

        final Iterator<Triple> iter = graph.filter(null, foafName, null);
        Assert.assertTrue(iter.hasNext());
        final BlankNodeOrIri person = iter.next().getSubject();
        Assert.assertTrue(person instanceof BlankNode);
        
        final Iterator<Triple> iter2 = graph.filter(person, rdfType, null);
        Assert.assertTrue(iter2.hasNext());
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
