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
public class BNodeCircleTest {

    final static int serverPort = findFreePort();
    static EmbeddedFusekiServer server;

    @BeforeClass
    public static void prepare() throws IOException {
        final String serviceURI = "http://localhost:" + serverPort + "/ds/data";
        final DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(serviceURI);
        final InputStream in = BNodeCircleTest.class.getResourceAsStream("bnode-circle.ttl");
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
        Assert.assertEquals("Graph not of the exepected size", 2, graph.size());
    }

    
    
    @Test
    public void nullFilter() {
        final Graph graph = new SparqlGraph("http://localhost:" + serverPort + "/ds/query");
        
        final Iri foafKnows = new Iri("http://xmlns.com/foaf/0.1/knows");

        final Iterator<Triple> iter = graph.filter(null, null, null);
        Assert.assertTrue(iter.hasNext());
        final Triple triple1 = iter.next();
        final BlankNodeOrIri subject = triple1.getSubject();
        final RdfTerm object = triple1.getObject();
        Assert.assertTrue(subject instanceof BlankNode);
        Assert.assertTrue(object instanceof BlankNode);
        Assert.assertNotEquals(subject, object);
        Assert.assertTrue(iter.hasNext());
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
