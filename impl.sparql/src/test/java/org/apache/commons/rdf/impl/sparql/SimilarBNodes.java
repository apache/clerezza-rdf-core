package org.apache.commons.rdf.impl.sparql;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import org.apache.commons.rdf.impl.sparql.*;
import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import java.io.IOException;
import java.net.ServerSocket;
import org.apache.jena.fuseki.EmbeddedFusekiServer;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import java.io.InputStream;
import java.util.Iterator;
import org.apache.commons.rdf.BlankNode;
import org.apache.commons.rdf.BlankNodeOrIri;
import org.apache.commons.rdf.Graph;
import org.apache.commons.rdf.Iri;
import org.apache.commons.rdf.RdfTerm;
import org.apache.commons.rdf.Triple;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author reto
 */
public class SimilarBNodes {

    final static int serverPort = findFreePort();
    static EmbeddedFusekiServer server;

    @BeforeClass
    public static void prepare() throws IOException {
        final String serviceURI = "http://localhost:" + serverPort + "/ds/data";
        final DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(serviceURI);
        final InputStream in = SimilarBNodes.class.getResourceAsStream("similar-bnodes.ttl");
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
    public void foafKnowsFilter() {
        final Graph graph = new SparqlGraph("http://localhost:" + serverPort + "/ds/query");
        
        final Iri foafKnows = new Iri("http://xmlns.com/foaf/0.1/knows");

        final Iterator<Triple> iter = graph.filter(null, foafKnows, null);
        Assert.assertTrue(iter.hasNext());
        final Triple triple1 = iter.next();
        final BlankNodeOrIri subject1 = triple1.getSubject();
        Assert.assertTrue(subject1 instanceof BlankNode);
        Assert.assertTrue(iter.hasNext());
        final Triple triple2 = iter.next();
        final BlankNodeOrIri subject2 = triple2.getSubject();
        Assert.assertTrue(subject2 instanceof BlankNode);
        Assert.assertNotEquals(subject1, subject2);
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
