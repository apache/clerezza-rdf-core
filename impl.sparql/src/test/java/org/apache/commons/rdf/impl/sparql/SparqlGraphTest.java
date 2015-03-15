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
import org.junit.Test;

/**
 *
 * @author reto
 */
public class SparqlGraphTest {

    final static int serverPort = findFreePort();
    static EmbeddedFusekiServer server;

    @BeforeClass
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

    @AfterClass
    public static void cleanup() {
        server.stop();
    }

    @Test
    public void graphSize() {
        final Graph graph = new SparqlGraph("http://localhost:" + serverPort + "/ds/query");
        Assert.assertEquals("Graph not of the exepected size", 8, graph.size());
    }

    @Test
    public void filter1() {
        final Graph graph = new SparqlGraph("http://localhost:" + serverPort + "/ds/query");
        final Iri spiderman = new Iri("http://example.org/#spiderman");
        final Iri greenGoblin = new Iri("http://example.org/#green-goblin");
        final Iri enemyOf = new Iri("http://www.perceive.net/schemas/relationship/enemyOf");
        final Iri foafName = new Iri("http://xmlns.com/foaf/0.1/name");
        {
            final Iterator<Triple> iter = graph.filter(spiderman, null, greenGoblin);
            Assert.assertTrue(iter.hasNext());
            Assert.assertEquals(enemyOf, iter.next().getPredicate());
            Assert.assertFalse(iter.hasNext());
        }
        {
            final Iterator<Triple> iter = graph.filter(spiderman, foafName, null);
            Set<Literal> names = new HashSet<>();
            for (int i = 0; i < 2; i++) {
                Assert.assertTrue(iter.hasNext());
                RdfTerm name = iter.next().getObject();
                Assert.assertTrue(name instanceof Literal);
                names.add((Literal)name);
            }
            Assert.assertFalse(iter.hasNext());
            Assert.assertTrue(names.contains(new PlainLiteralImpl("Spiderman")));
            Assert.assertTrue(names.contains(new PlainLiteralImpl("Человек-паук", new Language("ru"))));
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
