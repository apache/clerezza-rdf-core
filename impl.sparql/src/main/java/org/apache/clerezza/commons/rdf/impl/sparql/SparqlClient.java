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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import javax.xml.parsers.*;
import org.apache.clerezza.commons.rdf.BlankNode;
import org.apache.clerezza.commons.rdf.IRI;
import org.apache.clerezza.commons.rdf.Language;
import org.apache.clerezza.commons.rdf.RDFTerm;
import org.apache.clerezza.commons.rdf.impl.utils.AbstractLiteral;
import org.xml.sax.*;
import org.xml.sax.helpers.*;

/**
 *
 * @author developer
 */
public class SparqlClient {

    final String endpoint;

    public SparqlClient(final String endpoint) {
        this.endpoint = endpoint;
    }

    List<Map<String, RDFTerm>> queryResultSet(final String query) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(endpoint);
        List<NameValuePair> nvps = new ArrayList<NameValuePair>();
        nvps.add(new BasicNameValuePair("query", query));
        httpPost.setEntity(new UrlEncodedFormEntity(nvps));
        CloseableHttpResponse response2 = httpclient.execute(httpPost);

        try {
            HttpEntity entity2 = response2.getEntity();
            InputStream in = entity2.getContent();
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);
            SAXParser saxParser = spf.newSAXParser();
            XMLReader xmlReader = saxParser.getXMLReader();
            final SparqlsResultsHandler sparqlsResultsHandler = new SparqlsResultsHandler();
            xmlReader.setContentHandler(sparqlsResultsHandler);
            xmlReader.parse(new InputSource(in));
            /*
             for (int ch = in.read(); ch != -1; ch = in.read()) {
             System.out.print((char)ch);
             }
             */
            // do something useful with the response body
            // and ensure it is fully consumed
            EntityUtils.consume(entity2);
            return sparqlsResultsHandler.getResults();
        } catch (ParserConfigurationException ex) {
            throw new RuntimeException(ex);
        } catch (SAXException ex) {
            throw new RuntimeException(ex);
        } finally {
            response2.close();
        }

    }

    final public static class SparqlsResultsHandler extends DefaultHandler {

        private String currentBindingName;
        private Map<String, RDFTerm> currentResult = null;
        private final List<Map<String, RDFTerm>> results = new ArrayList<>();
        private boolean readingValue;
        private String lang; //the xml:lang attribute of a literal
        private String value;
        private Map<String, BlankNode> bNodeMap = new HashMap<>();
        private static final IRI XSD_STRING = new IRI("http://www.w3.org/2001/XMLSchema#string");
        private static final IRI RDF_LANG_STRING = new IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString");

        private RDFTerm getBNode(String value) {
            if (!bNodeMap.containsKey(value)) {
                bNodeMap.put(value, new BlankNode());
            }
            return bNodeMap.get(value);
        }

        private List<Map<String, RDFTerm>> getResults() {
            return results;
        }

        enum BindingType {

            uri, bnode, literal;
        }

        @Override
        public void startDocument() throws SAXException {

        }

        @Override
        public void startElement(String namespaceURI,
                String localName,
                String qName,
                Attributes atts)
                throws SAXException {
            if ("http://www.w3.org/2005/sparql-results#".equals(namespaceURI)) {
                if ("result".equals(localName)) {
                    if (currentResult != null) {
                        throw new SAXException("unexpected tag <result>");
                    }
                    currentResult = new HashMap<>();
                } else if ("binding".equals(localName)) {
                    if (currentResult == null) {
                        throw new SAXException("unexpected tag <binding>");
                    }
                    currentBindingName = atts.getValue("name");
                } else if ("uri".equals(localName) || "bnode".equals(localName) || "literal".equals(localName)) {
                    if (readingValue) {
                        throw new SAXException("unexpected tag <" + localName + ">");
                    }
                    lang = atts.getValue("http://www.w3.org/XML/1998/namespace", "lang");
                    readingValue = true;
                }
            }

            //System.out.println(namespaceURI);
            //System.out.println(qName);
        }

        @Override
        public void characters(char[] chars, int start, int length) throws SAXException {
            if (readingValue) {
                value = new String(chars, start, length);
                //System.err.println(value + start + ", " + length);
            }
        }

        @Override
        public void endElement(String namespaceURI,
                String localName,
                String qName)
                throws SAXException {
            if ("http://www.w3.org/2005/sparql-results#".equals(namespaceURI)) {
                if ("result".equals(localName)) {
                    results.add(currentResult);
                    currentResult = null;
                } else if ("binding".equals(localName)) {
                    if (currentBindingName == null) {
                        throw new SAXException("unexpected tag </binding>");
                    }
                    currentBindingName = null;
                } else {
                    try {
                        BindingType b = BindingType.valueOf(localName);
                        RDFTerm rdfTerm = null;
                        final Language language = lang == null? null : new Language(lang);;
                        switch (b) {
                            case uri:
                                rdfTerm = new IRI(value);
                                break;
                            case bnode:
                                rdfTerm = getBNode(value);
                                break;
                            case literal:
                                final String lf = value;
                                rdfTerm = new AbstractLiteral() {

                                    @Override
                                    public String getLexicalForm() {
                                        return lf;
                                    }

                                    @Override
                                    public IRI getDataType() {
                                        if (language != null) {
                                            return RDF_LANG_STRING;
                                        }
                                        //TODO implement
                                        return XSD_STRING;
                                    }

                                    @Override
                                    public Language getLanguage() {
                                        return language;
                                    }
                                };
                                break;
                        }
                        currentResult.put(currentBindingName, rdfTerm);
                        readingValue = false;
                    } catch (IllegalArgumentException e) {
                            //not uri|bnode|literal
                    }
                }
            }
        }

        public void endDocument() throws SAXException {
            //System.out.println("results: " + results.size());
        }

    }

}
