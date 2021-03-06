/*******************************************************************************
 * Copyright (c) 2016 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/
package org.eclipse.rdf4j.sail.federation;

import junit.framework.Test;

import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.parser.sparql.manifest.SPARQL10ManifestTest;
import org.eclipse.rdf4j.query.parser.sparql.manifest.SPARQLQueryTest;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.dataset.DatasetRepository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

public class FederationSPARQL10QueryTest extends SPARQLQueryTest {

	public static Test suite() throws Exception {
		return SPARQL10ManifestTest.suite(new Factory() {

			public FederationSPARQL10QueryTest createSPARQLQueryTest(String testURI, String name, String queryFileURL,
					String resultFileURL, Dataset dataSet, boolean laxCardinality) {
				return createSPARQLQueryTest(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality,
						false);
			}

			public FederationSPARQL10QueryTest createSPARQLQueryTest(String testURI, String name, String queryFileURL,
					String resultFileURL, Dataset dataSet, boolean laxCardinality, boolean checkOrder) {
				String[] ignoredTests = {
						// incompatible with SPARQL 1.1 - syntax for decimals was modified
						"Basic - Term 6",
						// incompatible with SPARQL 1.1 - syntax for decimals was modified
						"Basic - Term 7",
						// Test is incorrect: assumes timezoned date is comparable with non-timezoned
						"date-2" };

				return new FederationSPARQL10QueryTest(testURI, name, queryFileURL, resultFileURL, dataSet,
						laxCardinality, checkOrder, ignoredTests);
			}
		});

	}

	protected FederationSPARQL10QueryTest(String testURI, String name, String queryFileURL, String resultFileURL,
			Dataset dataSet, boolean laxCardinality, String... ignoredTests) {
		this(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality, false, ignoredTests);
	}

	protected FederationSPARQL10QueryTest(String testURI, String name, String queryFileURL, String resultFileURL,
			Dataset dataSet, boolean laxCardinality, boolean checkOrder, String... ignoredTests) {
		super(testURI, name, queryFileURL, resultFileURL, dataSet, laxCardinality, checkOrder, ignoredTests);
	}

	protected Repository newRepository() {
		Federation sail = new Federation();
		sail.addMember(new SailRepository(new MemoryStore()));
		sail.addMember(new SailRepository(new MemoryStore()));
		sail.addMember(new SailRepository(new MemoryStore()));
		return new DatasetRepository(new SailRepository(sail));
	}
}
