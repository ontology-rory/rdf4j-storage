package org.eclipse.rdf4j.repository.sail.memory;

import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TempTest {


	private IRI PAINTER;

	private IRI PAINTS;

	private IRI PAINTING;

	private IRI YEAR;

	private IRI PERIOD;

	private IRI PICASSO;

	private IRI REMBRANDT;

	private IRI GUERNICA;

	private IRI JACQUELINE;

	private IRI NIGHTWATCH;

	private IRI ARTEMISIA;

	private IRI DANAE;

	private IRI JACOB;

	private IRI ANATOMY;

	private IRI BELSHAZZAR;

	private String NS = "http://rdf.example.org/";

	public TempTest() {
		ValueFactory uf = SimpleValueFactory.getInstance();
		PAINTER = uf.createIRI(NS, "Painter");
		PAINTS = uf.createIRI(NS, "paints");
		PAINTING = uf.createIRI(NS, "Painting");
		YEAR = uf.createIRI(NS, "year");
		PERIOD = uf.createIRI(NS, "period");
		PICASSO = uf.createIRI(NS, "picasso");
		REMBRANDT = uf.createIRI(NS, "rembrandt");
		GUERNICA = uf.createIRI(NS, "guernica");
		JACQUELINE = uf.createIRI(NS, "jacqueline");
		NIGHTWATCH = uf.createIRI(NS, "nightwatch");
		ARTEMISIA = uf.createIRI(NS, "artemisia");
		DANAE = uf.createIRI(NS, "danaë");
		JACOB = uf.createIRI(NS, "jacob");
		ANATOMY = uf.createIRI(NS, "anatomy");
		BELSHAZZAR = uf.createIRI(NS, "belshazzar");

	}

	@Test
	public void test_changedOptionalFilterInsert_2() throws Exception {
		SailRepository repository = new SailRepository(new MemoryStore());
		repository.init();

		SailRepositoryConnection a = repository.getConnection();
		SailRepositoryConnection b = repository.getConnection();

		a.add(PICASSO, RDF.TYPE, PAINTER);
		a.add(PICASSO, PAINTS, GUERNICA);
		a.add(PICASSO, PAINTS, JACQUELINE);
		b.add(REMBRANDT, RDF.TYPE, PAINTER);
		b.add(REMBRANDT, PAINTS, NIGHTWATCH);
		b.add(REMBRANDT, PAINTS, ARTEMISIA);
		b.add(REMBRANDT, PAINTS, DANAE);
		a.begin(IsolationLevels.SNAPSHOT);
		b.begin(IsolationLevels.SNAPSHOT);
		a.add(GUERNICA, RDF.TYPE, PAINTING);
		a.add(JACQUELINE, RDF.TYPE, PAINTING);
		b.prepareUpdate(QueryLanguage.SPARQL,
			"INSERT { ?painting a <Painting> }\n" + "WHERE { [a <Painter>] <paints> ?painting "
				+ "OPTIONAL { ?painting a ?type  } FILTER (!bound(?type)) }",
			NS).execute();
		a.commit();
		assertEquals(5, size(b, null, RDF.TYPE, PAINTING, false));
		b.commit();
		assertEquals(5, size(b, null, RDF.TYPE, PAINTING, false));
		assertEquals(12, size(a, null, null, null, false));
	}

	private static int size(RepositoryConnection con, Resource subj, IRI pred, Value obj, boolean inf, Resource... ctx)
		throws Exception {
		return QueryResults.asList(con.getStatements(subj, pred, obj, inf, ctx)).size();
	}


}
