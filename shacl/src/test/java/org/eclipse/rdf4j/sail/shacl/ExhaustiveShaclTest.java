package org.eclipse.rdf4j.sail.shacl;

import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Ignore;
import org.junit.Test;
import org.paukov.combinatorics.CombinatoricsFactory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.paukov.combinatorics.CombinatoricsFactory.createPermutationGenerator;
import static org.paukov.combinatorics.CombinatoricsFactory.createSubSetGenerator;
import static org.paukov.combinatorics.CombinatoricsFactory.createVector;

public class ExhaustiveShaclTest {

	Random r = new Random(6647832);

	@Test
	@Ignore
	public void test() throws IOException {


		Model data = Rio.parse(ExhaustiveShaclTest.class.getClassLoader().getResourceAsStream("complete/minCount/data.ttl"), "", RDFFormat.TURTLE);


		List<List<Transaction>> combinations = getCombinations(data);

		for (int j = 0; j <= combinations.size(); j++) {


			List<Transaction> combination = combinations.get(j);

			System.out.println(j + "/" + combinations.size() + " : " + combination.size());
			for (int i = 0; i < combination.size(); i++) {

				ICombinatoricsVector<Transaction> vector = CombinatoricsFactory.createVector(combination.subList(0, i).stream().toArray(Transaction[]::new));
				Generator<Transaction> gen = createPermutationGenerator(vector);
				for (ICombinatoricsVector<Transaction> perm : gen) {

					if (r.nextInt(10000000) != 1) {
						continue;
					}

					ArrayList<Transaction> transactions = new ArrayList<>();
					perm.forEach(transactions::add);


					for (int i1 = 0; i1 < transactions.size(); i1++) {
						boolean validAsSingleTransaction = isValidAsSingleTransaction(transactions.subList(0, i1));
						boolean validAsMultipleTransaction = isValidAsMultipleTransaction(transactions.subList(0, i1));


						if (validAsMultipleTransaction) {
							assertTrue("\nj = " + j + "\ni = " + i + "\ni1 = " + i1 + "\n" +
								Arrays.toString(transactions.toArray()), validAsSingleTransaction);
						}
						if (!validAsSingleTransaction) {
							assertFalse("\nj = " + j + "\ni = " + i + "\ni1 = " + i1 + "\n" +
								Arrays.toString(transactions.toArray()), validAsMultipleTransaction);
						}

						System.out.print(".");
						if (!validAsMultipleTransaction) {
							break;
						}
					}


				}


			}


		}


	}

	private boolean isValidAsSingleTransaction(List<Transaction> combination) {
		boolean valid = true;

		ShaclSail innerShaclSail = new ShaclSail(new MemoryStore(), Utils.getSailRepository("complete/minCount/shacl.ttl"));
		innerShaclSail.setDebugPrintPlans(false);
		SailRepository shaclSail = new SailRepository(innerShaclSail);
		shaclSail.initialize();


		try (SailRepositoryConnection connection = shaclSail.getConnection()) {

			connection.begin(IsolationLevels.SERIALIZABLE);

			for (Transaction transaction : combination) {
				if (transaction.transactionType == TransactionType.insert) {
					connection.add(transaction.statementList);
				} else if (transaction.transactionType == TransactionType.delete) {
					connection.remove(transaction.statementList);
				}
			}

			connection.commit();

		} catch (RepositoryException e) {
			valid = false;
		}
		shaclSail.shutDown();

		return valid;
	}


	private boolean isValidAsMultipleTransaction(List<Transaction> combination) {
		boolean valid = true;


		ShaclSail innerShaclSail = new ShaclSail(new MemoryStore(), Utils.getSailRepository("complete/minCount/shacl.ttl"));
		innerShaclSail.setDebugPrintPlans(false);
		SailRepository shaclSail = new SailRepository(innerShaclSail);
		shaclSail.initialize();


		try (SailRepositoryConnection connection = shaclSail.getConnection()) {


			for (Transaction transaction : combination) {
				connection.begin(IsolationLevels.SERIALIZABLE);

				if (transaction.transactionType == TransactionType.insert) {
					connection.add(transaction.statementList);
				} else if (transaction.transactionType == TransactionType.delete) {
					connection.remove(transaction.statementList);
				}
				connection.commit();

			}


		} catch (RepositoryException e) {
			valid = false;
		}
		shaclSail.shutDown();

		return valid;
	}


	private List<List<Transaction>> getCombinations(Model data) {

		List<List<Transaction>> lists = new ArrayList<>();


		ICombinatoricsVector<Statement> vector = createVector(data.stream().toArray(Statement[]::new));
		Generator<Statement> gen = createSubSetGenerator(vector);
		for (ICombinatoricsVector<Statement> subSet : gen) {
			ArrayList<Transaction> transactions = new ArrayList<>();

			for (Statement statement : subSet) {
				transactions.add(new Transaction(Arrays.asList(statement), TransactionType.insert));
			}
			lists.add(transactions);
		}

		return lists;

	}

	class Transaction {
		List<Statement> statementList;
		TransactionType transactionType;

		public Transaction(List<Statement> statementList, TransactionType transactionType) {
			this.statementList = statementList;
			this.transactionType = transactionType;
		}

		@Override
		public String toString() {
			return "Transaction{" +
				"statementList=" + statementList +
				", transactionType=" + transactionType +
				'}';
		}
	}

	enum TransactionType {
		insert, delete;
	}


}
