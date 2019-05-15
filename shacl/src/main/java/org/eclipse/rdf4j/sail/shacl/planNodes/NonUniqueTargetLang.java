/*******************************************************************************
 * Copyright (c) 2019 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/

package org.eclipse.rdf4j.sail.shacl.planNodes;

import org.apache.commons.text.StringEscapeUtils;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.shacl.CloseablePeakableIteration;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * This PlanNode takes a stream of Tuples like: (ex:companyA, "Company A"@en). It assumes that the stream is sorted on
 * index 0 (eg. ex:CompanyA). It will cache all non-empty languages from index 1 (eg. "en") and outputs any tuples where
 * the language has already been seen.
 *
 * If a Value on index 1 has no language because it is a literal without a language or because it is an IRI or BNode,
 * then its language is considered empty and not cached.
 *
 * @author Håvard Ottestad
 */
public class NonUniqueTargetLang implements PlanNode {
	PlanNode parent;
	Return returnType;
	private boolean printed = false;


	public NonUniqueTargetLang(PlanNode parent, Return returnType) {
		this.parent = parent;
		this.returnType = returnType;
	}

	public enum Return {
		onlyUnique, onlyNotUnique
	}

	@Override
	public CloseableIteration<Tuple, SailException> iterator() {

		if (returnType == Return.onlyNotUnique) {
			return new OnlyNonUnique(parent);
		} else if (returnType == Return.onlyUnique) {
			return new OnlyUnique(parent);
		}

		throw new IllegalStateException("Unknown return type: " + returnType);
	}

	@Override
	public int depth() {
		return parent.depth() + 1;
	}

	@Override
	public void getPlanAsGraphvizDot(StringBuilder stringBuilder) {
		if (printed) {
			return;
		}
		printed = true;
		stringBuilder.append(getId() + " [label=\"" + StringEscapeUtils.escapeJava(this.toString()) + "\"];")
			.append("\n");
		stringBuilder.append(parent.getId() + " -> " + getId()).append("\n");
		parent.getPlanAsGraphvizDot(stringBuilder);
	}

	@Override
	public String toString() {
		return "NonUniqueTargetLang";
	}

	@Override
	public String getId() {
		return System.identityHashCode(this) + "";
	}

	@Override
	public IteratorData getIteratorDataType() {
		return parent.getIteratorDataType();
	}


}


class OnlyNonUnique implements CloseableIteration<Tuple, SailException> {

	Tuple next;
	Tuple previous;

	private Set<String> seenLanguages = new HashSet<>();

	CloseableIteration<Tuple, SailException> parentIterator;

	public OnlyNonUnique(PlanNode parent) {
		parentIterator = parent.iterator();
	}

	private void calculateNext() {
		if (next != null) {
			return;
		}


		while (next == null && parentIterator.hasNext()) {
			next = parentIterator.next();

			if ((previous != null)) {
				if (!previous.line.get(0).equals(next.line.get(0))) {
					seenLanguages = new HashSet<>();
				}
			}

			previous = next;

			Value value = next.getlist().get(1);

			if (value instanceof Literal) {
				Optional<String> lang = ((Literal) value).getLanguage();

				if (!lang.isPresent()) {
					next = null;
				} else if (!seenLanguages.contains(lang.get())) {
					seenLanguages.add(lang.get());
					next = null;
				}

			} else {
				next = null;
			}

		}

	}


	@Override
	public void close() throws SailException {
		parentIterator.close();
	}

	@Override
	public boolean hasNext() throws SailException {
		calculateNext();
		return next != null;
	}

	@Override
	public Tuple next() throws SailException {
		calculateNext();

		Tuple temp = next;
		next = null;
		return temp;
	}

	@Override
	public void remove() throws SailException {

	}
}

class OnlyUnique implements CloseableIteration<Tuple, SailException> {

	Deque<Tuple> nextQueue = new ArrayDeque<>();

	private Set<String> seenLanguages = new HashSet<>();
	private Set<String> seenLanguagesTwice = new HashSet<>();


	CloseablePeakableIteration<Tuple, SailException> parentIterator;

	public OnlyUnique(PlanNode parent) {
		parentIterator = new CloseablePeakableIteration<>(parent.iterator());
	}

	private void calculateNext() {
		if (!nextQueue.isEmpty()) {
			return;
		}


		while (true) {

			Tuple peek = null;
			if (parentIterator.hasNext()) {
				peek = parentIterator.peek();
			}

			if (nextQueue.isEmpty() && peek == null) {
				break;
			}


			if ((nextQueue.peekLast() != null)) {
				if (peek == null || !nextQueue.peekLast().line.get(0).equals(peek.line.get(0))) {
					Deque<Tuple> temp = new ArrayDeque<>();

					nextQueue.stream()
						.filter(next -> {
							Value value = next.getlist().get(1);
							if (value instanceof Literal) {
								Optional<String> language = ((Literal) value).getLanguage();
								if (language.isPresent()) {
									return !seenLanguagesTwice.contains(language.get());
								}
							}

							return true;
						})
						.forEach(temp::addLast);

					nextQueue = temp;

					seenLanguages = new HashSet<>();
					seenLanguagesTwice = new HashSet<>();

					if (!nextQueue.isEmpty()) {
						break;
					}

				}
			}

			if (peek == null) {
				break;
			}

			Tuple next = parentIterator.next();
			nextQueue.addLast(next);


			Value value = next.getlist().get(1);

			if (value instanceof Literal) {
				Optional<String> lang = ((Literal) value).getLanguage();

				if (lang.isPresent()) {

					String language = lang.get();
					boolean langSeenBefore = seenLanguages.contains(language);
					if (langSeenBefore) {
						seenLanguagesTwice.add(language);
					} else {
						seenLanguages.add(language);
					}

				}
			}

		}

	}


	@Override
	public void close() throws SailException {
		parentIterator.close();
	}

	@Override
	public boolean hasNext() throws SailException {
		calculateNext();
		return !nextQueue.isEmpty();
	}

	@Override
	public Tuple next() throws SailException {
		calculateNext();
		return nextQueue.removeFirst();
	}

	@Override
	public void remove() throws SailException {

	}
}