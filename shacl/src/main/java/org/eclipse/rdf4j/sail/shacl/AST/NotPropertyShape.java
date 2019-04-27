/*******************************************************************************
 * Copyright (c) 2019 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/
package org.eclipse.rdf4j.sail.shacl.AST;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.shacl.ShaclSailConnection;
import org.eclipse.rdf4j.sail.shacl.SourceConstraintComponent;
import org.eclipse.rdf4j.sail.shacl.planNodes.EnrichWithShape;
import org.eclipse.rdf4j.sail.shacl.planNodes.PlanNode;
import org.eclipse.rdf4j.sail.shacl.planNodes.PlanNodeProvider;
import org.eclipse.rdf4j.sail.shacl.planNodes.UnionNode;
import org.eclipse.rdf4j.sail.shacl.planNodes.Unique;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Håvard Ottestad
 */
public class NotPropertyShape extends PathPropertyShape {

	private final PropertyShape orPropertyShape;

	private static final Logger logger = LoggerFactory.getLogger(NotPropertyShape.class);

	NotPropertyShape(Resource id, SailRepositoryConnection connection, NodeShape nodeShape, boolean deactivated,
			PathPropertyShape parent, Resource path, Resource not) {
		super(id, connection, nodeShape, deactivated, parent, path);

		List<List<PathPropertyShape>> collect = Factory.getPropertyShapesInner(connection, nodeShape, not, this)
				.stream()
				.map(Collections::singletonList)
				.collect(Collectors.toList());

		orPropertyShape = new OrPropertyShape(id, connection, nodeShape, deactivated, this, path, collect);

	}

	@Override
	public PlanNode getPlan(ShaclSailConnection shaclSailConnection, NodeShape nodeShape, boolean printPlans,
			PlanNodeProvider overrideTargetNode, boolean negateThisPlan, boolean negateSubPlans) {
		if (deactivated) {
			return null;
		}
		EnrichWithShape plan;
		if (negateThisPlan) {
			plan = (EnrichWithShape) orPropertyShape.getPlan(shaclSailConnection, nodeShape, printPlans,
					overrideTargetNode, false, false);
		} else {
			plan = (EnrichWithShape) orPropertyShape.getPlan(shaclSailConnection, nodeShape, printPlans,
					overrideTargetNode, false, true);

		}

		PlanNode parent = plan.getParent();

		return new EnrichWithShape(parent, this);

	}

	static private PlanNode unionAll(List<PlanNode> planNodes) {
		return new Unique(new UnionNode(planNodes.toArray(new PlanNode[0])));
	}

	@Override
	public boolean requiresEvaluation(SailConnection addedStatements, SailConnection removedStatements) {
		if (deactivated) {
			return false;
		}

		return super.requiresEvaluation(addedStatements, removedStatements)
				|| orPropertyShape.requiresEvaluation(addedStatements, removedStatements);
	}

	@Override
	public SourceConstraintComponent getSourceConstraintComponent() {
		return SourceConstraintComponent.NotConstraintComponent;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		NotPropertyShape that = (NotPropertyShape) o;
		return orPropertyShape.equals(that.orPropertyShape);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), orPropertyShape);
	}

	@Override
	public String toString() {
		return "NotPropertyShape{" +
				"orPropertyShape=" + orPropertyShape +
				'}';
	}
}
