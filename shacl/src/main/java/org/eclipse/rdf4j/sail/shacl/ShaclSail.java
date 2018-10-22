/*******************************************************************************
 * Copyright (c) 2018 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/

package org.eclipse.rdf4j.sail.shacl;

import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.NotifyingSail;
import org.eclipse.rdf4j.sail.NotifyingSailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.NotifyingSailWrapper;
import org.eclipse.rdf4j.sail.shacl.AST.NodeShape;

import java.util.List;

/**
 * @author Heshan Jayasinghe
 */
public class ShaclSail extends NotifyingSailWrapper {

	public List<NodeShape> nodeShapes;
	boolean debugPrintPlans = false;

	ShaclSailConfig config = new ShaclSailConfig();

	public ShaclSail(NotifyingSail baseSail, SailRepository shaclSail) {
		super(baseSail);
		try (SailRepositoryConnection shaclSailConnection = shaclSail.getConnection()) {
			nodeShapes = NodeShape.Factory.getShapes(shaclSailConnection);
		}
	}

	@Override
	public NotifyingSailConnection getConnection()
		throws SailException {
		return new ShaclSailConnection(this, super.getConnection(), super.getConnection());
	}

	public void disableValidation() {
		config.validationEnabled = false;
	}

	public void enableValidation() {
		config.validationEnabled = true;
	}

	public boolean isDebugPrintPlans() {
		return debugPrintPlans;
	}

	public void setDebugPrintPlans(boolean debugPrintPlans) {
		this.debugPrintPlans = debugPrintPlans;
	}
}

class ShaclSailConfig {

	boolean validationEnabled = true;

}