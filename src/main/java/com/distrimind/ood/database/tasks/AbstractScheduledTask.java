package com.distrimind.ood.database.tasks;
/*
Copyright or © or Corp. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java language 

This software is governed by the CeCILL-C license under French law and
abiding by the rules of distribution of free software.  You can  use, 
modify and/ or redistribute the software under the terms of the CeCILL-C
license as circulated by CEA, CNRS and INRIA at the following URL
"http://www.cecill.info". 

As a counterpart to the access to the source code and  rights to copy,
modify and redistribute granted by the license, users are provided only
with a limited warranty  and the software's author,  the holder of the
economic rights,  and the successive licensors  have only  limited
liability. 

In this respect, the user's attention is drawn to the risks associated
with loading,  using,  modifying and/or developing or reproducing the
software by the user in light of its specific status of free software,
that may mean  that it is complicated to manipulate,  and  that  also
therefore means  that it is reserved for developers  and  experienced
professionals having in-depth computer knowledge. Users are therefore
encouraged to load and test the software's suitability as regards their
requirements in conditions enabling the security of their systems and/or 
data to be ensured and,  more generally, to use and operate it in the 
same conditions as regards security. 

The fact that you are presently reading this means that you have had
knowledge of the CeCILL-C license and that you accept its terms.
 */

import com.distrimind.util.io.*;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.2.0
 */
public abstract class AbstractScheduledTask implements SecureExternalizable {
	public static final Set<Class<?>> internalTaskSchedulingClassesList=new HashSet<>(Collections.singletonList(ExecutedTasksTable.class));
	private Class<? extends ITaskStrategy> strategyClass;


	public AbstractScheduledTask(Class<? extends ITaskStrategy> strategyClass) {
		if (strategyClass==null)
			throw new NullPointerException();
		try {
			strategyClass.getConstructor();
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException();
		}

		this.strategyClass=strategyClass;
	}


	public Class<? extends ITaskStrategy> getStrategyClass() {
		return strategyClass;
	}

	@Override
	public int getInternalSerializedSize() {
		return SerializationTools.getInternalSize(strategyClass);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeClass(strategyClass, false);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		Class<?> c=in.readClass(false);
		if (!ITaskStrategy.class.isAssignableFrom(c))
			throw new MessageExternalizationException(Integrity.FAIL);
		//noinspection unchecked
		strategyClass=(Class<? extends ITaskStrategy>)c;
		try {
			strategyClass.getConstructor();
		} catch (NoSuchMethodException e) {
			throw new MessageExternalizationException(Integrity.FAIL);
		}
	}


}
