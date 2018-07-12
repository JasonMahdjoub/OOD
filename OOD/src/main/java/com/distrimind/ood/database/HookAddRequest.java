
/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java langage 

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
package com.distrimind.ood.database;

import java.util.ArrayList;

import com.distrimind.util.AbstractDecentralizedID;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public class HookAddRequest extends DatabaseEvent implements DatabaseEventToSend {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3238906777698759525L;

	private AbstractDecentralizedID hostSource;
	private AbstractDecentralizedID hostDestination;
	private ArrayList<String> packagesToSynchronize;
	private ArrayList<AbstractDecentralizedID> hostAlreadySynchronized;
	private boolean mustReturnMessage;
	private boolean replaceDistantConflictualData;

	HookAddRequest(AbstractDecentralizedID _hostSource, AbstractDecentralizedID _hostDestination,
			ArrayList<String> packagesToSynchronize, ArrayList<AbstractDecentralizedID> hostAlreadySynchronized,
			boolean mustReturnMessage, boolean replaceDistantConflictualData) {
		super();
		if (_hostSource == null)
			throw new NullPointerException();
		if (_hostDestination == null)
			throw new NullPointerException();
		hostSource = _hostSource;
		hostDestination = _hostDestination;
		this.packagesToSynchronize = packagesToSynchronize;
		this.hostAlreadySynchronized = hostAlreadySynchronized;
		this.replaceDistantConflictualData = replaceDistantConflictualData;
		this.mustReturnMessage = mustReturnMessage;
	}

	@Override
	public AbstractDecentralizedID getHostDestination() {
		return hostDestination;
	}

	@Override
	public AbstractDecentralizedID getHostSource() {
		return hostSource;
	}

	public ArrayList<AbstractDecentralizedID> getHostsAlreadySynchronized() {
		return hostAlreadySynchronized;
	}

	public boolean mustReturnsMessage() {
		return mustReturnMessage;
	}

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	public boolean isReplaceDistantConflictualData() {
		return replaceDistantConflictualData;
	}

	public ArrayList<String> getPackagesToSynchronize() {
		return packagesToSynchronize;
	}
}
