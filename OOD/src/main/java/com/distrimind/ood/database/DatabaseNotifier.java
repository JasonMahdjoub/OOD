/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

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

package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;

import java.util.Set;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 2.0.0
 */
public interface DatabaseNotifier {
    void newDatabaseEventDetected(DatabaseWrapper wrapper) throws DatabaseException;

    /**
     * This function is called before a transaction of synchronization with another peer begin
     * @throws DatabaseException if a problem occurs
     */
    void startNewSynchronizationTransaction() throws DatabaseException;
    /**
     * This function is called after a transaction of synchronization with another peer has finished
     * It is possible to apply post synchronization actions here
     * @throws DatabaseException if a problem occurs
     */
    void endSynchronizationTransaction() throws DatabaseException;

    /**
     * The given host was disconnected. Local host is not concerned.
     * @param hostID the disconnected host
     */
    void hostDisconnected(DecentralizedValue hostID);

    /**
     * The given host was connected. Local host is not concerned.
     * @param hostID the connected host
     */
    void hostConnected(DecentralizedValue hostID);

    /**
     * The given local host was initialized.
     * @param hostID the initialized local host
     */
    void localHostInitialized(DecentralizedValue hostID);

    /**
     * This function is called when a peer or more was added
     * @param peersIdentifiers the identifiers of the added peers
     */
    void hostsAdded(Set<DecentralizedValue> peersIdentifiers);

    /**
     * This function is called when central database backup certificate was revoked
     */
    void centralDatabaseBackupCertificateRevoked();

}
