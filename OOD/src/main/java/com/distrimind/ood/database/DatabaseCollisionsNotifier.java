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

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.AbstractDecentralizedID;

import java.util.HashMap;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 2.0.0
 */
public interface DatabaseCollisionsNotifier<DR extends DatabaseRecord, T extends Table<DR>>
{
    /**
     * This function is called when a direct collision is detected during the
     * synchronization process, when receiving data from a distant peer.
     *
     * @param distantPeerID
     *            the concerned distant peer, that produced the data modification.
     * @param intermediatePeerID
     *            nearest intermediate peer that transfered the data (can be null).
     *            This intermediate peer is not those who have generated conflict
     *            modifications.
     * @param type
     *            the database event type
     * @param concernedTable
     *            the concerned table
     * @param keys
     *            the concerned field keys
     * @param newValues
     *            the new received values
     * @param actualValues
     *            the actual field values
     * @return true if the new event can replace the actual value and false if the
     *         actual value must replace the distant value.
     * @throws DatabaseException
     *             if a problem occurs
     */
    boolean collisionDetected(AbstractDecentralizedID distantPeerID,
                              AbstractDecentralizedID intermediatePeerID, DatabaseEventType type, T concernedTable,
                              HashMap<String, Object> keys, DR newValues, DR actualValues)
            throws DatabaseException;

    /**
     * When a collision occurs, tells if it must be ignored when the two concerned events are the same and produce the same effect
     * True value is recommended.
     * @return true if collision must be ignored when the two concerned events are the same and produce the same effect
     */
    boolean areDuplicatedEventsNotConsideredAsCollisions();


}
