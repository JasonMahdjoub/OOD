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

package com.distrimind.ood.database;

import com.distrimind.util.DecentralizedValue;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 2.0.0
 */
public interface DatabaseAnomaliesNotifier<DR extends DatabaseRecord, T extends Table<DR>>
{
    /**
     * This function is call if an anomaly occurs the synchronization process.
     * Anomalies can be produced when :
     * <ul>
     * <li>the record to remove was not found :
     * {@link DatabaseWrapper.SynchronizationAnomalyType#RECORD_TO_REMOVE_NOT_FOUND}</li>
     * <li>the record to update was not found :
     * {@link DatabaseWrapper.SynchronizationAnomalyType#RECORD_TO_UPDATE_NOT_FOUND}</li>
     * <li>the record to add already exists :
     * {@link DatabaseWrapper.SynchronizationAnomalyType#RECORD_TO_ADD_ALREADY_PRESENT}</li>
     * </ul>
     * Anomalies differs with collision (see
     * {@link DatabaseCollisionsNotifier#collisionDetected(DecentralizedValue, DecentralizedValue, DatabaseEventType, Table, HashMap, DatabaseRecord, DatabaseRecord)}).
     * They should not occur and represents a synchronization failure. Whereas
     * collisions are produced when users make modifications on the same data into
     * several peers. These kind of conflict are considered as normal by the system.
     *
     * @param distantPeerID
     *            the concerned distant peer, that produced the data modification.
     * @param intermediatePeerID
     *            nearest intermediate peer that transferred the data (can be null).
     *            This intermediate peer is not those who have generated conflict
     *            modifications.
     * @param type
     *            anomaly type
     * @param concernedTable
     *            the concerned table
     * @param primary_keys
     *            the concerned field keys
     * @param record
     *            the transmitted record
     */
    void anomalyDetected(DecentralizedValue distantPeerID, DecentralizedValue intermediatePeerID,
                         DatabaseWrapper.SynchronizationAnomalyType type, T concernedTable, Map<String, Object> primary_keys,
                         DR record);
}
