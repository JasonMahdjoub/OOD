
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
package com.distrimind.ood.database.fieldaccessors;

import com.distrimind.ood.database.exceptions.IncompatibleFieldDatabaseException;

/**
 * This class aims to convert an object to byte tab, and conversely.
 * This class aims to get better performance than object serialization.
 *  
 * @author Jason Mahdjoub
 * @version 1.1
 * @since OOD 1.5
 */
public abstract class ByteTabObjectConverter
{
    
    /**
     * Convert an object to a byte tab.
     * 
     * @param o the object to convert to a byte tab
     * @return the byte tab or null the object is null.
     * @throws IncompatibleFieldDatabaseException if the object is not compatible
     */
    public abstract byte[] getByte(Object o) throws IncompatibleFieldDatabaseException;
    
    /**
     * Convert the byte tab to an object instance
     * @param object_type the object type expected
     * @param bytesTab the byte tab to convert
     * @return the resulted object instance, or null if the object type is null.
     * @throws IncompatibleFieldDatabaseException if the object is not compatible
     */
    public abstract Object getObject(Class<?> object_type, byte bytesTab[]) throws IncompatibleFieldDatabaseException;
    
    
    /**
     * Defines is the object type given as parameter is compatible with the current class, and then if it can be converted to a byte tab.
     * @param object_type the object type to test
     * @return true is the object type is managed by this class, false else.
     */
    public abstract boolean isCompatible(Class<?> object_type);
    
}
