/*
 * Object Oriented Database (created by Jason MAHDJOUB (jason.mahdjoub@distri-mind.fr)) Copyright (c)
 * 2012, JBoss Inc., and individual contributors as indicated by the @authors
 * tag.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package com.distrimind.ood.database.fieldaccessors;

import com.distrimind.ood.database.exceptions.IncompatibleFieldDatabaseException;

/**
 * This class aims to convert an object to byte tab, and conversely.
 * This class aims to get better performance than object serialization.
 *  
 * @author Jason Mahdjoub
 * @version 1.0
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
