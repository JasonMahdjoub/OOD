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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;


import com.distrimind.ood.database.exceptions.IncompatibleFieldDatabaseException;
/**
 * 
 * {@inheritDoc}
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 1.5
 * 
 */
public class DefaultByteTabObjectConverter extends ByteTabObjectConverter
{

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getByte(Object _o) throws IncompatibleFieldDatabaseException
    {
	if (_o==null)
	    return null;
	if (_o.getClass()==Inet6Address.class || _o.getClass()==Inet4Address.class)
	    return ((InetAddress)_o).getAddress();
	throw new IncompatibleFieldDatabaseException("Incompatible type "+_o.getClass().getCanonicalName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getObject(Class<?> _object_type, byte[] _bytesTab) throws IncompatibleFieldDatabaseException
    {
	
	try
	{
	    if (_bytesTab==null)
		return null;
	    if (_object_type==Inet6Address.class || _object_type==Inet4Address.class)
		return InetAddress.getByAddress(_bytesTab);
	}
	catch (UnknownHostException e)
	{
	    throw new IncompatibleFieldDatabaseException("A problems occurs", e);
	}

	throw new IncompatibleFieldDatabaseException("Incompatible type "+_object_type.getCanonicalName());
	
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCompatible(Class<?> field_type)
    {
	return field_type==Inet4Address.class
		|| field_type==Inet6Address.class;
    }

}
