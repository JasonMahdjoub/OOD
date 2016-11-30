
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

import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Map;


import com.distrimind.ood.database.exceptions.IncompatibleFieldDatabaseException;
import com.distrimind.util.crypto.ASymmetricKeyPair;
import com.distrimind.util.crypto.ASymmetricPrivateKey;
import com.distrimind.util.crypto.ASymmetricPublicKey;
import com.distrimind.util.crypto.SymmetricSecretKey;

/**
 * 
 * {@inheritDoc}
 * 
 * @author Jason Mahdjoub
 * @version 1.1
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
	else if (_o instanceof ASymmetricKeyPair)
	    return ((ASymmetricKeyPair)_o).encode();
	else if (_o instanceof ASymmetricPublicKey)
	    return ((ASymmetricPublicKey)_o).encode();
	else if (_o instanceof ASymmetricPrivateKey)
	    return ((ASymmetricPrivateKey)_o).encode();
	else if (_o instanceof SymmetricSecretKey)
	    return ((SymmetricSecretKey)_o).encode();
	else if (_o instanceof Enum<?>)
	    return ((Enum<?>)_o).toString().getBytes();
	
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
	    else if (ASymmetricKeyPair.class.isAssignableFrom(_object_type))
	    {
		return ASymmetricKeyPair.decode(_bytesTab);
	    }
	    else if (ASymmetricPublicKey.class.isAssignableFrom(_object_type))
	    {
		return ASymmetricPublicKey.decode(_bytesTab);
	    }
	    else if (ASymmetricPrivateKey.class.isAssignableFrom(_object_type))
	    {
		return ASymmetricPrivateKey.decode(_bytesTab);
	    }
	    else if (SymmetricSecretKey.class.isAssignableFrom(_object_type))
	    {
		return SymmetricSecretKey.decode(_bytesTab);
	    }
	    else if (Enum.class.isAssignableFrom(_object_type))
	    {
		
	        @SuppressWarnings("unchecked")
		Enum<?> result = ((Map<String, Enum<?>>)enumConstantDirectory.invoke(_object_type)).get(new String(_bytesTab));
	        if (result != null)
	            return result;
	        throw new IllegalArgumentException(
	            "No enum constant " + _object_type.getCanonicalName() + "." + new String(_bytesTab));
	    }
	}
	catch (Exception e)
	{
	    throw new IncompatibleFieldDatabaseException("A problems occurs", e);
	}

	throw new IncompatibleFieldDatabaseException("Incompatible type "+_object_type.getCanonicalName());
    }
    
    static final Method enumConstantDirectory;
    static
    {
	Method m=null;
	try
	{
	    m=Class.class.getDeclaredMethod("enumConstantDirectory");
	    m.setAccessible(true);
	}
	catch(Exception e)
	{
	    e.printStackTrace();
	    System.exit(-1);
	}
	enumConstantDirectory=m;
    }
    

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCompatible(Class<?> field_type)
    {
	return field_type==Inet4Address.class
		|| field_type==Inet6Address.class
		|| field_type==ASymmetricKeyPair.class
		|| field_type==ASymmetricPublicKey.class
		|| field_type==ASymmetricPrivateKey.class
		|| field_type==SymmetricSecretKey.class
		|| Enum.class.isAssignableFrom(field_type);
    }

}
