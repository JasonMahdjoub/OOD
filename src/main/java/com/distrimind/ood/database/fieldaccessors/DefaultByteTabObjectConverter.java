
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
package com.distrimind.ood.database.fieldaccessors;

import com.distrimind.ood.database.exceptions.IncompatibleFieldDatabaseException;
import com.distrimind.util.crypto.WrappedEncryptedASymmetricPrivateKey;
import com.distrimind.util.crypto.WrappedEncryptedSymmetricSecretKey;
import com.distrimind.util.crypto.WrappedHashedPassword;
import com.distrimind.util.data_buffers.WrappedData;
import com.distrimind.util.data_buffers.WrappedSecretData;
import com.distrimind.util.io.SerializationTools;

import java.io.File;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

/**
 * 
 * 
 * 
 * @author Jason Mahdjoub
 * @version 1.4
 * @since OOD 1.5
 * 
 */
public class DefaultByteTabObjectConverter extends ByteTabObjectConverter {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public byte[] getBytes(Object _o) throws IncompatibleFieldDatabaseException {
		if (_o == null)
			return null;
		if (_o.getClass() == Inet6Address.class || _o.getClass() == Inet4Address.class)
			return ((InetAddress) _o).getAddress();
		else if (_o instanceof Enum<?>)
		{
			return ((Enum<?>)_o).name().getBytes(StandardCharsets.UTF_8);
		}
		else if (_o instanceof File)
		{
			return ((File) _o).getPath().getBytes(StandardCharsets.UTF_8);
		}
		else if (_o instanceof WrappedData)
		{
			return ((WrappedData) _o).getBytes();
		}

		throw new IncompatibleFieldDatabaseException("Incompatible type " + _o.getClass().getCanonicalName());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object getObject(Class<?> _object_type, byte[] _bytesTab) throws IncompatibleFieldDatabaseException {

		try {
			if (_bytesTab == null)
				return null;
			if (_object_type == Inet6Address.class || _object_type == Inet4Address.class)
				return InetAddress.getByAddress(_bytesTab);
			else if (Enum.class.isAssignableFrom(_object_type)) {
				return _object_type.getDeclaredMethod("valueOf", String.class).invoke(null, new String(_bytesTab, StandardCharsets.UTF_8));
			}
			else if (File.class.isAssignableFrom(_object_type))
			{
				return new File(new String(_bytesTab, StandardCharsets.UTF_8));
			}
			else if (WrappedEncryptedASymmetricPrivateKey.class.isAssignableFrom(_object_type))
			{
				return new WrappedEncryptedASymmetricPrivateKey(_bytesTab);
			}
			else if (WrappedEncryptedSymmetricSecretKey.class.isAssignableFrom(_object_type))
			{
				return new WrappedEncryptedSymmetricSecretKey(_bytesTab);
			}
			else if (WrappedHashedPassword.class.isAssignableFrom(_object_type))
			{
				return new WrappedHashedPassword(_bytesTab);
			}
			else if (WrappedSecretData.class.isAssignableFrom(_object_type))
			{
				return new WrappedSecretData(_bytesTab);
			}
			else if (WrappedData.class.isAssignableFrom(_object_type))
			{
				return new WrappedData(_bytesTab);
			}

		} catch (Exception e) {
			throw new IncompatibleFieldDatabaseException("A problems occurs", e);
		}

		throw new IncompatibleFieldDatabaseException("Incompatible type " + _object_type.getCanonicalName());
	}



	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isCompatible(Class<?> field_type) {
		return field_type == Inet4Address.class || field_type == Inet6Address.class ||
				Enum.class.isAssignableFrom(field_type)
				|| File.class.isAssignableFrom(field_type)
				|| WrappedData.class.isAssignableFrom(field_type);
	}

	@Override
	public int getDefaultSizeLimit(Class<?> _object_type) throws IncompatibleFieldDatabaseException{
		if (_object_type == Inet6Address.class || _object_type == Inet4Address.class)
			return 128;
		else if (Enum.class.isAssignableFrom(_object_type)) {
			return SerializationTools.MAX_CLASS_LENGTH;
		}
		else if (File.class.isAssignableFrom(_object_type))
		{
			return 16384;
		}
		else if (WrappedEncryptedASymmetricPrivateKey.class.isAssignableFrom(_object_type))
		{
			return WrappedEncryptedASymmetricPrivateKey.MAX_SIZE_IN_BYTES_OF_KEY;
		}
		else if (WrappedEncryptedSymmetricSecretKey.class.isAssignableFrom(_object_type))
		{
			return WrappedEncryptedSymmetricSecretKey.MAX_SIZE_IN_BYTES_OF_KEY;
		}
		else if (WrappedHashedPassword.class.isAssignableFrom(_object_type))
		{
			return WrappedHashedPassword.MAX_SIZE_IN_BYTES_OF_DATA;
		}
		else if (WrappedSecretData.class.isAssignableFrom(_object_type))
		{
			return ByteTabFieldAccessor.defaultByteTabSize;
		}
		else if (WrappedData.class.isAssignableFrom(_object_type))
		{
			return ByteTabFieldAccessor.defaultByteTabSize;
		}

		throw new IncompatibleFieldDatabaseException("Incompatible type " + _object_type.getCanonicalName());
	}

	

}
