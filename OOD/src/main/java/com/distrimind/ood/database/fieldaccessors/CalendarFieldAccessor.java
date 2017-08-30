
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.Calendar;

import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;
import com.distrimind.ood.database.exceptions.IncompatibleFieldDatabaseException;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.2
 * @since OOD 1.0
 * 
 */
public class CalendarFieldAccessor extends ByteTabConvertibleFieldAccessor {

	protected CalendarFieldAccessor(Class<? extends Table<?>> table_class, DatabaseWrapper _sql_connection,
			Field _field, String parentFieldName) throws DatabaseException {
		super(table_class, _sql_connection, _field, parentFieldName, new ByteTabObjectConverter() {
			
			@Override
			public boolean isCompatible(Class<?> object_type) {
				return Calendar.class.isAssignableFrom(object_type);
			}
			
			@Override
			public Object getObject(Class<?> object_type, byte[] bytesTab) throws IncompatibleFieldDatabaseException {
				try(ByteArrayInputStream bais=new ByteArrayInputStream(bytesTab); ObjectInputStream dis=new ObjectInputStream(bais))
				{
					Object o=dis.readObject();
					if (o!=null && !(o instanceof Calendar))
						throw new IncompatibleFieldDatabaseException("The class "+o.getClass()+" does is not a calendar !");
					return o;
				}
				catch(Exception e)
				{
					throw new IncompatibleFieldDatabaseException("",e);
				}
				
			}
			
			@Override
			public int getDefaultSizeLimit(Class<?> object_type) throws IncompatibleFieldDatabaseException{
				return 200;
			}
			
			@Override
			public byte[] getByte(Object o) throws IncompatibleFieldDatabaseException {
				try(ByteArrayOutputStream baos=new ByteArrayOutputStream();ObjectOutputStream oos=new ObjectOutputStream(baos))
				{
					if (o!=null && !(o instanceof Calendar))
						throw new IncompatibleFieldDatabaseException("The class "+o.getClass()+" does is not a calendar !");
					oos.writeObject(o);
					return baos.toByteArray();
				}
				catch(Exception e)
				{
					throw new IncompatibleFieldDatabaseException("",e);
				}
			}
		});
		if (!Calendar.class.isAssignableFrom(_field.getType()))
			throw new FieldDatabaseException(
					"The field " + _field.getName() + " of the class " + _field.getDeclaringClass().getName()
							+ " of type " + _field.getType() + " must be a Calendar type.");
	}

}
