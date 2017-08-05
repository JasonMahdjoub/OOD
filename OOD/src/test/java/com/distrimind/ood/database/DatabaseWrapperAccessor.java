
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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.SQLException;

import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.util.DecentralizedIDGenerator;
import com.distrimind.util.RenforcedDecentralizedIDGenerator;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
class DatabaseWrapperAccessor {
	private static final Method m_get_big_decimal_type;
	private static final Method m_get_big_integer_type;
	private static final Method m_get_byte_type;
	private static final Method m_is_var_binary_supported;
	private static final Method m_get_blob;
	private static final Method m_get_double_type;
	private static final Method m_get_float_type;
	private static final Method m_get_int_type;
	private static final Method m_get_long_type;
	private static final Method m_get_serializable_type;
	private static final Method m_get_short_type;
	private static final Method m_get_var_char_limit;
	private static final Constructor<DecentralizedIDGenerator> m_decentralized_id_constructor;
	private static final Constructor<RenforcedDecentralizedIDGenerator> m_renforced_decentralized_id_constructor;

	static String getBigDecimalType(DatabaseWrapper wrapper) {
		try {
			return (String) invoke(m_get_big_decimal_type, wrapper);
		} catch (InvocationTargetException e) {
			System.err.println("Unexpected error :");
			e.printStackTrace();
			System.exit(-1);
		}
		return null;

	}

	static String getBigIntegerType(DatabaseWrapper wrapper) {
		try {
			return (String) invoke(m_get_big_integer_type, wrapper);
		} catch (InvocationTargetException e) {
			System.err.println("Unexpected error :");
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}

	static String getByteType(DatabaseWrapper wrapper) {
		try {
			return (String) invoke(m_get_byte_type, wrapper);
		} catch (InvocationTargetException e) {
			System.err.println("Unexpected error :");
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}

	static String getDoubleType(DatabaseWrapper wrapper) {
		try {
			return (String) invoke(m_get_double_type, wrapper);
		} catch (InvocationTargetException e) {
			System.err.println("Unexpected error :");
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}

	static String getFloatType(DatabaseWrapper wrapper) {
		try {
			return (String) invoke(m_get_float_type, wrapper);
		} catch (InvocationTargetException e) {
			System.err.println("Unexpected error :");
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}

	static String getIntType(DatabaseWrapper wrapper) {
		try {
			return (String) invoke(m_get_int_type, wrapper);
		} catch (InvocationTargetException e) {
			System.err.println("Unexpected error :");
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}

	static String getLongType(DatabaseWrapper wrapper) {
		try {
			return (String) invoke(m_get_long_type, wrapper);
		} catch (InvocationTargetException e) {
			System.err.println("Unexpected error :");
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}

	static String getSerializableType(DatabaseWrapper wrapper) {
		try {
			return (String) invoke(m_get_serializable_type, wrapper);
		} catch (InvocationTargetException e) {
			System.err.println("Unexpected error :");
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}

	static String getShortType(DatabaseWrapper wrapper) {
		try {
			return (String) invoke(m_get_short_type, wrapper);
		} catch (InvocationTargetException e) {
			System.err.println("Unexpected error :");
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}

	static int getVarCharLimit(DatabaseWrapper wrapper) {
		try {
			return ((Integer) invoke(m_get_var_char_limit, wrapper)).intValue();
		} catch (InvocationTargetException e) {
			System.err.println("Unexpected error :");
			e.printStackTrace();
			System.exit(-1);
		}
		return -1;
	}

	static boolean isVarBinarySupported(DatabaseWrapper wrapper) {
		try {
			return ((Boolean) invoke(m_is_var_binary_supported, wrapper)).booleanValue();
		} catch (InvocationTargetException e) {
			System.err.println("Unexpected error :");
			e.printStackTrace();
			System.exit(-1);
		}
		return false;
	}

	static Blob getBlob(DatabaseWrapper wrapper, byte[] tab) throws SQLException {
		try {
			return (Blob) invoke(m_get_blob, wrapper, tab);
		} catch (InvocationTargetException e) {
			throw (SQLException) e.getCause();
		}
	}

	static DecentralizedIDGenerator getDecentralizedIDGeneratorInstance(long timestamp, long work_id_sequence) {
		try {
			return m_decentralized_id_constructor.newInstance(new Long(timestamp), new Long(work_id_sequence));
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException
				| IllegalArgumentException e) {
			System.err.println("Unexpected error :");
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}

	static RenforcedDecentralizedIDGenerator getRenforcedDecentralizedIDGeneratorInstance(long timestamp,
			long work_id_sequence) {
		try {
			return m_renforced_decentralized_id_constructor.newInstance(new Long(timestamp),
					new Long(work_id_sequence));
		} catch (InvocationTargetException | InstantiationException | IllegalAccessException
				| IllegalArgumentException e) {
			System.err.println("Unexpected error :");
			e.printStackTrace();
			System.exit(-1);
		}
		return null;
	}

	static {
		m_get_big_decimal_type = getMethod(DatabaseWrapper.class, "getBigDecimalType");
		m_get_big_integer_type = getMethod(DatabaseWrapper.class, "getBigIntegerType");
		m_get_byte_type = getMethod(DatabaseWrapper.class, "getByteType");
		m_is_var_binary_supported = getMethod(DatabaseWrapper.class, "isVarBinarySupported");
		m_get_blob = getMethod(DatabaseWrapper.class, "getBlob", byte[].class);
		m_get_double_type = getMethod(DatabaseWrapper.class, "getDoubleType");
		m_get_float_type = getMethod(DatabaseWrapper.class, "getFloatType");
		m_get_int_type = getMethod(DatabaseWrapper.class, "getIntType");
		m_get_long_type = getMethod(DatabaseWrapper.class, "getLongType");
		m_get_serializable_type = getMethod(DatabaseWrapper.class, "getSerializableType");
		m_get_short_type = getMethod(DatabaseWrapper.class, "getShortType");
		m_get_var_char_limit = getMethod(DatabaseWrapper.class, "getVarCharLimit");
		m_decentralized_id_constructor = getConstructor(DecentralizedIDGenerator.class, long.class, long.class);
		m_renforced_decentralized_id_constructor = getConstructor(RenforcedDecentralizedIDGenerator.class, long.class,
				long.class);
	}

	private static Object invoke(Method m, Object o, Object... args) throws InvocationTargetException {
		try {
			return m.invoke(o, args);
		} catch (IllegalAccessException | IllegalArgumentException e) {
			System.err.println("Impossible to access to the function " + m.getName() + " of the class "
					+ m.getDeclaringClass()
					+ ". This is an inner bug of MadKitLanEdition. Please contact the developers. Impossible to continue. See the next error :");
			e.printStackTrace();
			System.exit(-1);
			return null;
		}
	}

	private static Method getMethod(Class<?> c, String method_name, Class<?>... parameters) {
		try {
			Method m = c.getDeclaredMethod(method_name, parameters);
			m.setAccessible(true);
			return m;
		} catch (SecurityException | NoSuchMethodException e) {
			System.err.println("Impossible to access to the function " + method_name + " of the class "
					+ c.getCanonicalName()
					+ ". This is an inner bug of MadKitLanEdition. Please contact the developers. Impossible to continue. See the next error :");
			e.printStackTrace();
			System.exit(-1);
			return null;
		}
	}

	private static <E> Constructor<E> getConstructor(Class<E> c, Class<?>... parameters) {
		try {
			Constructor<E> m = c.getDeclaredConstructor(parameters);
			m.setAccessible(true);
			return m;
		} catch (SecurityException | NoSuchMethodException e) {
			System.err.println("Impossible to access to the constructor of the class " + c.getCanonicalName()
					+ ". This is an inner bug of MadKitLanEdition. Please contact the developers. Impossible to continue. See the next error :");
			e.printStackTrace();
			System.exit(-1);
			return null;
		}
	}

}
