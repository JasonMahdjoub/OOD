
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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class represent a field description into the Sql database. The user
 * should not use this class.
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 */
public class SqlField {

	private static final AtomicInteger fieldAliasNumber=new AtomicInteger(0);

	/**
	 * the name of the Sql field appended with the name of the Sql Table.
	 */
	public String field;

	/**
	 * the name of the Sql field without quote appended with the name of the Sql Table.
	 */
	public String fieldWithoutQuote;


	/**
	 * the type of the Sql field.
	 */
	public final String type;

	/**
	 * The name of the pointed Sql table, if this field is a foreign key
	 */
	public final String pointedTable;


	/**
	 * The name of the pointed Sql table alias given into a sql query, and if this field is a foreign key
	 */
	public final String pointedTableAlias;

	/**
	 * The name of the pointed Sql field appended with its Sql Table, if this field
	 * is a foreign key
	 */
	public final String pointedField;

	/**
	 * The name of the pointed Sql table, if this field is a foreign key
	 */
	public final String pointedTableWithoutQuote;

	/**
	 * The name of the pointed Sql field appended with its Sql Table, if this field
	 * is a foreign key
	 */
	public final String pointedFieldWithoutQuote;

	/**
	 * the name of the Sql field not appended with the name of the Sql Table.
	 */
	public final String shortField;

	/**
	 * the name of the Sql field without quote not appended with the name of the Sql Table.
	 */
	public final String shortFieldWithoutQuote;

	/**
	 * The name of the pointed Sql field not appended with its Sql Table, if this
	 * field is a foreign key
	 */
	public String shortPointedField;

	/**
	 * The name of the pointed Sql field without quote not appended with its Sql Table, if this
	 * field is a foreign key
	 */
	public final String shortPointedFieldWithoutQuote;


	/**
	 * Tells if this field is not null
	 */
	public final boolean notNull;

	public int sqlPosition = -1;

	public final String sqlFieldAliasName;

	/**
	 * Constructor
	 *
	 * @param _field
	 *            the name of the Sql field.
	 * @param _type
	 *            the type of the Sql field.
	 * @param _not_null
	 *            tells if the field is not null
	 */
	public SqlField(boolean supportQuote, String _field, String _type, boolean _not_null) {
		this(supportQuote, _field, _type, null, null, null, _not_null);
	}

	/**
	 * Constructor
	 * 
	 * @param _field
	 *            the name of the Sql field.
	 * @param _type
	 *            the type of the Sql field.
	 * @param _pointed_field
	 *            The name of the pointed Sql field, if this field is a foreign key
	 * @param _pointed_table
	 *            The name of the pointed Sql table, if this field is a foreign key
	 * @param pointedTableAlias
	 *            The name of the pointed Sql table alias given into a sql query, and if this field is a foreign key
	 * @param _not_null
	 *            tells if the field is not null
	 */
	public SqlField(boolean supportQuote, String _field, String _type, String _pointed_table, String pointedTableAlias, String _pointed_field, boolean _not_null) {
		this(supportQuote, _field, _type,  _pointed_table, pointedTableAlias, _pointed_field, _not_null, "F"+fieldAliasNumber.incrementAndGet()+"__");
	}
	protected SqlField(boolean supportQuote, String _field, String _type, String _pointed_table, String pointedTableAlias, String _pointed_field, boolean _not_null, String sqlFieldAliasName)
	{
		if ((_pointed_table==null)!=(pointedTableAlias ==null) && (_pointed_table==null)!=(_pointed_field==null))
			throw new NullPointerException();
		if (sqlFieldAliasName ==null || sqlFieldAliasName.length()==0)
			throw new NullPointerException();
		this.sqlFieldAliasName = sqlFieldAliasName;
		fieldWithoutQuote = _field.toUpperCase();
		type = _type.toUpperCase();
		pointedTable = _pointed_table == null ? null : _pointed_table.toUpperCase();
		this.pointedTableAlias = pointedTableAlias;
		pointedField = _pointed_field == null ? null : (pointedTableAlias +"."+_pointed_field.substring(_pointed_field.lastIndexOf('.')+1)).toUpperCase();


		int index = -1;
		for (int i = 0; i < fieldWithoutQuote.length(); i++) {
			if (fieldWithoutQuote.charAt(i) == '.') {
				index = i + 1;
				break;
			}
		}
		if (index != -1) {
			shortFieldWithoutQuote = fieldWithoutQuote.substring(index);
			if (supportQuote) {
				shortField = "`" + shortFieldWithoutQuote + "`";
				field = fieldWithoutQuote.substring(0, index) + "`" + fieldWithoutQuote.substring(index) + "`";
			}
			else {
				shortField = shortFieldWithoutQuote;
				field = fieldWithoutQuote;
			}

		}
		else {
			shortFieldWithoutQuote = fieldWithoutQuote;
			if (supportQuote) {
				shortField = "`" + fieldWithoutQuote + "`";
			}
			else
				shortField = fieldWithoutQuote;

			field= shortField;
		}

		if (pointedField != null) {
			index = -1;
			for (int i = 0; i < pointedField.length(); i++) {
				if (pointedField.charAt(i) == '.') {
					index = i + 1;
					break;
				}
			}
			if (index != -1) {
				shortPointedField = pointedField.substring(index);
			}
			else
				shortPointedField = pointedField;
		} else
			shortPointedField = null;
		if (pointedTable !=null && pointedField !=null)
		{
			pointedTableWithoutQuote = pointedTable.replace("`", "");
			pointedFieldWithoutQuote = pointedField.replace("`", "");
			shortPointedFieldWithoutQuote = shortPointedField.replace("`", "");
		}
		else {
			pointedTableWithoutQuote = null;
			pointedFieldWithoutQuote = null;
			shortPointedFieldWithoutQuote =null;
		}
		this.notNull = _not_null;
	}
}
