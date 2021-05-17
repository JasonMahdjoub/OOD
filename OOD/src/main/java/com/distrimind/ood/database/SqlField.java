
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

package com.distrimind.ood.database;

/**
 * This class represent a field description into the Sql database. The user
 * should not use this class.
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 */
public class SqlField {

	/**
	 * the name of the Sql field appended with the name of the Sql Table.
	 */
	public String field;

	/**
	 * the name of the Sql field without quote appended with the name of the Sql Table.
	 */
	public String field_without_quote;


	/**
	 * the type of the Sql field.
	 */
	public final String type;

	/**
	 * The name of the pointed Sql table, if this field is a foreign key
	 */
	public final String pointed_table;


	/**
	 * The name of the pointed Sql table alias given into a sql query, and if this field is a foreign key
	 */
	public final String pointed_table_alias;

	/**
	 * The name of the pointed Sql field appended with its Sql Table, if this field
	 * is a foreign key
	 */
	public final String pointed_field;

	/**
	 * The name of the pointed Sql table, if this field is a foreign key
	 */
	public final String pointed_table_without_quote;

	/**
	 * The name of the pointed Sql field appended with its Sql Table, if this field
	 * is a foreign key
	 */
	public final String pointed_field_without_quote;

	/**
	 * the name of the Sql field not appended with the name of the Sql Table.
	 */
	public final String short_field;

	/**
	 * the name of the Sql field without quote not appended with the name of the Sql Table.
	 */
	public final String short_field_without_quote;

	/**
	 * The name of the pointed Sql field not appended with its Sql Table, if this
	 * field is a foreign key
	 */
	public String short_pointed_field;

	/**
	 * The name of the pointed Sql field without quote not appended with its Sql Table, if this
	 * field is a foreign key
	 */
	public final String short_pointed_field_without_quote;


	/**
	 * Tells if this field is not null
	 */
	public final boolean not_null;

	public int sql_position = -1;

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
	 * @param pointed_table_alias
	 *            The name of the pointed Sql table alias given into a sql query, and if this field is a foreign key
	 * @param _not_null
	 *            tells if the field is not null
	 */
	public SqlField(boolean supportQuote, String _field, String _type, String _pointed_table, String pointed_table_alias, String _pointed_field, boolean _not_null) {
		if ((_pointed_table==null)!=(pointed_table_alias==null) && (_pointed_table==null)!=(_pointed_field==null))
			throw new NullPointerException();

		field_without_quote = _field.toUpperCase();
		type = _type.toUpperCase();
		pointed_table = _pointed_table == null ? null : _pointed_table.toUpperCase();
		this.pointed_table_alias=pointed_table_alias;
		pointed_field = _pointed_field == null ? null : (pointed_table_alias+"."+_pointed_field.substring(_pointed_field.lastIndexOf('.')+1)).toUpperCase();


		int index = -1;
		for (int i = 0; i < field_without_quote.length(); i++) {
			if (field_without_quote.charAt(i) == '.') {
				index = i + 1;
				break;
			}
		}
		if (index != -1) {
			short_field_without_quote = field_without_quote.substring(index);
			if (supportQuote) {
				short_field = "`" + short_field_without_quote + "`";
				field = field_without_quote.substring(0, index) + "`" + field_without_quote.substring(index) + "`";
			}
			else {
				short_field = short_field_without_quote;
				field = field_without_quote;
			}

		}
		else {
			short_field_without_quote = field_without_quote;
			if (supportQuote) {
				short_field = "`" + field_without_quote + "`";
			}
			else
				short_field = field_without_quote;

			field=short_field;
		}

		if (pointed_field != null) {
			index = -1;
			for (int i = 0; i < pointed_field.length(); i++) {
				if (pointed_field.charAt(i) == '.') {
					index = i + 1;
					break;
				}
			}
			if (index != -1) {
				short_pointed_field = pointed_field.substring(index);
			}
			else
				short_pointed_field = pointed_field;
		} else
			short_pointed_field = null;
		if (pointed_table!=null && pointed_field!=null)
		{
			pointed_table_without_quote=pointed_table.replace("`", "");
			pointed_field_without_quote=pointed_field.replace("`", "");
			short_pointed_field_without_quote=short_pointed_field.replace("`", "");
		}
		else {
			pointed_table_without_quote = null;
			pointed_field_without_quote = null;
			short_pointed_field_without_quote=null;
		}
		this.not_null = _not_null;
	}
}
