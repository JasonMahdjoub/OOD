
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

import com.distrimind.ood.database.exceptions.ConstraintsNotRespectedDatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

/**
 * This class enables to group results according given table fields. It is
 * equivalent to the function "GROUP BY" in the SQL language.
 * 
 * To get an instance of this class, call the functions
 * {@link com.distrimind.ood.database.Table#getGroupedResults(String...)} or
 * {@link com.distrimind.ood.database.Table#getGroupedResults(Collection, String...)}.
 * 
 * @author Jason Mahdoub
 * @since 1.0
 * @version 1.0
 * @param <T>
 *            The type of the DatabaseRecord.
 */
public final class GroupedResults<T extends DatabaseRecord> {
	private class Field {
		private final String field_name;
		private final FieldAccessor field ;

		public Field(String _field_name) throws ConstraintsNotRespectedDatabaseException {
			field_name = _field_name;
			this.field=GroupedResults.this.table.getFieldAccessor(_field_name);
			if (this.field==null)
				throw new ConstraintsNotRespectedDatabaseException("The field " + field_name + " does not exists.");

			/*ArrayList<String> strings = splitPoint(field_name);
			Table<?> current_table = GroupedResults.this.table;

			for (String f : strings) {
				if (current_table == null)
					throw new ConstraintsNotRespectedDatabaseException("The field " + field_name + " does not exists.");
				FieldAccessor founded_field = null;
				for (FieldAccessor fa : current_table.getFieldAccessors()) {
					if (fa.getFieldName().equals(f)) {
						founded_field = fa;
						break;
					}
				}
				if (founded_field == null)
					throw new ConstraintsNotRespectedDatabaseException("The field " + f
							+ " does not exist into the class/table " + current_table.getClass().getSqlTableName());

				fields.add(founded_field);

				if (founded_field.isForeignKey())
					current_table = ((ForeignKeyFieldAccessor) founded_field).getPointedTable();
				else
					current_table = null;
			}*/
		}

		public String getName() {
			return field_name;
		}

		/*private ArrayList<String> splitPoint(String s) {
			ArrayList<String> res = new ArrayList<>(10);
			int last_index = 0;
			for (int i = 0; i < s.length(); i++) {
				if (s.charAt(i) == '.') {
					if (i != last_index) {
						res.add(s.substring(last_index, i));
					}
					last_index = i + 1;
				}
			}
			if (s.length() != last_index) {
				res.add(s.substring(last_index));
			}

			return res;
		}*/

		public boolean equals(T o1, Object o2) throws DatabaseException {
			Object record=GroupedResults.this.table.getFieldAccessorAndValue(o1, field_name).getValue();
			/*Object r1 = o1;
			for (int i = 0; i < fields.size() - 1; i++) {
				if (r1 == null)
					return null == o2;
				ForeignKeyFieldAccessor f = (ForeignKeyFieldAccessor) fields.get(i);
				r1 = f.getValue(r1);
			}
			if (r1 == null)
				return null == o2;
			FieldAccessor fa = fields.get(fields.size() - 1);*/
			return this.field.equals(record, o2);
		}

		public Object getValue(T o) throws DatabaseException {
			Object record=GroupedResults.this.table.getFieldAccessorAndValue(o, field_name).getValue();
			return this.field.getValue(record);
			/*Object r = o;
			for (int i = 0; i < fields.size() - 1; i++) {
				if (r == null)
					return null;
				ForeignKeyFieldAccessor f = (ForeignKeyFieldAccessor) fields.get(i);
				r = f.getValue(r);
			}
			if (r == null)
				return null;
			FieldAccessor fa = fields.get(fields.size() - 1);
			return fa.getValue(r);*/
		}

	}

	protected final Table<T> table;
	protected final ArrayList<Field> group_definition = new ArrayList<>();
	private final ArrayList<Group> groups = new ArrayList<>();

	@SuppressWarnings("unchecked")
	private GroupedResults(DatabaseWrapper _sql_conncection, int databaseVersion, Collection<T> _records, Class<T> _class_record,
			String... _fields) throws DatabaseException {
		Class<T> class_record;

		class_record = _class_record;

		table = (Table<T>) _sql_conncection.getTableInstance(Table.getTableClass(class_record), databaseVersion);

		if (_fields.length == 0)
			throw new ConstraintsNotRespectedDatabaseException("It must have at mean one field to use.");

		for (String _field : _fields) {
			group_definition.add(new Field(_field));
		}

		if (_records != null)
			addRecords(_records);
	}

	/**
	 * Add a record and sort it according the group definition.
	 * 
	 * @param _record
	 *            the record
	 * @throws DatabaseException
	 *             if a database exception occurs
	 */
	public void addRecord(T _record) throws DatabaseException {
		for (Group g : groups) {
			if (g.addRecord(_record))
				return;
		}
		groups.add(new Group(_record));
	}

	/**
	 * Add records and sort them according the group definition.
	 * 
	 * @param _records
	 *            the records to add
	 * @throws DatabaseException
	 *             if a database exception occurs
	 */
	public void addRecords(Collection<T> _records) throws DatabaseException {
		for (T r : _records) {
			addRecord(r);
		}
	}

	/**
	 * Returns the list of groups each containing a list of records.
	 * 
	 * @return the groups.
	 */
	public ArrayList<Group> getGroupedResults() {
		return groups;
	}

	/**
	 * This class represent a group (according a set of table fields) of records. It
	 * is relative to the use of the class
	 * {@link com.distrimind.ood.database.GroupedResults}.
	 * 
	 * @author Jason Mahdjoub
	 * @since 1.0
	 * @version 1.0
	 *
	 */
	public class Group {
		protected final HashMap<String, Object> group;
		protected final int hash_code;
		protected final ArrayList<T> results;

		protected Group(HashMap<String, Object> _group) {
			group = _group;
			hash_code = group.hashCode();
			results = new ArrayList<>();
		}

		protected Group(T _record) throws DatabaseException {
			group = new HashMap<>();
			for (Field fa : group_definition) {
				group.put(fa.getName(), fa.getValue(_record));
			}

			hash_code = group.hashCode();
			results = new ArrayList<>();
			results.add(_record);
		}

		protected boolean addRecord(T _record) throws DatabaseException {
			for (Field fa : group_definition) {
				if (!fa.equals(_record, group.get(fa.getName())))
					return false;
			}
			results.add(_record);
			return true;
		}

		/**
		 * Returns the identity of the group by association a set of table fields with
		 * their instance.
		 * 
		 * @return identity of the group
		 */
		public HashMap<String, Object> getGroupIdentity() {
			return group;
		}

		@Override
		public int hashCode() {
			return hash_code;
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean equals(Object o) {
			if (o instanceof GroupedResults.Group)
				return equals((GroupedResults<T>.Group) o);
			else
				return false;
		}

		public boolean equals(Group _group) {
			for (String s : group.keySet()) {
				if (!group.get(s).equals(_group.group.get(s)))
					return false;
			}
			return true;
		}

		/**
		 * Returns the records associated to this group.
		 * 
		 * @return the records associated to this group.
		 */
		public ArrayList<T> getResults() {
			return results;
		}

	}

}
