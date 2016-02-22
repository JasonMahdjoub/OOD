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


package com.distrimind.ood.database;


/**
 * The database record class is a generic abstract class destinated to represent a database record with each of its fields.
 * The user must inherit this class to declare a type of record. He must declare it into the correspondent table/class as a public class. Moreover, he must name the child class 'Record'.
 * Every field which must be included into the database must have an annotation ({@link com.distrimind.ood.database.annotations.Field}, {@link com.distrimind.ood.database.annotations.PrimaryKey}, {@link com.distrimind.ood.database.annotations.AutoPrimaryKey}, {@link com.distrimind.ood.database.annotations.RandomPrimaryKey}, {@link com.distrimind.ood.database.annotations.NotNull}, {@link com.distrimind.ood.database.annotations.Unique}, {@link com.distrimind.ood.database.annotations.ForeignKey}).
 * If no annotation is given, the corresponding field will not be added into the database.
 *  
 * Note that the native types are always NotNull. 
 * Fields which have the annotation {@link com.distrimind.ood.database.annotations.AutoPrimaryKey} must be 'int' or 'short' values.
 * Fields which have the annotation {@link com.distrimind.ood.database.annotations.RandomPrimaryKey} must be 'long' values.
 * Fields which have the annotation {@link com.distrimind.ood.database.annotations.ForeignKey} must be DatabaseRecord instances. 
 *
 * Never alter a field directly throw a function of the child class. Do it with the function {@link com.distrimind.ood.database.Table#alterRecord(DatabaseRecord, java.util.Map)}.
 * Moreover, the constructor of the child class must be protected with no parameter (if this constraint is not respected, an exception will be generated). So it is not possible to instantiate directly this class. To do that, you must use the function {@link com.distrimind.ood.database.Table#addRecord(java.util.Map)}, or the function {@link com.distrimind.ood.database.Table#addRecords(java.util.Map...)}. 
 * 
 * The inner database fields do not need to be initialized. However, fields which have no annotation (which are not included into the database) are not concerned.
 *  
 * @author Jason Mahdjoub
 * @version 1.0
 */
public abstract class DatabaseRecord
{
    protected DatabaseRecord()
    {
	
    }
    
    @Deprecated @Override public boolean equals(Object obj)
    {
	throw new IllegalAccessError("This function is deprecated.");
    }
    
}
