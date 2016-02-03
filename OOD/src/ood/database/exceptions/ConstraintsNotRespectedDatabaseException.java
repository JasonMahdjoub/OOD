/*
 * Object Oriented Database (created by Jason MAHDJOUB (jason.mahdjoub@free.fr)) Copyright (c)
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



package ood.database.exceptions;

/**
 * This exception is generated when the constraints of the database are not respected. 
 * For example, if you delete a record through the function {@link ood.database.Table#removeRecord(ood.database.DatabaseRecord)}, and if this record is pointed by another record through its foreign key, this exception will be generated. Note that in this example, the problem does not occur with the function {@link ood.database.Table#removeRecordWithCascade(ood.database.DatabaseRecord)}.
 * @author Jason Mahdjoub
 * @version 1.0
 */
public class ConstraintsNotRespectedDatabaseException extends DatabaseException
{

    public ConstraintsNotRespectedDatabaseException(String _message)
    {
	super(_message);
    }
    public ConstraintsNotRespectedDatabaseException(String _message, Exception e)
    {
	super(_message, e);
    }

    /**
     * 
     */
    private static final long serialVersionUID = 6609743812035622427L;

}
