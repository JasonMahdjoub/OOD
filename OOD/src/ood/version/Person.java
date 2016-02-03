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

package ood.version;

public class Person
{
    protected String m_name, m_first_name;
    
    public Person(String _name, String _first_name)
    {
	m_name=_name.toUpperCase();
	if (_first_name.length()>0)
	    m_first_name=_first_name.substring(0,1).toUpperCase()+_first_name.substring(1).toLowerCase();
	else 
	    m_first_name="";
    }

    public String getFirstName()
    {
	return m_first_name;
    }

    public String getName()
    {
	return m_name;
    }
    
    @Override public String toString()
    {
	return m_first_name+" "+m_name;
    }
}
