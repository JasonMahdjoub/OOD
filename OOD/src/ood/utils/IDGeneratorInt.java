/*
 * CIPORG (created by Jason MAHDJOUB (jason.mahdjoub@free.fr)) Copyright (c)
 * 2011, JBoss Inc., and individual contributors as indicated by the @authors
 * tag. See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License, or (at your option)
 * any later version.
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

package ood.utils;

import java.io.Serializable;

/**
 * Represent a set of unique identifiers until they are released.
 * @author Jason Mahdjoub
 * @version 1.1
 * 
 *
 */
public final class IDGeneratorInt implements Serializable
{
    /**
     * 
     */
    private static final long serialVersionUID = 6578160385229331890L;
    
    private int[] m_ids;
    private int m_size=0;
    private final int m_reserve_max;
    //private final int m_id_start;
    private int m_last_id;
    
    private int getReserve() {return m_ids.length-m_size;}
    
    public IDGeneratorInt()
    {
	//m_id_start=0;
	m_reserve_max=20;
	m_ids=new int[m_reserve_max];
	m_last_id=-1;
    }
    public IDGeneratorInt(int _reserve_max, int _id_start)
    {
	//m_id_start=_id_start;
	if (_reserve_max<0) _reserve_max=0;
	m_reserve_max=_reserve_max;
	m_ids=new int[m_reserve_max];
	m_last_id=_id_start-1;
    }
    public IDGeneratorInt(int _id_start)
    {
	//m_id_start=_id_start;
	m_reserve_max=20;
	m_ids=new int[m_reserve_max];
	m_last_id=_id_start-1;
    }
    
    public int getNewID()
    {
	int res=++m_last_id;
	int i=-1;
	if (m_size>0)
	{
	    if (m_ids[0]>res)
		i=0;
	    else if (m_ids[m_size-1]<res)
		i=m_size;
	}
	if (i==-1)
	{
	    for (i=0;i<m_size;++i, ++res)
		if (m_ids[i]>res) break;
	}
	if (i<m_size)
	{
	    if (getReserve()>0)
	    {
		for (int j=m_size;j>i;j--)
		{
		    m_ids[j]=m_ids[j-1];
		}
		m_ids[i]=res;
		++m_size;
	    }
	    else
	    {
		int[] ids=new int[m_size+m_reserve_max+1];
		System.arraycopy(m_ids, 0, ids, 0, i);
		ids[i]=res;
		System.arraycopy(m_ids, i+1, ids, i, m_size-i);
		m_ids=ids;
		++m_size;
	    }
	}
	else
	{
	    if (getReserve()<=0)
	    {
		int []ids=new int[m_size+m_reserve_max+1];
		System.arraycopy(m_ids, 0, ids, 0, m_size);
		m_ids=ids;
	    }
	    m_ids[m_size++]=res;
	}
	return res;
    }
    public boolean removeID(int _val)
    {
	int i;
	for (i=0;i<m_size && m_ids[i]<=_val;i++)
	{
	    if (m_ids[i]==_val) break;
	}
	if (i<m_size && m_ids[i]==_val)
	{
	    if (i==m_size-1)
		m_last_id=_val-1;
	    
	    if (getReserve()>m_reserve_max*2)
	    {
		int []ids=new int[m_size+m_reserve_max];
		System.arraycopy(m_ids, 0, ids, 0, i);
		int s=m_size-i-1;
		if (s>0)
		    System.arraycopy(m_ids, i, ids, i+1, s);
		m_ids=ids;
		--m_size;
	    }
	    else
	    {
		for (int j=i+1;j<m_size;j++)
		{
		    m_ids[j-1]=m_ids[j];
		}
		--m_size;
	    }
	    return true;
	}
	return false;
    }
    
    public boolean hasID(int _val)
    {
	for (int i=0;i<m_size && m_ids[i]<=_val;i++)
	{
	    if (m_ids[i]==_val) return true;
	}
	return false;
    }

}
