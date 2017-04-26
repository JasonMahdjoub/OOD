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
package com.distrimind.ood.interpreter;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.SqlField;
import com.distrimind.ood.database.SqlFieldInstance;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseSyntaxException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public class RuleInstance implements QueryPart
{

    private final Rule rule;
    private final ArrayList<QueryPart> parts;

    
    public RuleInstance(Rule rule, ArrayList<QueryPart> parts, int off, int len)
    {
	if (rule==null)
	    throw new NullPointerException("rule");
	this.rule=rule;
	this.parts=new ArrayList<>();
	for (int i=off;i<off+len;i++)
	    this.parts.add(parts.get(i));
    }
    public RuleInstance(Rule rule, QueryPart part, QueryPart...parts)
    {
	if (rule==null)
	    throw new NullPointerException("rule");

	this.rule=rule;
	this.parts=new ArrayList<>();
	this.parts.add(part);
	for (QueryPart qp : parts)
	    this.parts.add(qp);
	
    }
    
    @Override
    public Object getContent()
    {
	//TODO
	return null;
    }
    
    @Override
    public String getBackusNaurNotation()
    {
	return "<"+rule.name()+">";
    }

    public Rule getRule()
    {
	return rule;
    }
    
    @Override
    public <T extends DatabaseRecord> boolean isMultiType(Table<T> table, Map<String, Object> parameters) throws DatabaseSyntaxException
    {
	switch(rule)
	{
	    case COMPARE:
		return false;
	    case EXPRESSION:case FACTEUR:
		if (parts.size()==1)
		{
		    return parts.get(0).isMultiType(table, parameters);
		}
		else
		{
		    return false;
		}
	    case OP_ADD:
		return false;
	    case OP_COMP:
		return false;
	    case OP_CONDITION:
		return false;
	    case OP_MUL:
		return false;
	    case QUERY:
		return false;
	    case TERME:
	    {
		if (parts.size()==1)
		{
		    Symbol s=(Symbol)parts.get(0);
		    return s.isMultiType(table, parameters);
		}
		else
		{
		    return false;
		}
		    
	    }
	    default:
		throw new IllegalAccessError();
	    
	}
    }
    
    @Override
    public <T extends DatabaseRecord> boolean isEqualable(Table<T> table, Map<String, Object> parameters, QueryPart otherPart) throws DatabaseSyntaxException
    {
	String valueType=getValueType(table, parameters);
	if (valueType==null)
	    return false;
	String otherValueType=otherPart.getValueType(table, parameters);
	return valueType.equals(otherValueType);
    }

    @Override
    public <T extends DatabaseRecord> String getValueType(Table<T> table, Map<String, Object> parameters) throws DatabaseSyntaxException
    {
	switch(rule)
	{
	    case COMPARE:
		return "boolean";
	    case EXPRESSION:
		if (parts.size()==1)
		    return parts.get(0).getValueType(table, parameters);
		else
		    return "comparable";
	    case FACTEUR:
		if (parts.size()==1)
		    return parts.get(0).getValueType(table, parameters);
		else
		    return "comparable";
	    case OP_ADD:
		return "comparable";
	    case OP_COMP:
		return "boolean";
	    case OP_CONDITION:
		return "boolean";
	    case OP_MUL:
		return "comparable";
	    case QUERY:
		return "boolean";
	    case TERME:
		if (parts.size()==1)
		    return parts.get(0).getValueType(table, parameters);
		else
		    return parts.get(1).getValueType(table, parameters);
	    default:
		throw new IllegalAccessError();
	    
	}
    }
    @Override
    public boolean needParenthesis()
    {
	return (parts.size()==1 && parts.get(0).needParenthesis()) || (parts.size()==3 && (parts.get(0) instanceof Symbol) && ((Symbol)parts.get(0)).getType()==SymbolType.OPEN_PARENTHESIS && (parts.get(2) instanceof Symbol) && ((Symbol)parts.get(2)).getType()==SymbolType.CLOSE_PARENTHESIS);
    }
    public <T extends DatabaseRecord> BigDecimal getNumber(Table<T> table, Map<String, Object> parameters, T record) throws DatabaseSyntaxException
    {
	Object o=getComparable(table, parameters, record);
	if (o instanceof BigDecimal)
	    return (BigDecimal)o;
	else
	    throw new DatabaseSyntaxException(o+" of type "+o.getClass()+" is not a number !");
    }
    public <T extends DatabaseRecord> Object getComparable(Table<T> table, Map<String, Object> parameters, T record) throws DatabaseSyntaxException
    {
	switch(rule)
	{
	    case COMPARE:case QUERY:
		throw new IllegalAccessError();
	    case EXPRESSION:case FACTEUR:
		if (parts.size()==1)
		{
		    return ((RuleInstance)parts.get(0)).getComparable(table, parameters, record);
		}
		else if (parts.size()==3)
		{
		    RuleInstance ri1=(RuleInstance)parts.get(0);
		    RuleInstance ri3=(RuleInstance)parts.get(2);
		    BigDecimal v1=ri1.getNumber(table, parameters, record);
		    BigDecimal v2=ri3.getNumber(table, parameters, record);
		    
		    Symbol comp=(Symbol)((RuleInstance)parts.get(1)).parts.get(0);
		    
		    if (comp.getType()==SymbolType.ADD_OPERATOR)
		    {
			return v1.add(v2);
		    }
		    else if (comp.getType()==SymbolType.SUB_OPERATOR)
		    {
			return v1.subtract(v2);
		    }
		    else if (comp.getType()==SymbolType.MUL_OPETATOR)
		    {
			return v1.multiply(v2);
		    }
		    else if (comp.getType()==SymbolType.DIV_OPERATOR)
		    {
			return v1.divide(v2);
		    }
		    else
			throw new IllegalAccessError();
		}
		else
		    throw new IllegalAccessError();
		
		
	    case OP_COMP:case OP_ADD:case OP_CONDITION:case OP_MUL:
		throw new IllegalAccessError();
	    case TERME:
		if (parts.size()==1)
		{
		    Symbol s=(Symbol)parts.get(0);
		    if (s.getType()==SymbolType.IDENTIFIER)
		    {
			String fieldName=s.getSymbol();
			FieldAccessor fa=table.getFieldAccessor(fieldName);
			if (fa==null)
			    throw new DatabaseSyntaxException("Cannot find field "+fieldName+" into table "+table.getName());
			if (fa.isComparable())
			    return fa;
			else
			    throw new DatabaseSyntaxException("The "+fieldName+" into table "+table.getName()+" is not comparable !");
		    }
		    else if (s.getType()==SymbolType.PARAMETER)
		    {
			Object parameter1=parameters.get(s.getSymbol());
			if (parameter1==null)
			    throw new DatabaseSyntaxException("Cannot find parameter "+s.getSymbol());
			return parameter1;
		    }
		    else
			return new BigDecimal(s.getSymbol());
		}
		else if (parts.size()==3)
		{
		    return ((RuleInstance)parts.get(1)).getNumber(table, parameters, record);
		}
		else
		    throw new IllegalAccessError();
		
	    
	}
	throw new IllegalAccessError();
    }
    
    public <T extends DatabaseRecord> Object getEquallable(Table<T> table, Map<String, Object> parameters, T record) throws DatabaseSyntaxException
    {
	switch(rule)
	{
	    case EXPRESSION:case FACTEUR:
		return getNumber(table, parameters, record);
	    case OP_COMP:case OP_ADD:case OP_CONDITION:case OP_MUL:case COMPARE:case QUERY:
		throw new IllegalAccessError();
	    case TERME:
		if (parts.size()==1)
		{
		    Symbol s=(Symbol)parts.get(0);
		    if (s.getType()==SymbolType.IDENTIFIER)
		    {
			String fieldName=s.getSymbol();
			FieldAccessor fa=table.getFieldAccessor(fieldName);
			if (fa==null)
			    throw new DatabaseSyntaxException("Cannot find field "+fieldName+" into table "+table.getName());
			return fa;
		    }
		    else if (s.getType()==SymbolType.PARAMETER)
		    {
			Object parameter1=parameters.get(s.getSymbol());
			if (parameter1==null)
			    throw new DatabaseSyntaxException("Cannot find parameter "+s.getSymbol());
			return parameter1;
		    }
		    else
			return new BigDecimal(s.getSymbol());
		}
		else if (parts.size()==3)
		{
		    return ((RuleInstance)parts.get(1)).getNumber(table, parameters, record);
		}
		else
		    throw new IllegalAccessError();
		
	    
	}
	throw new IllegalAccessError();
    }
    
    
    public <T extends DatabaseRecord> int compareTo(Table<T> table, T record, Object o1, Object o2) throws DatabaseSyntaxException
    {
	try
	{
	    if ((!FieldAccessor.class.isAssignableFrom(o1.getClass()) && FieldAccessor.class.isAssignableFrom(o2.getClass()))
		    ||
		    (!BigDecimal.class.isAssignableFrom(o1.getClass()) && BigDecimal.class.isAssignableFrom(o2.getClass())))
	    {
		Object o=o1;
		o1=o2;
		o2=o;
	    }
	    if (FieldAccessor.class.isAssignableFrom(o1.getClass()))
		o1=((FieldAccessor)o1).getValue(record);
	    if (FieldAccessor.class.isAssignableFrom(o2.getClass()))
		o2=((FieldAccessor)o2).getValue(record);
	    if (BigDecimal.class.isAssignableFrom(o1.getClass()))
	    {
		if (BigDecimal.class.isAssignableFrom(o2.getClass()))
		    return ((BigDecimal)o1).compareTo((BigDecimal)o2);
		else if (Number.class.isAssignableFrom(o2.getClass()))
		{
		    return ((BigDecimal)o1).compareTo(new BigDecimal(o2.toString()));
		}
	    }
	    else if (Number.class.isAssignableFrom(o1.getClass()))
	    {
		for (Method m : o1.getClass().getDeclaredMethods())
		{
		    if (m.equals("compareTo") && m.getParameterTypes().length==1)
		    {
			return ((Integer)m.invoke(o1, o2)).intValue();
		    }
		}
	    }
	}
	catch(Exception e)
	{
	    throw new DatabaseSyntaxException("Unexpected exception ! ", e);
	}
	throw new DatabaseSyntaxException("Cannot compare "+o1+" and "+o2);
	    
    }
    public <T extends DatabaseRecord> boolean equals(Table<T> table, T record, Object o1, Object o2) throws DatabaseSyntaxException
    {
	try
	{
	    if ((!FieldAccessor.class.isAssignableFrom(o1.getClass()) && FieldAccessor.class.isAssignableFrom(o2.getClass()))
		    ||
		    (!BigDecimal.class.isAssignableFrom(o1.getClass()) && BigDecimal.class.isAssignableFrom(o2.getClass())))
	    {
		Object o=o1;
		o1=o2;
		o2=o;
	    }
	    if (BigDecimal.class.isAssignableFrom(o1.getClass()))
	    {
		if (BigDecimal.class.isAssignableFrom(o2.getClass()))
		    return ((BigDecimal)o1).equals(o2);
		else if (Number.class.isAssignableFrom(o2.getClass()))
		{
		    return ((BigDecimal)o1).equals(new BigDecimal(o2.toString()));
		}
		else
		    return false;
	    }
	    else if (FieldAccessor.class.isAssignableFrom(o1.getClass()))
	    {
		return ((FieldAccessor)o1).equals(record, o2);
	    }
	    else
		return o1.equals(o2);

	}
	catch(Exception e)
	{
	    throw new DatabaseSyntaxException("Unexpected exception ! ", e);
	}
	
	    
    }
    
    public <T extends DatabaseRecord> boolean isConcernedBy(Table<T> table, Map<String, Object> parameters, T record) throws DatabaseSyntaxException
    {
	switch(rule)
	{
	    case COMPARE:
		if (parts.size()==3)
		{
		    RuleInstance ri2=(RuleInstance)parts.get(1);
		    if (ri2.getRule().equals(Rule.QUERY))
		    {
			return ri2.isConcernedBy(table, parameters, record);
		    }
		    else
		    {
			RuleInstance ri1=(RuleInstance)parts.get(0);
			RuleInstance ri3=(RuleInstance)parts.get(2);
			
			Symbol comp=(Symbol)ri2.parts.get(0);
			if (comp.getType()==SymbolType.EQUAL_OPERATOR)
			{
			    return equals(table, record, ri1.getEquallable(table, parameters, record), ri3.getEquallable(table, parameters, record));
			}
			else
			{
			    int c=compareTo(table, record, ri1.getComparable(table, parameters, record), ri3.getComparable(table, parameters, record));
			    if (comp.getType()==SymbolType.LOWER_OPERATOR)
			    {
				return c<0;
			    }
			    else if (comp.getType()==SymbolType.GREATER_OPERATOR)
			    {
				return c>0;
			    }
			    else if (comp.getType()==SymbolType.LOWER_OR_EQUAL_OPERATOR)
			    {
				return c<=0;
			    }
			    else if (comp.getType()==SymbolType.GREATER_OR_EQUAL_OPERATOR)
			    {
				return c>0;
			    }
			    else
				throw new IllegalAccessError();
			}
		    }
		}
		else
		    throw new IllegalAccessError();
	    case QUERY:
		if (parts.size()==1)
		{
		    return ((RuleInstance)parts.get(0)).isConcernedBy(table, parameters, record);
		}
		else if (parts.size()==3)
		{
		    RuleInstance ri1=(RuleInstance)parts.get(0);
		    RuleInstance ri3=(RuleInstance)parts.get(2);
		    Symbol comp=(Symbol)((RuleInstance)parts.get(1)).parts.get(0);
		    
		    boolean vri1=ri1.isConcernedBy(table, parameters, record);
		    boolean vri2=ri3.isConcernedBy(table, parameters, record);
		    if (comp.getType()==SymbolType.ADD_OPERATOR)
			return vri1 && vri2;
		    else if (comp.getType()==SymbolType.OR_CONDITION)
			return vri1 || vri2;
		    else
			throw new IllegalAccessError();
		}
		else
		    throw new IllegalAccessError();
	    case TERME:case FACTEUR:case EXPRESSION:case OP_COMP:case OP_ADD:case OP_CONDITION:case OP_MUL:
		throw new IllegalAccessError();
	}
	throw new IllegalAccessError();
    }
    public <T extends DatabaseRecord> StringBuffer translateToSqlQuery(Table<T> table, Map<String, Object> parameters, Map<Integer, Object> outputParameters) throws DatabaseSyntaxException
    {
	return translateToSqlQuery(table, parameters, outputParameters, 1);
    }
    public <T extends DatabaseRecord> StringBuffer translateToSqlQuery(Table<T> table, Map<String, Object> parameters, Map<Integer, Object> outputParameters, int firstParameterIndex) throws DatabaseSyntaxException
    {
	return translateToSqlQuery(table, parameters, outputParameters, new AtomicInteger(firstParameterIndex));
    }
    <T extends DatabaseRecord> StringBuffer translateToSqlQuery(Table<T> table, Map<String, Object> parameters, Map<Integer, Object> outputParameters, AtomicInteger currentParameterID) throws DatabaseSyntaxException
    {
	switch(rule)
	{
	    case COMPARE:
		if (parts.size()==3)
		{
		    RuleInstance ri2=(RuleInstance)parts.get(1);
		    if (ri2.getRule().equals(Rule.QUERY))
		    {
			StringBuffer tmp=ri2.translateToSqlQuery(table, parameters, outputParameters, currentParameterID);
			StringBuffer sb=new StringBuffer(tmp.length()+2);
			sb.append("(");
			sb.append(tmp);
			sb.append(")");
			return sb;
		    }
		    else
		    {
			RuleInstance ri1=(RuleInstance)parts.get(0);
			RuleInstance ri3=(RuleInstance)parts.get(2);
			Symbol comp=(Symbol)ri2.parts.get(0);
			
			if (comp.getType()==SymbolType.EQUAL_OPERATOR)
			{
			    if (!ri1.isEqualable(table, parameters, ri3))
			    {
				throw new DatabaseSyntaxException("Cannot compare "+ri1.getContent()+" and "+ri3.getContent());
			    }
			    if (ri1.isMultiType(table, parameters))
			    {
				Symbol s1=((Symbol)((RuleInstance)((RuleInstance)((RuleInstance)ri1.parts.get(0)).parts.get(0)).parts.get(0)).parts.get(0));
				Symbol s2=((Symbol)((RuleInstance)((RuleInstance)((RuleInstance)ri1.parts.get(0)).parts.get(0)).parts.get(0)).parts.get(0));
				if (s1.getType()==SymbolType.PARAMETER && s2.getType()==SymbolType.PARAMETER)
				    throw new DatabaseSyntaxException("Cannot do comparison between two paramters");
				FieldAccessor fa1=null, fa2=null;
				Object parameter1=null,parameter2=null;

				if (s1.getType()==SymbolType.IDENTIFIER)
				{
				    String fieldName=s1.getSymbol();
				    fa1=table.getFieldAccessor(fieldName);
				    if (fa1==null)
					throw new DatabaseSyntaxException("Cannot find field "+fieldName+" into table "+table.getName());
				}
				else if (s1.getType()==SymbolType.PARAMETER)
				{
				    parameter1=parameters.get(s1.getSymbol());
				    if (parameter1==null)
					throw new DatabaseSyntaxException("Cannot find parameter "+s1.getSymbol());
					
				}
				else
				    throw new IllegalAccessError();
				
				if (s2.getType()==SymbolType.IDENTIFIER)
				{
				    String fieldName=s2.getSymbol();
				    fa2=table.getFieldAccessor(fieldName);
				    if (fa2==null)
					throw new DatabaseSyntaxException("Cannot find field "+fieldName+" into table "+table.getName());
				}
				else if (s2.getType()==SymbolType.PARAMETER)
				{
				    parameter2=parameters.get(s2.getSymbol());
				    if (parameter2==null)
					throw new DatabaseSyntaxException("Cannot find parameter "+s2.getSymbol());
					
				}
				else
				    throw new IllegalAccessError();
				
				
				if (s1.getType()==SymbolType.PARAMETER && s2.getType()==SymbolType.IDENTIFIER)
				{
				    fa1=fa2;
				    fa2=null;
				    parameter2=parameter1;
				    parameter1=null;
				}
				int fieldsNumber=0;
				StringBuffer res=new StringBuffer("");
				SqlField sfs[]=fa1.getDeclaredSqlFields();
				SqlFieldInstance sfis[]=null;
				try
				{
				    if (fa2==null)
					sfis=fa1.getSqlFieldsInstances(parameter1);
				}
				catch(DatabaseException e)
				{
				    throw new DatabaseSyntaxException("Database exception", e);
				}
				SqlField sfs2[]=null;
				if (fa2!=null)
				    sfs2=fa2.getDeclaredSqlFields();
				    
				for (SqlField sf : sfs)
				{
				    ++fieldsNumber;
				    if (fieldsNumber>1)
					res.append(" AND ");
				    res.append(sf.field);
				    res.append(" ");
				    res.append(comp.getContent());
				    res.append(" ");
				    if (fa2==null)
				    {
					boolean found=false;
					for (SqlFieldInstance sfi : sfis)
					{
					    if (sfi.field==sf.field)
					    {
						int id=currentParameterID.getAndIncrement();
						res.append("%"+id);
						outputParameters.put(new Integer(id), sfi.instance);
						found=true;
						break;
					    }
					}
					if (!found)
					    throw new DatabaseSyntaxException("Field not found. Unexpected error !");
					    
				    }
				    else
				    {
					if (fa2.getFieldName().equals(fa1.getFieldName()))
					    throw new DatabaseSyntaxException("Cannot compare two same fields : "+fa1.getFieldName());
					if (fa1.getClass()!=fa2.getClass())
					    throw new DatabaseSyntaxException("Cannot compare two fields whose type is different : "+fa1.getFieldName()+" and "+fa2.getFieldName());
					res.append(sfs2[fieldsNumber-1].field);
				    }
				}
				return res;
				
			    }
			}
			else
			{
			    if (!ri1.getValueType(table, parameters).equals("comparable") || !ri3.getValueType(table, parameters).equals("comparable"))
			    {
				throw new DatabaseSyntaxException("Cannot compare "+ri1.getContent()+" and "+ri3.getContent());
			    }
			}
			StringBuffer res=new StringBuffer();
			StringBuffer sb=ri1.translateToSqlQuery(table, parameters, outputParameters, currentParameterID);
			if (ri1.needParenthesis())
			    res.append("(");
			res.append(sb);
			if (ri1.needParenthesis())
			    res.append(")");
			res.append(" ");
			res.append(comp.getContent());
			res.append(" ");
			sb=ri3.translateToSqlQuery(table, parameters, outputParameters, currentParameterID);
			if (ri3.needParenthesis())
			    res.append("(");
			res.append(sb);
			if (ri3.needParenthesis())
			    res.append(")");
			return res;
		    }
		}
		else
		    throw new IllegalAccessError();
		
	    case EXPRESSION:case FACTEUR:case QUERY:
		if (parts.size()==1)
		{
		    return ((RuleInstance)parts.get(0)).translateToSqlQuery(table, parameters, outputParameters, currentParameterID);
		}
		else if (parts.size()==3)
		{
		    RuleInstance ri1=(RuleInstance)parts.get(0);
		    RuleInstance ri3=(RuleInstance)parts.get(2);
		    if (rule==Rule.QUERY)
		    {
			if (!ri1.getValueType(table, parameters).equals("boolean") || !ri3.getValueType(table, parameters).equals("boolean"))
			    throw new DatabaseSyntaxException("Cannot make test with "+ri1.getContent()+" and "+ri3.getContent());
		    }
		    else
		    {
			if (!ri1.getValueType(table, parameters).equals("comparable") || !ri3.getValueType(table, parameters).equals("comparable"))
			    throw new DatabaseSyntaxException("Cannot mul/div "+ri1.getContent()+" and "+ri3.getContent());
		    }
		    
		    StringBuffer sb=new StringBuffer();
		    if (ri1.needParenthesis())
			sb.append("(");
		    sb.append(ri1.translateToSqlQuery(table, parameters, outputParameters, currentParameterID));
		    if (ri1.needParenthesis())
			sb.append(")");
		    Symbol comp=(Symbol)((RuleInstance)parts.get(1)).parts.get(0);
		    sb.append(" ");
		    sb.append(comp.getSymbol());
		    sb.append(" ");
		    if (ri3.needParenthesis())
			sb.append("(");
		    sb.append(ri3.translateToSqlQuery(table, parameters, outputParameters, currentParameterID));
		    if (ri3.needParenthesis())
			sb.append(")");
		}
		else
		    throw new IllegalAccessError();
		break;
	    case OP_COMP:case OP_ADD:case OP_CONDITION:case OP_MUL:
		throw new IllegalAccessError();
	    case TERME:
		if (parts.size()==1)
		{
		    Symbol s=(Symbol)parts.get(0);
		    if (s.getType()==SymbolType.IDENTIFIER)
		    {
			String fieldName=s.getSymbol();
			FieldAccessor fa=table.getFieldAccessor(fieldName);
			if (fa==null)
			    throw new DatabaseSyntaxException("Cannot find field "+fieldName+" into table "+table.getName());
			SqlField sfs[]=fa.getDeclaredSqlFields();
			if (sfs.length>1)
			    throw new IllegalAccessError();
			return new StringBuffer(sfs[0].field);
		    }
		    else if (s.getType()==SymbolType.PARAMETER)
		    {
			Object parameter1=parameters.get(s.getSymbol());
			if (parameter1==null)
			    throw new DatabaseSyntaxException("Cannot find parameter "+s.getSymbol());
			int id=currentParameterID.getAndIncrement();
			outputParameters.put(new Integer(id), parameter1);
			return new StringBuffer("%"+id);
		    }
		    else
			return new StringBuffer(s.getSymbol());
		    
		}
		else if (parts.size()==3)
		{
		    StringBuffer sb=new StringBuffer();
		    sb.append("(");
		    sb.append(((RuleInstance)parts.get(1)).translateToSqlQuery(table, parameters, outputParameters, currentParameterID));
		    sb.append(")");
		}
		else
		    throw new IllegalAccessError();
		
	    
	}
	throw new IllegalAccessError();
    }
    
}
