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

import java.util.Map;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.DatabaseSyntaxException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public class Symbol implements QueryPart
{
    private SymbolType type;
    private String symbol;
    public Symbol(SymbolType type, String symbol)
    {
	if (type==null)
	    throw new NullPointerException("type");
	if (symbol==null)
	    throw new NullPointerException("symbol");
	
	this.type=type;
	this.symbol=symbol;
	
    }
    public SymbolType getType()
    {
	return type;
    }
    
    public String getSymbol()
    {
	return symbol;
    }
    @Override
    public Object getContent()
    {
	return getSymbol();
    }

    public RuleInstance getRule()
    {
	switch(type)
	{
	    case ADD_OPERATOR:case SUB_OPERATOR:
		return new RuleInstance(Rule.OP_ADD, this);
	    case AND_CONDITION:case OR_CONDITION:
		return new RuleInstance(Rule.OP_CONDITION, this);
	    case CLOSE_PARENTHESIS:
		return null;
	    case DIV_OPERATOR:case MUL_OPETATOR:
		return new RuleInstance(Rule.OP_MUL, this);
	    case EQUAL_OPERATOR:case GREATER_OPERATOR:case GREATER_OR_EQUAL_OPERATOR:case LOWER_OPERATOR:case LOWER_OR_EQUAL_OPERATOR:
		return new RuleInstance(Rule.OP_COMP, this);
	    case IDENTIFIER:case NUMBER:case PARAMETER:
		return new RuleInstance(Rule.TERME, this);
	    default:
		return null;
	}
    }
        
    @Override
    public String getBackusNaurNotation()
    {
	if (type==SymbolType.OPEN_PARENTHESIS || type==SymbolType.CLOSE_PARENTHESIS)
	    return type.getContent();
	else
	    return "<"+getType().name()+">";
    }
    
    @Override
    public <T extends DatabaseRecord> boolean isMultiType(Table<T> table, Map<String, Object> parameters) throws DatabaseSyntaxException
    {
	    if (getType()==SymbolType.IDENTIFIER)
	    {
		FieldAccessor fa=table.getFieldAccessor(getSymbol());
		if (fa==null)
		    throw new DatabaseSyntaxException("Cannot find field "+getSymbol()+" into table "+table.getName());
		return fa.getDeclaredSqlFields().length>1;
	    }
	    else if (getType()==SymbolType.PARAMETER)
	    {
		Object p=parameters.get(getSymbol());
		if (p==null)
		    throw new DatabaseSyntaxException("Impossible to find parameter "+getSymbol());
		FieldAccessor fa=getFieldAccessor(table, p);
		if (fa!=null)
		    return fa.getDeclaredSqlFields().length>1;
		else
		    return false;
	    }
	    else
		return false;

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
	if (getType()==SymbolType.IDENTIFIER)
	{
	    FieldAccessor fa=table.getFieldAccessor(getSymbol());
	    if (fa==null)
		throw new DatabaseSyntaxException("Cannot find field "+getSymbol()+" into table "+table.getName());
	    else if (fa.isComparable())
		return "comparable";
	    else
		return fa.getFieldName();
	}
	else if (getType()==SymbolType.PARAMETER)
	{
	    Object p=parameters.get(getSymbol());
	    if (p==null)
		throw new DatabaseSyntaxException("Impossible to find parameter "+getSymbol());
	    FieldAccessor fa=getFieldAccessor(table, p);
	    if (fa!=null && fa.isComparable())
		return "comparable";
	    else
		return p.getClass().getName();
	}
	else if (getType()==SymbolType.NUMBER)
	    return "comparable";
	else
	    return getType().name();
	    
    }
    private <T extends DatabaseRecord> FieldAccessor getFieldAccessor(Table<T> table, Object o)
    {
	if (o==null)
	    return null;
	for (FieldAccessor fa : table.getFieldAccessors())
	{
	    if (fa.getField().getDeclaringClass().isAssignableFrom(o.getClass()))
		return fa;
	}
	return null;
    }
    
    public boolean needParenthesis()
    {
	return false;
    }
}
