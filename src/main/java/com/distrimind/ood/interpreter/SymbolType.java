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
package com.distrimind.ood.interpreter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Type of symbol
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public enum SymbolType {
	LOWER_COMPARATOR(true, false, false, true, false, "^<", new String[] { "<" }, "<"),
	EQUAL_COMPARATOR(true, false, false, true, false, "^(=){1,2}", new String[] { "=", "==" }, "="),
	NOT_EQUAL_COMPARATOR(true, false, false, true, false, "^(!=|<>)", new String[] { "!=", "<>" },"<>"),
	GREATER_COMPARATOR(true, false, false, true, false, "^>", new String[] { ">" }, ">"),
	LOWER_OR_EQUAL_COMPARATOR( true, false, false, true, false, "^<=", new String[] { "<=" }, "<="),
	GREATER_OR_EQUAL_COMPARATOR(true,false, false, true,false, "^>=", new String[] { ">=" }, ">="),
	OPEN_PARENTHESIS(true, false, false, false, false, "^\\(", new String[] { "(" }, "("),
	CLOSE_PARENTHESIS(true, false, false, false, false, "^\\)", new String[] { ")" }, ")"),
	AND_CONDITION(false,true, false, false,true, "^[ ]?[Aa][Nn][Dd][ \\(]", null, " AND "),
	OR_CONDITION(false, true, false, false,true, "^[ ]?[Oo][Rr][ \\(]", null, " OR "),


	LIKE(true, false, false, false,false, "^[ ]?(L|l)(I|i)(K|k)(E|e)[ ]", new String[] { " LIKE " }, " LIKE "),
	NOT_LIKE(true, false,false, false,true,"^[ ]?(N|n)(O|o)(T|t)[ _](L|l)(I|i)(K|k)(E|e)[ ]",new String[] {" NOT LIKE "," NOT_LIKE " }," NOT LIKE "),
	IS(true, false,false, false,false,"^[ ]?(I|i)(S|s)[ ]",new String[] {" IS "}," IS "),
	IS_NOT(true, false,false, false,true,"^[ ]?(I|i)(S|s)[ _](N|n)(O|o)(T|t)[ ]",new String[] {" IS NOT "," IS_NOT " }," IS NOT "),
	IN(true, false,false, false,false,"^[ ]?(I|i)(N|n)[ ]",new String[] {" IN "}," IN "),
	NOT_IN(true, false,false, false,true,"^[ ]?(N|n)(O|o)(T|t)[ _](I|i)(N|n)[ ]",new String[] {" NOT IN ", " NOT_IN "}," NOT IN "),
	PLUS(true, false,true, false,false,"^[+]",new String[] {"+"},"+"),
	MINUS(true, false,true, false, false,"^[-]",new String[] {"-"},"-"),
	MULTIPLY(true, false,true, false,false,"^[*]",new String[] {"*"},"*"),
	DIVIDE(true, false,true, false,false,"^/",new String[] {"/"},"/"),
	MODULO(true, false,true, false,false,"^[ ]?MOD[ ]",new String[] {" MOD "},"%"),
	NULL(false, false, false, false,false, "^(N|n)(U|u)(L|l)(L|l)( |\\)|$)", null, "NULL"),
	NUMBER(false, false, false, false,false, "^(([0-9]+(\\.[0-9]+)?(E(\\-|\\+)?[0-9]+)?)|([0-9]*\\.[0-9]+(E(\\-|\\+)?[0-9]+)?))", null, null),
	PARAMETER( false, false, false, false,false, "^(\\%|\\:)[a-zA-Z\\-_][0-9a-zA-Z\\-_]*", null, null),
	STRING(false, false, false, false,false, "^(\"|\\')[\\p{Alnum}\\p{Blank}\\!\\#\\$\\%\\&\\(\\)\\*\\+\\,\\-\\.\\/:;\\<\\=\\>\\?\\@\\[\\\\\\]\\^_\\`\\{\\|\\}\\~]+(\"|\\')", null, null),
	IDENTIFIER(false, false, false, false,false, "^[a-zA-Z][a-zA-Z0-9\\._\\-]*", null, null),
	;

	private final Pattern pattern;
	private final String content;
	private final String[] matches;
	private final boolean isOperator;
	private final boolean isCondition;
	private final boolean isMathematicalOperator;
	private final boolean isComparator;
	private final boolean mustHaveSpaces;

	SymbolType(boolean isOperator, boolean isCondition, boolean isMathematicalOperator, boolean isComparator, boolean mustHaveSpaces, String regex, String[] matches,
			   String content) {
		this.isOperator = isOperator;
		this.isCondition = isCondition;
		this.isMathematicalOperator = isMathematicalOperator;
		this.isComparator=isComparator;
		this.mustHaveSpaces = mustHaveSpaces;
		this.pattern = Pattern.compile(regex);
		this.content = content;
		this.matches = matches;
	}

	public String[] getMatches() {
		return matches;
	}

	public String getContent() {
		return content;
	}

	public int match(CharSequence symbol) {
		Matcher m=pattern.matcher(symbol);
		if (m.find())
		{
			if (m.start()!=0)
				throw new IllegalAccessError("symbolType="+this+", m.start()="+m.start()+", symbol : "+symbol);
			return m.end();
		}
		else
			return -1;
	}

	public boolean isOperator() {
		return isOperator;
	}

	public boolean isCondition() {
		return isCondition;
	}

	public boolean mustHaveSpaces() {
		return mustHaveSpaces;
	}

	/*public static Symbol getSymbol(String symbol) {
		SymbolType best = null;
		for (SymbolType st : SymbolType.values()) {
			if (st.match(symbol)) {
				best = st;
				if (st.isOperator() || st.isCondition()) {
					break;
				}

			}
		}
		if (best != null) {
			if (best == PARAMETER)
				return new Symbol(best, symbol.substring(1));
			else
				return new Symbol(best, symbol);

		}
		return null;
	}*/

	public static Pattern convertLikeStringToPattern(String likeContent) {
		return Pattern.compile(convertLikeStringToRegex(likeContent));
	}

	public static String convertLikeStringToRegex(String likeContent) {
		if (likeContent == null)
			throw new NullPointerException("likeContent");
		String regex;
		if (likeContent.startsWith("\"") && likeContent.endsWith("\""))
			regex = likeContent.substring(1, likeContent.length() - 1);
		else
			regex = likeContent;
		if (regex.startsWith("%"))
			regex = regex.substring(1);
		else
			regex = "^" + likeContent;
		if (regex.endsWith("%"))
			regex = regex.substring(0, regex.length() - 1);
		else
			regex = regex + "$";

		regex = regex.replace("_", ".");

		return regex;
	}

	public boolean isMathematicalOperator()
	{
		return isMathematicalOperator;
	}

	public boolean isComparator() {
		return isComparator;
	}
}
