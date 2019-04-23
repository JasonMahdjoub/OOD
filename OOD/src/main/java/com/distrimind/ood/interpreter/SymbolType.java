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

import java.util.regex.Pattern;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public enum SymbolType {
	/*
	 * ADDOPERATOR(true,false,false,"^\\+$",new String[]{"+"}, "+"),
	 * SUBOPERATOR(true,false,false,"^\\-$",new String[]{"-"}, "-"),
	 * MULOPETATOR(true,false,false,"^\\*$",new String[]{"*"}, "*"),
	 * DIVOPERATOR(true,false,false,"^/$",new String[]{"/"}, "/"),
	 */
	LOWEROPERATOR(true, false, false, "^<$", new String[] { "<" }, "<"), 
	EQUALOPERATOR(true, false, false, "^(=){1,2}$", new String[] { "=", "==" }, "="), 
	NOTEQUALOPERATOR(true, false, false, "^(!=|<>)$", new String[] { "!=", "<>" },"<>"), 
	GREATEROPERATOR(true, false, false, "^>$", new String[] { ">" }, ">"), 
	LOWEROREQUALOPERATOR( true, false, false, "^<=$", new String[] { "<=" }, "<="), GREATEROREQUALOPERATOR(true,false, false, "^>=$", new String[] { ">=" }, ">="), 
	OPEN_PARENTHESIS(true, false, false, "^\\($", new String[] { "(" }, "("), CLOSE_PARENTHESIS(true, false, false, "^\\)$", new String[] { ")" }, ")"), 
	ANDCONDITION(false,true, true, "^[Aa][Nn][Dd]$", null, "AND"), 
	ORCONDITION(false, true, true, "^[Oo][Rr]$", null, "OR"), 
	IDENTIFIER(false, false, false, "^[a-zA-Z][a-zA-Z0-9\\._\\-]*+$", null, null), 
	NUMBER(false, false, false, "^(\\+|\\-)?(([0-9]+(\\.[0-9]+)?(E(\\-|\\+)?[0-9]+)?)|([0-9]*\\.[0-9]+(E(\\-|\\+)?[0-9]+)?))$", null, null), 
	STRING(false, false, false, "^(\"|\\')[\\p{Alnum}\\p{Blank}\\!\\#\\$\\%\\&\\(\\)\\*\\+\\,\\-\\.\\/:;\\<\\=\\>\\?\\@\\[\\\\\\]\\^_\\`\\{\\|\\}\\~]+(\"|\\')$", null, null),
	NULL(false, false, false, "^[nN][uU][lL][lL]$", new String[] {"null","NULL"}, "NULL"),
	PARAMETER( false, false, false, "(\\%|\\:)[a-zA-Z\\-_][0-9a-zA-Z\\-_]*$", null, null), LIKE(true, false, true, "^(L|l)(I|i)(K|k)(E|e)$", new String[] { " LIKE " }, " LIKE "),
	NOTLIKE(true, false,true,"^(N|n)(O|o)(T|t)_(L|l)(I|i)(K|k)(E|e)$",new String[] {" NOT LIKE "," NOT_LIKE " }," NOT LIKE ");

	private final Pattern pattern;
	private final String content;
	private final String matches[];
	private final boolean isOperator;
	private final boolean isCondition;
	private final boolean mustHaveSpaces;

	SymbolType(boolean isOperator, boolean isCondition, boolean mustHaveSpaces, String regex, String matches[],
			String content) {
		this.isOperator = isOperator;
		this.isCondition = isCondition;
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

	public boolean match(String symbol) {
		return pattern.matcher(symbol).matches();
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

	public static Symbol getSymbol(String symbol) {
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
	}

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
			regex = regex.substring(1, regex.length());
		else
			regex = "^" + likeContent;
		if (regex.endsWith("%"))
			regex = regex.substring(0, regex.length() - 1);
		else
			regex = regex + "$";

		regex = regex.replace("_", ".");

		return regex;
	}
}
