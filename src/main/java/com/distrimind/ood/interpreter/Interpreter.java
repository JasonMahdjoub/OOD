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

import com.distrimind.ood.database.exceptions.DatabaseSyntaxException;
import com.distrimind.ood.database.exceptions.QueryInterpretationImpossible;
import com.distrimind.ood.database.exceptions.UnrecognizedSymbolException;
import com.distrimind.util.Reference;

import java.util.ArrayList;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public class Interpreter {
	/*
	 * public static <T extends DatabaseRecord> String execute(String
	 * whereCommand,Table<T> table, Map<String, Object> parameters, Map<Integer,
	 * Object> outputParameters) throws DatabaseSyntaxException { return
	 * execute(whereCommand, table, parameters, outputParameters, 1); } public
	 * static <T extends DatabaseRecord> String execute(String whereCommand,Table<T>
	 * table, Map<String, Object> parameters, Map<Integer, Object> outputParameters,
	 * int firstParameterIndex) throws DatabaseSyntaxException {
	 * ArrayList<QueryPart> qp=getRules(lexicalAnalyse(whereCommand)); RuleInstance
	 * query=getQuery(whereCommand, qp); return query.translateToSqlQuery(table,
	 * parameters, outputParameters, new
	 * AtomicInteger(firstParameterIndex)).toString(); }
	 */

	public static RuleInstance getRuleInstance(String whereCommand) throws DatabaseSyntaxException {
		if (whereCommand == null)
			throw new NullPointerException("whereCommand");
		ArrayList<QueryPart> qp = getRules(lexicalAnalyse(whereCommand));
		return getQuery(whereCommand, qp);
	}
	/*
	 * public static <T extends DatabaseRecord> boolean isConcernedBy(String
	 * whereCommand,Table<T> table, Map<String, Object> parameters, T record) throws
	 * DatabaseSyntaxException { ArrayList<QueryPart>
	 * qp=getRules(lexicalAnalyse(whereCommand)); RuleInstance
	 * query=getQuery(whereCommand, qp); return query.isConcernedBy(table,
	 * parameters, record); }
	 */

	private static String preProcess(String command) {
		command = command.replaceAll("\\p{Blank}([Nn])([Oo])([Tt])\\p{Blank}([Ll])([Ii])([Kk])([Ee])\\p{Blank}", " NOT_LIKE ");
		command = command.replaceAll("\\p{Blank}([Ii])([Ss])\\p{Blank}([Nn])([Oo])([Tt])\\p{Blank}", " IS_NOT ");

		StringBuilder sb = new StringBuilder();

		int index = 0;
		while (index < command.length()) {

			String bestMatch = null;
			SymbolType bestSymbol = null;
			for (SymbolType s : SymbolType.values()) {
				if (!s.isOperator())
					continue;

				for (String symbol : s.getMatches()) {
					boolean ok = true;

					for (int i = 0; i < symbol.length(); i++) {
						if (Character.toUpperCase(command.charAt(index + i)) != Character
								.toUpperCase(symbol.charAt(i))) {
							ok = false;
							break;
						}
					}
					if (ok) {
						if (bestMatch == null || bestMatch.length() < symbol.length()) {
							bestMatch = symbol;
							bestSymbol = s;
						}

					}
				}
			}
			if (bestMatch != null) {
				boolean space = bestSymbol != SymbolType.LIKE && bestSymbol != SymbolType.NOTLIKE
						&& bestSymbol!=SymbolType.IS && bestSymbol!=SymbolType.ISNOT
						&& bestSymbol!=SymbolType.IN && bestSymbol!=SymbolType.NOTIN;
				if (space)
					sb.append(" ");
				sb.append(bestMatch);
				if (space)
					sb.append(" ");
				index += bestMatch.length();
			} else {
				sb.append(command.charAt(index));
				index++;
			}
		}
		return sb.toString();
	}

	private static ArrayList<Symbol> lexicalAnalyse(String command) throws UnrecognizedSymbolException {
		command = preProcess(command);
		String[] sls = command.split(" ");
		ArrayList<Symbol> symbols = new ArrayList<>(sls.length);
		for (String s : sls) {
			String st = s.trim();
			if (st.isEmpty())
				continue;
			Symbol symbol = SymbolType.getSymbol(st);
			if (symbol == null)
				throw new UnrecognizedSymbolException(st);

			symbols.add(symbol);

		}
		return symbols;
	}

	private static ArrayList<QueryPart> getRules(ArrayList<Symbol> symbols) {
		ArrayList<QueryPart> res = new ArrayList<>(symbols.size());
		for (Symbol s : symbols) {
			RuleInstance r = s.getRule();

			if (r != null) {
				res.add(r);
			} else
				res.add(s);
		}
		return res;
	}

	private static RuleInstance getQuery(String command, ArrayList<QueryPart> parts) throws DatabaseSyntaxException {
		if (parts.size() == 0)
			throw new QueryInterpretationImpossible(command);

		while (parts.size() > 1 || (!parts.isEmpty() && (!(parts.get(0) instanceof RuleInstance)
				|| ((RuleInstance) parts.get(0)).getRule() != Rule.QUERY))) {
			parts = getNewQueryParts(command, parts);
		}
		if (parts.isEmpty())
			throw new QueryInterpretationImpossible(command);

		QueryPart ri = parts.get(0);
		if (!(ri instanceof RuleInstance))
			throw new QueryInterpretationImpossible(command);
		RuleInstance res = (RuleInstance) ri;
		if (res.getRule() != Rule.QUERY) {
			throw new QueryInterpretationImpossible(command);
		}
		return res;
	}

	private static ArrayList<QueryPart> getNewQueryParts(String command, ArrayList<QueryPart> parts)
			throws DatabaseSyntaxException {
		if (parts.size() == 0)
			throw new DatabaseSyntaxException("No rules");
		if (command == null)
			throw new NullPointerException("command");

		Reference<Integer> index = new Reference<>(0);
		ArrayList<QueryPart> res = new ArrayList<>(parts.size());
		boolean changed = false;

		while (index.get() < parts.size()) {
			int previousIndex=index.get();

			QueryPart ap = getQuery(parts, index.get(), index);

			if (ap == null) {
				ap=parts.get(index.get());
				index.set(index.get()+1);
			} else {
				changed = true;
			}
			res.add(ap);
			if (index.get()==previousIndex && res.size()>0 && ap.equals(res.get(res.size()-1)))
				throw new DatabaseSyntaxException("Invalid syntax");
		}
		if (changed)
			return res;
		else {
			throw new QueryInterpretationImpossible(command);

		}
	}

	private static RuleInstance getQuery(ArrayList<QueryPart> parts, int index,
										 Reference<Integer> newIndex) {

		Rule chosenValidRule = null;

		StringBuilder currentRule = new StringBuilder();
		int len = 0;

		for (int i = index; i < parts.size(); i++) {
			boolean valid = true;

			Rule chosenRule = null;
			currentRule.append(parts.get(i).getBackusNaurNotation());

			String rc = currentRule.toString();
			for (Rule r : Rule.values()) {
				if (r.match(rc)) {
					if (chosenRule != null) {
						valid = false;
						break;
					}
					chosenRule = r;
				}
			}
			if (valid && chosenRule != null) {
				chosenValidRule = chosenRule;
				len = i - index + 1;
			}
		}
		if (chosenValidRule == null) {
			return null;
		}

		newIndex.set(index + len);
		return new RuleInstance(chosenValidRule, parts, index, len);
	}

}
