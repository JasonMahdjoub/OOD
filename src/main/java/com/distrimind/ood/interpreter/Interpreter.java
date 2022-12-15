/*
Copyright or © or Corp. Jason Mahdjoub (01/04/2013)

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
import com.distrimind.ood.database.exceptions.ImpossibleQueryInterpretation;
import com.distrimind.util.Reference;

import java.util.ArrayList;
import java.util.List;

/**
 * Pseudo SQL interpreter
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
		List<RuleInstance> parts=getRuleInstances(whereCommand);
		if (parts.size() == 0)
			throw new ImpossibleQueryInterpretation(whereCommand);

		int level = Rule.getLowerPriorLevel();
		ml:
		while (level<Rule.values().length
				&& (parts.size() > 1 || (!parts.isEmpty() && (parts.get(0)).getRule() != Rule.QUERY))) {

			ArrayList<RuleInstance> p;
			while ((p= getNewQueryParts(whereCommand, parts, level))!=null) {
				parts = p;
				if (level!=Rule.getLowerPriorLevel()) {
					level = Rule.getLowerPriorLevel();
					continue ml;
				}
			}
			if (level==Rule.getHigherPriorLevel())
				level=Rule.values().length-1;
			else
				level++;
		}
		if (parts.isEmpty())
			throw new ImpossibleQueryInterpretation(whereCommand);

		RuleInstance res = parts.get(0);
		if (res.getRule() != Rule.QUERY) {
			throw new ImpossibleQueryInterpretation(whereCommand);
		}
		return res;
	}
	/*
	 * public static <T extends DatabaseRecord> boolean isConcernedBy(String
	 * whereCommand,Table<T> table, Map<String, Object> parameters, T record) throws
	 * DatabaseSyntaxException { ArrayList<QueryPart>
	 * qp=getRules(lexicalAnalyse(whereCommand)); RuleInstance
	 * query=getQuery(whereCommand, qp); return query.isConcernedBy(table,
	 * parameters, record); }
	 */

	private static List<RuleInstance> getRuleInstances(String command) throws DatabaseSyntaxException {
		ArrayList<RuleInstance> rules=new ArrayList<>();

		int index = 0;
		while (index < command.length()) {

			CharSequence cs = command.subSequence(index, command.length());
			int bestMatchLength = -1;
			SymbolType bestSymbol = null;
			for (SymbolType s : SymbolType.values()) {

				int p = s.match(cs);

				if (p > 0) {
					if (p>1) {
						char c=cs.charAt(p-1);
						if (c==')' || c=='(')
							--p;
					}
					if (bestMatchLength == -1 || bestMatchLength < p || bestSymbol==SymbolType.IDENTIFIER) {
						bestMatchLength = p;
						bestSymbol = s;
					}
				}
			}
			if (bestMatchLength==-1) {
				if (command.charAt(index)==' ')
				{
					++index;
					continue;
				}
				throw new DatabaseSyntaxException("Incomprehensible command part : " + cs);
			}

			Symbol symbol=new Symbol(bestSymbol, bestSymbol==SymbolType.PARAMETER?cs.subSequence(1, bestMatchLength).toString():cs.subSequence(0, bestMatchLength).toString());
			rules.add(symbol.getRule());
			index+=bestMatchLength;
		}
		return rules;
	}





	private static ArrayList<RuleInstance> getNewQueryParts(String command, List<RuleInstance> parts, int level)
			throws DatabaseSyntaxException {
		if (parts.size() == 0)
			throw new DatabaseSyntaxException("No rules");
		if (command == null)
			throw new NullPointerException("command");

		Reference<Integer> index = new Reference<>(0);
		ArrayList<RuleInstance> res = new ArrayList<>(parts.size());
		boolean changed = false;

		while (index.get() < parts.size()) {
			int previousIndex=index.get();

			RuleInstance ap = getQuery(parts, index.get(), index, level);

			if (ap == null) {
				ap=parts.get(index.get());
				index.set(index.get()+1);
			} else {
				if (ap.getParts().size()==1 && ap.getParts().get(0) instanceof RuleInstance && ap.getRule()==((RuleInstance) ap.getParts().get(0)).getRule())
					throw new IllegalAccessError(ap.getRule()+" ; "+ap.getParts().get(0));
				changed = true;
			}
			res.add(ap);
			if (index.get()==previousIndex && res.size()>0 && ap.equals(res.get(res.size()-1)))
				throw new DatabaseSyntaxException("Invalid syntax");
		}
		if (changed)
			return res;
		else {
			if (level==Rule.values().length-1)
				throw new ImpossibleQueryInterpretation(command+"\n"+toString(parts));
			else
				return null;
		}
	}
	static String toString(List<? extends QueryPart> parts)
	{
		int l=0;
		StringBuilder res=new StringBuilder("Syntax tree = \n\t");
		while (toString(parts, l, res))
		{
			res.append("\n\t");
			++l;
		}
		res.append("\n\t");
		toStringSymbols(parts, res);
		res.append("\n\t");
		toStringSymbolsContent(parts, res);
		res.append("\n\t");
		return res.toString();
	}
	private static void toStringSymbols(List<? extends QueryPart> parts, StringBuilder res)
	{
		for (QueryPart qp : parts)
		{
			if (qp instanceof Symbol) {
				res.append("<")
						.append(((Symbol) qp).getType().name())
						.append(">");
			}
			else if (qp instanceof RuleInstance)
			{
				RuleInstance ri=(RuleInstance) qp;
				toStringSymbols(ri.getParts(), res);
			}
		}
	}
	static void toStringSymbolsContent(List<? extends QueryPart> parts, StringBuilder res)
	{
		for (QueryPart qp : parts)
		{
			if (qp instanceof Symbol) {
				if (((Symbol) qp).getType()==SymbolType.PARAMETER)
					res.append("%");
				res.append(qp.getContent());
			}
			else if (qp instanceof RuleInstance)
			{
				RuleInstance ri=(RuleInstance) qp;
				toStringSymbolsContent(ri.getParts(), res);
			}
		}
	}
	private static boolean toString(List<? extends QueryPart> parts, int level, StringBuilder res)
	{
		boolean hasSubLevel=false;
		for (QueryPart qp : parts)
		{
			if (qp instanceof Symbol) {
				res.append("<")
						.append(((Symbol) qp).getType().name())
						.append(">");
			}
			else if (qp instanceof RuleInstance){

				RuleInstance ri=(RuleInstance) qp;
				boolean symbolRule=((ri.getParts().size()==1 && ri.getParts().get(0) instanceof Symbol));
				if (level==0 || symbolRule) {
					if (!symbolRule)
						hasSubLevel = true;
					res.append("<")
							.append(ri.getRule().name())
							.append(">");
				}
				else {
					hasSubLevel |= toString(ri.getParts(), level - 1, res);
				}
			}
		}
		return hasSubLevel;
	}


	private static RuleInstance getQuery(List<RuleInstance> parts, int index,
										 Reference<Integer> newIndex, int level) {

		Rule bestRule=null;
		int bestRulePartsLength=-1;
		for (Rule r : Rule.getNonSymbolRules()) {
			if (r.ordinal() > level)
				continue;

			for (Rule[] ruleComposition : r.getRulesComposition()) {
				if (parts.size() - index >= ruleComposition.length) {
					boolean found = true;
					for (int j = 0; j < ruleComposition.length; j++) {
						if (ruleComposition[j] != parts.get(index + j).getRule()) {
							found = false;
							break;
						}
					}
					if (found && (bestRule == null || bestRulePartsLength <= ruleComposition.length)) {
						/*if (bestRule!=null)
							return null;*/
						if (bestRulePartsLength == ruleComposition.length) {
							return null;
						}
						bestRule = r;
						bestRulePartsLength = ruleComposition.length;
					}
				}
			}
		}
		if (bestRule == null) {
			return null;
		}

		newIndex.set(index + bestRulePartsLength);
		return new RuleInstance(bestRule, parts, index, bestRulePartsLength);
	}

}
