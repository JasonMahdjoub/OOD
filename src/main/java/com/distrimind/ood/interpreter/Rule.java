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

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Type of rule
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public enum Rule {

    OPEN_PARENTHESIS(true, "^(<" + SymbolType.OPEN_PARENTHESIS.name()+">)$"),
    CLOSE_PARENTHESIS(true, "^(<" + SymbolType.CLOSE_PARENTHESIS.name()+">)$"),
    NULL_WORD(true, "^(<" + SymbolType.NULL.name()+">)$"),
    WORD(true, "^(<" + SymbolType.IDENTIFIER.name() + ">|<" + SymbolType.NUMBER.name() + ">|<" + SymbolType.STRING.name() + ">|<" + SymbolType.PARAMETER.name() + ">)$"),
    MULTIPLY_OPERATOR(true, "^(<"+SymbolType.MULTIPLY.name()+"<|>"+SymbolType.DIVIDE.name()+"<|>"+SymbolType.MODULO+">)$"),
    ADD_OPERATOR(true, "^(<"+SymbolType.PLUS.name()+"<|>"+SymbolType.MINUS.name()+">)$"),
    OP_COMP(true, "^(<" + SymbolType.EQUAL_COMPARATOR.name() + ">|<"
            + SymbolType.NOT_EQUAL_COMPARATOR.name() + ">|<" + SymbolType.LOWER_COMPARATOR.name() + ">|<"
            + SymbolType.LOWER_OR_EQUAL_COMPARATOR.name() + ">|<" + SymbolType.GREATER_COMPARATOR.name() + ">|<"
            + SymbolType.GREATER_OR_EQUAL_COMPARATOR.name() + ">|<" + SymbolType.LIKE.name() + ">|<"
            + SymbolType.NOT_LIKE.name() + ">)$"),
    IS_OP(true, "^(<" + SymbolType.IS.name() + ">|<" + SymbolType.IS_NOT.name() + ">)$"),
    IN_OP(true, "^(<" + SymbolType.IN.name() + ">|<"+ SymbolType.NOT_IN.name() + ">)$"),
    OP_CONDITION(true, "^(<" + SymbolType.AND_CONDITION.name() + ">|<"+ SymbolType.OR_CONDITION.name() + ">)$"),

    WORD_RULE(true, "^(<"+ WORD.name()+">|<"+ ">|<SIGNED_VALUE>|<"+OPEN_PARENTHESIS.name()+"><EXPRESSION><"+CLOSE_PARENTHESIS.name()+">)$"),
    NULL_TEST(false, "^(<" + WORD_RULE.name() + "><" + IS_OP.name() + "><" + NULL_WORD.name() + ">)$"),
    FACTOR(false, "^(<"+ WORD_RULE.name()+">|(<EXPRESSION>|<FACTOR>)<"+MULTIPLY_OPERATOR.name()+">(<FACTOR>|<EXPRESSION>))$"),
    EXPRESSION(false, "^(<FACTOR>|(<FACTOR>|<EXPRESSION>)<"+ADD_OPERATOR.name()+">(<EXPRESSION>|<"+FACTOR.name()+">))$"),
    SIGNED_VALUE(true, "^(<"+ADD_OPERATOR.name()+">(<"+EXPRESSION.name()+">|<"+FACTOR.name()+">|<"+ WORD_RULE.name()+">))$"),

    COMPARE(false, "^(<" + EXPRESSION.name() + "><" + OP_COMP.name() + "><" + EXPRESSION.name()+ ">|<"+OPEN_PARENTHESIS.name()+"><QUERY><"+CLOSE_PARENTHESIS.name()+">)$"),

    IN_TEST(false, "^(<"+EXPRESSION.name()+"><" + IN_OP.name() + ">(<"+ WORD_RULE.name()+">|<"+EXPRESSION.name()+">))$"),

    QUERY(false, "^(<" + COMPARE.name() + ">|<"+ NULL_TEST.name()+">|<"+ IN_TEST.name()+">|<QUERY><"+ OP_CONDITION.name() + "><QUERY>)$");

    private final boolean isAtomic;
    private final Pattern pattern;

    private Rule[][] rulesComposition=null;
    private static final Rule[] nonSymbolRules;

    static
    {
        nonSymbolRules= Arrays.copyOfRange(Rule.values(), Rule.WORD_RULE.ordinal(), Rule.values().length);

        Rule.WORD_RULE.rulesComposition=new Rule[][]{
                {WORD},
                {SIGNED_VALUE},
                {OPEN_PARENTHESIS, EXPRESSION, CLOSE_PARENTHESIS},
        };
        Rule.NULL_TEST.rulesComposition=new Rule[][]{{WORD_RULE, IS_OP, NULL_WORD}};
        Rule.FACTOR.rulesComposition=new Rule[][]{
                {WORD_RULE},
                {EXPRESSION, MULTIPLY_OPERATOR, EXPRESSION},
                {FACTOR, MULTIPLY_OPERATOR, FACTOR},
                {EXPRESSION, MULTIPLY_OPERATOR, FACTOR},
                {FACTOR, MULTIPLY_OPERATOR, EXPRESSION},
        };
        Rule.EXPRESSION.rulesComposition=new Rule[][]{
                {FACTOR},
                {EXPRESSION, ADD_OPERATOR, EXPRESSION},
                {FACTOR, ADD_OPERATOR, FACTOR},
                {EXPRESSION, ADD_OPERATOR, FACTOR},
                {FACTOR, ADD_OPERATOR, EXPRESSION},
        };
        Rule.SIGNED_VALUE.rulesComposition=new Rule[][]{
                {ADD_OPERATOR, EXPRESSION},
                {ADD_OPERATOR, FACTOR},
                {ADD_OPERATOR, WORD_RULE}
        };
        Rule.COMPARE.rulesComposition=new Rule[][]{
                {EXPRESSION, OP_COMP, EXPRESSION},
                {OPEN_PARENTHESIS, QUERY, CLOSE_PARENTHESIS},
                {WORD_RULE, WORD_RULE}
        };
        Rule.IN_TEST.rulesComposition=new Rule[][]{
                {EXPRESSION, IN_OP, WORD_RULE},
                {EXPRESSION, IN_OP, EXPRESSION},
        };
        Rule.QUERY.rulesComposition=new Rule[][]{
                {COMPARE},
                {NULL_TEST},
                {IN_TEST},
                {QUERY, OP_CONDITION, QUERY},
        };

    }

    static Rule[] getNonSymbolRules()
    {
        return nonSymbolRules;
    }

    public Rule[][] getRulesComposition() {
        return rulesComposition;
    }

    Rule(boolean isAtomic, String regex) {
        if (regex == null)
            throw new NullPointerException("regex");
        this.isAtomic=isAtomic;
        this.pattern = Pattern.compile(regex);
    }

    public boolean match(String backusNaurRule) {
        return pattern.matcher(backusNaurRule).matches();
    }

    public boolean isAtomic() {
        return isAtomic;
    }
    static int getLowerPriorLevel()
    {
        return FACTOR.ordinal();
    }
    static int getHigherPriorLevel()
    {
        return SIGNED_VALUE.ordinal();
    }
}
