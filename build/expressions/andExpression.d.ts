import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionMatchFn, ExpressionValue, ExtractAndRest } from './baseExpression';
export declare class AndExpression extends ChainableUnaryExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): AndExpression;
    static merge(ex1: Expression, ex2: Expression): Expression | null;
    constructor(parameters: ExpressionValue);
    protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue;
    protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string;
    protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string;
    isCommutative(): boolean;
    isAssociative(): boolean;
    protected specialSimplify(): Expression;
    extractFromAnd(matchFn: ExpressionMatchFn): ExtractAndRest;
}
