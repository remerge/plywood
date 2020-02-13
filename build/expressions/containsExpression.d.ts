import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class ContainsExpression extends ChainableUnaryExpression {
    static NORMAL: string;
    static IGNORE_CASE: string;
    static caseIndependent(str: string): boolean;
    static op: string;
    static fromJS(parameters: ExpressionJS): ContainsExpression;
    compare: string;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: ContainsExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue;
    protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string;
    protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string;
    changeCompare(compare: string): ContainsExpression;
    specialSimplify(): Expression;
}
