import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableUnaryExpression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class IndexOfExpression extends ChainableUnaryExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): IndexOfExpression;
    constructor(parameters: ExpressionValue);
    protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue;
    protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string;
    protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string;
}
