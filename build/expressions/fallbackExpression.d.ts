import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class FallbackExpression extends ChainableUnaryExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): FallbackExpression;
    constructor(parameters: ExpressionValue);
    protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue;
    protected _getJSChainableUnaryHelper(operandJS: string, expressionJS: string): string;
    protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string;
    specialSimplify(): Expression;
}
