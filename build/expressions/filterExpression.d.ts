import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableUnaryExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class FilterExpression extends ChainableUnaryExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): FilterExpression;
    constructor(parameters: ExpressionValue);
    protected _calcChainableUnaryHelper(operandValue: any, expressionValue: any): PlywoodValue;
    protected _getSQLChainableUnaryHelper(dialect: SQLDialect, operandSQL: string, expressionSQL: string): string;
    isNester(): boolean;
    fullyDefined(): boolean;
    specialSimplify(): Expression;
}
