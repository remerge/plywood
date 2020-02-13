import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class CustomAggregateExpression extends ChainableExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): CustomAggregateExpression;
    custom: string;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: CustomAggregateExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
}
