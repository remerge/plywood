import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class LimitExpression extends ChainableExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): LimitExpression;
    value: int;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: LimitExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
    protected specialSimplify(): Expression;
}
