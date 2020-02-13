import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class SubstrExpression extends ChainableExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): SubstrExpression;
    position: int;
    len: int;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: SubstrExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getJSChainableHelper(operandJS: string): string;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
    specialSimplify(): Expression;
}
