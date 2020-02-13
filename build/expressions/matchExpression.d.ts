import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class MatchExpression extends ChainableExpression {
    static likeToRegExp(like: string, escapeChar?: string): string;
    static op: string;
    static fromJS(parameters: ExpressionJS): MatchExpression;
    regexp: string;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: MatchExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getJSChainableHelper(operandJS: string): string;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
}
