import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class LookupExpression extends ChainableExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): LookupExpression;
    lookupFn: string;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: LookupExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    fullyDefined(): boolean;
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getJSChainableHelper(operandJS: string): string;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
}
