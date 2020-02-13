import { Datum, PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, ExpressionJS, ExpressionValue } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
export declare class CountExpression extends ChainableExpression implements Aggregate {
    static op: string;
    static fromJS(parameters: ExpressionJS): CountExpression;
    constructor(parameters: ExpressionValue);
    calc(datum: Datum): PlywoodValue;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
}
