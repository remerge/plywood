import { Duration, Timezone } from 'chronoshift';
import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, ExpressionJS, ExpressionValue } from './baseExpression';
export declare class TimeBucketExpression extends ChainableExpression {
    static op: string;
    static fromJS(parameters: ExpressionJS): TimeBucketExpression;
    duration: Duration;
    timezone: Timezone;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: TimeBucketExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getJSChainableHelper(operandJS: string): string;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
    getTimezone: () => Timezone;
    changeTimezone: (timezone: Timezone) => this;
}
