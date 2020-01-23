import { Duration, Timezone } from 'chronoshift';
import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
import { HasTimezone } from './mixins/hasTimezone';
import { TimeBucketExpression } from './timeBucketExpression';
export declare class TimeFloorExpression extends ChainableExpression implements HasTimezone {
    static op: string;
    static fromJS(parameters: ExpressionJS): TimeFloorExpression;
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
    alignsWith(ex: Expression): boolean;
    specialSimplify(): Expression;
    getTimezone: () => Timezone;
    changeTimezone: (timezone: Timezone) => TimeFloorExpression;
}
