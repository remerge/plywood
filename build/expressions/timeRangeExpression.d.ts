import { Duration, Timezone } from 'chronoshift';
import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, ExpressionJS, ExpressionValue } from './baseExpression';
import { HasTimezone } from './mixins/hasTimezone';
export declare class TimeRangeExpression extends ChainableExpression implements HasTimezone {
    static DEFAULT_STEP: number;
    static op: string;
    static fromJS(parameters: ExpressionJS): TimeRangeExpression;
    duration: Duration;
    step: number;
    timezone: Timezone;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: TimeRangeExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    getQualifiedDurationDescription(capitalize?: boolean): string;
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getJSChainableHelper(operandJS: string): string;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
    getTimezone: () => Timezone;
    changeTimezone: (timezone: Timezone) => TimeRangeExpression;
}
