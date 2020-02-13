import { Timezone } from 'chronoshift';
import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, ExpressionJS, ExpressionValue } from './baseExpression';
import { HasTimezone } from './mixins/hasTimezone';
export declare class TimePartExpression extends ChainableExpression implements HasTimezone {
    static op: string;
    static fromJS(parameters: ExpressionJS): TimePartExpression;
    static PART_TO_FUNCTION: Record<string, (d: any) => number>;
    static PART_TO_MAX_VALUES: Record<string, number>;
    part: string;
    timezone: Timezone;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: TimePartExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getJSChainableHelper(operandJS: string): string;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
    maxPossibleSplitValues(): number;
    getTimezone: () => Timezone;
    changeTimezone: (timezone: Timezone) => TimePartExpression;
}
