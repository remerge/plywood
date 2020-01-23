import { Duration, Timezone } from 'chronoshift';
import { PlywoodValue } from '../datatypes/index';
import { SQLDialect } from '../dialect/baseDialect';
import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
import { HasTimezone } from './mixins/hasTimezone';
export declare class TimeShiftExpression extends ChainableExpression implements HasTimezone {
    static DEFAULT_STEP: number;
    static op: string;
    static fromJS(parameters: ExpressionJS): TimeShiftExpression;
    duration: Duration;
    step: number;
    timezone: Timezone;
    constructor(parameters: ExpressionValue);
    valueOf(): ExpressionValue;
    toJS(): ExpressionJS;
    equals(other: TimeShiftExpression | undefined): boolean;
    protected _toStringParameters(indent?: int): string[];
    protected _calcChainableHelper(operandValue: any): PlywoodValue;
    protected _getJSChainableHelper(operandJS: string): string;
    protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string;
    protected changeStep(step: int): Expression;
    specialSimplify(): Expression;
    getTimezone: () => Timezone;
    changeTimezone: (timezone: Timezone) => TimeShiftExpression;
}
