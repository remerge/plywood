import { Timezone } from 'chronoshift';
import { Environment } from '../../types';
import { Expression, ExpressionValue } from '../baseExpression';
export declare class HasTimezone {
    valueOf: () => ExpressionValue;
    timezone: Timezone;
    getTimezone(): Timezone;
    changeTimezone(timezone: Timezone): Expression;
    needsEnvironment(): boolean;
    defineEnvironment(environment: Environment): Expression;
}
