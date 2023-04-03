import { Duration, Timezone } from 'chronoshift';
import { PlyType } from '../types';
import { SQLDialect } from './baseDialect';
export declare class DruidDialect extends SQLDialect {
    static TIME_PART_TO_FUNCTION: Record<string, string>;
    static CAST_TO_FUNCTION: Record<string, Record<string, string>>;
    constructor();
    dateToSQLDateString(date: Date): string;
    floatDivision(numerator: string, denominator: string): string;
    constantGroupBy(): string;
    timeToSQL(date: Date): string;
    concatExpression(a: string, b: string): string;
    containsExpression(a: string, b: string): string;
    substrExpression(a: string, position: number, length: number): string;
    isNotDistinctFromExpression(a: string, b: string): string;
    castExpression(inputType: PlyType, operand: string, cast: string): string;
    private operandAsTimestamp;
    timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string;
    timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string;
    timePartExpression(operand: string, part: string, timezone: Timezone): string;
    timeShiftExpression(operand: string, duration: Duration, step: int, timezone: Timezone): string;
    extractExpression(operand: string, regexp: string): string;
    regexpExpression(expression: string, regexp: string): string;
    indexOfExpression(str: string, substr: string): string;
    logExpression(base: string, operand: string): string;
}
