import { Duration, Timezone } from 'chronoshift';
import { PlyType } from '../types';
import { SQLDialect } from './baseDialect';
export declare class DruidDialect extends SQLDialect {
    static TIME_BUCKETING: Record<string, string>;
    static TIME_PART_TO_FUNCTION: Record<string, string>;
    static CAST_TO_FUNCTION: Record<string, Record<string, string>>;
    constructor();
    nullConstant(): string;
    dateToSQLDateString(date: Date): string;
    constantGroupBy(): string;
    timeToSQL(date: Date): string;
    concatExpression(a: string, b: string): string;
    containsExpression(a: string, b: string): string;
    coalesceExpression(a: string, b: string): string;
    substrExpression(a: string, position: number, length: number): string;
    isNotDistinctFromExpression(a: string, b: string): string;
    castExpression(inputType: PlyType, operand: string, cast: string): string;
    timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string;
    timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string;
    timePartExpression(operand: string, part: string, timezone: Timezone): string;
    timeShiftExpression(operand: string, duration: Duration, timezone: Timezone): string;
    extractExpression(operand: string, regexp: string): string;
    indexOfExpression(str: string, substr: string): string;
}
