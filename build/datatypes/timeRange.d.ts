import { Duration, Timezone } from 'chronoshift';
import { Instance } from 'immutable-class';
import { NumberRange } from './numberRange';
import { Range } from './range';
export interface TimeRangeValue {
    start: Date;
    end: Date;
    bounds?: string;
}
export interface TimeRangeJS {
    start: Date | string;
    end: Date | string;
    bounds?: string;
}
export declare class TimeRange extends Range<Date> implements Instance<TimeRangeValue, TimeRangeJS> {
    static type: string;
    static isTimeRange(candidate: any): candidate is TimeRange;
    static intervalFromDate(date: Date): string;
    static timeBucket(date: Date, duration: Duration, timezone: Timezone): TimeRange;
    static fromTime(t: Date): TimeRange;
    static fromJS(parameters: TimeRangeJS): TimeRange;
    constructor(parameters: TimeRangeValue);
    protected _zeroEndpoint(): Date;
    protected _endpointEqual(a: Date, b: Date): boolean;
    protected _endpointToString(a: Date, tz?: Timezone): string;
    valueOf(): TimeRangeValue;
    toJS(): TimeRangeJS;
    equals(other: TimeRange | undefined): boolean;
    toInterval(): string;
    midpoint(): Date;
    changeToNumber(): NumberRange;
    isAligned(duration: Duration, timezone: Timezone): boolean;
    rebaseOnStart(newStart: Date): TimeRange;
}
