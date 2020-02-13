import { Timezone } from 'chronoshift';
import { PlyType } from '../types';
export declare type PlywoodRange = Range<number | Date | string>;
export interface PlywoodRangeJS {
    start: null | number | Date | string;
    end: null | number | Date | string;
    bounds?: string;
}
export declare abstract class Range<T> {
    static DEFAULT_BOUNDS: string;
    static isRange(candidate: any): candidate is PlywoodRange;
    static isRangeType(type: PlyType): boolean;
    static unwrapRangeType(type: PlyType): PlyType | null;
    static classMap: Record<string, typeof Range>;
    static register(ctr: any): void;
    static fromJS(parameters: PlywoodRangeJS): PlywoodRange;
    start: T;
    end: T;
    bounds: string;
    constructor(start: T, end: T, bounds: string);
    protected _zeroEndpoint(): T;
    protected _endpointEqual(a: T, b: T): boolean;
    protected _endpointToString(a: T, tz?: Timezone): string;
    protected _equalsHelper(other: Range<T>): boolean;
    abstract equals(other: Range<T>): boolean;
    abstract toJS(): PlywoodRangeJS;
    toJSON(): any;
    toString(tz?: Timezone): string;
    compare(other: Range<T>): number;
    openStart(): boolean;
    openEnd(): boolean;
    empty(): boolean;
    degenerate(): boolean;
    contains(val: T | Range<T>): boolean;
    protected validMemberType(val: any): boolean;
    containsValue(val: T): boolean;
    intersects(other: Range<T>): boolean;
    adjacent(other: Range<T>): boolean;
    mergeable(other: Range<T>): boolean;
    union(other: Range<T>): Range<T>;
    extent(): Range<T>;
    extend(other: Range<T>): Range<T>;
    intersect(other: Range<T>): Range<T> | null;
    abstract midpoint(): T;
    isFinite(): boolean;
}
