import { Timezone } from 'chronoshift';
import { Instance } from 'immutable-class';
import { Direction, Expression, ExpressionExternalAlteration } from '../expressions/index';
import { External } from '../external/baseExternal';
import { DatasetFullType, PlyType } from '../types';
import { AttributeInfo, AttributeJSs, Attributes } from './attributeInfo';
import { NumberRange } from './numberRange';
import { Set } from './set';
import { StringRange } from './stringRange';
import { TimeRange } from './timeRange';
export interface ComputeFn {
    (d: Datum): any;
}
export interface DirectionFn {
    (a: any, b: any): number;
}
export declare type PlywoodValue = null | boolean | number | string | Date | NumberRange | TimeRange | StringRange | Set | Dataset | External;
export interface PseudoDatum {
    [attribute: string]: any;
}
export interface Datum {
    [attribute: string]: PlywoodValue | Expression;
}
export interface DatasetExternalAlteration {
    index: number;
    key: string;
    external?: External;
    terminal?: boolean;
    result?: any;
    datasetAlterations?: DatasetExternalAlterations;
    expressionAlterations?: ExpressionExternalAlteration;
}
export declare type DatasetExternalAlterations = DatasetExternalAlteration[];
export interface AlterationFiller {
    (external: External, terminal: boolean): any;
}
export declare function fillExpressionExternalAlteration(alteration: ExpressionExternalAlteration, filler: AlterationFiller): void;
export declare function sizeOfExpressionExternalAlteration(alteration: ExpressionExternalAlteration): number;
export declare function fillDatasetExternalAlterations(alterations: DatasetExternalAlterations, filler: AlterationFiller): void;
export declare function sizeOfDatasetExternalAlterations(alterations: DatasetExternalAlterations): number;
export interface Formatter extends Record<string, Function | undefined> {
    'NULL'?: (v: any) => string;
    'TIME'?: (v: Date, tz: Timezone) => string;
    'TIME_RANGE'?: (v: TimeRange, tz: Timezone) => string;
    'SET/TIME'?: (v: Set, tz: Timezone) => string;
    'SET/TIME_RANGE'?: (v: Set, tz: Timezone) => string;
    'STRING'?: (v: string) => string;
    'SET/STRING'?: (v: Set) => string;
    'BOOLEAN'?: (v: boolean) => string;
    'NUMBER'?: (v: number) => string;
    'NUMBER_RANGE'?: (v: NumberRange) => string;
    'SET/NUMBER'?: (v: Set) => string;
    'SET/NUMBER_RANGE'?: (v: Set) => string;
    'DATASET'?: (v: Dataset) => string;
}
export interface Finalizer {
    (v: string): string;
}
export interface FlattenOptions {
    prefixColumns?: boolean;
    order?: 'preorder' | 'inline' | 'postorder';
    nestingName?: string;
    columnOrdering?: 'as-seen' | 'keys-first';
}
export declare type FinalLineBreak = 'include' | 'suppress';
export interface TabulatorOptions extends FlattenOptions {
    separator?: string;
    lineBreak?: string;
    finalLineBreak?: FinalLineBreak;
    formatter?: Formatter;
    finalizer?: Finalizer;
    timezone?: Timezone;
    attributeTitle?: (attribute: AttributeInfo) => string;
    attributeFilter?: (attribute: AttributeInfo) => boolean;
}
export interface DatasetValue {
    attributes?: Attributes;
    keys?: string[];
    data: Datum[];
    suppress?: boolean;
}
export interface DatasetJSFull {
    attributes?: AttributeJSs;
    keys?: string[];
    data?: Datum[];
}
export declare type DatasetJS = DatasetJSFull | Datum[];
export declare class Dataset implements Instance<DatasetValue, DatasetJS> {
    static type: string;
    static DEFAULT_FORMATTER: Formatter;
    static CSV_FINALIZER: Finalizer;
    static TSV_FINALIZER: Finalizer;
    static datumToLine(datum: Datum, attributes: Attributes, timezone: Timezone, formatter: Formatter, finalizer: Finalizer, separator: string): string;
    static isDataset(candidate: any): candidate is Dataset;
    static datumFromJS(js: PseudoDatum, attributeLookup?: Record<string, AttributeInfo>): Datum;
    static datumToJS(datum: Datum): PseudoDatum;
    static getAttributesFromData(data: Datum[]): Attributes;
    static parseJSON(text: string): any[];
    static fromJS(parameters: DatasetJS | any[]): Dataset;
    suppress: boolean;
    attributes: Attributes;
    keys: string[];
    data: Datum[];
    constructor(parameters: DatasetValue);
    valueOf(): DatasetValue;
    toJS(): DatasetJS;
    toString(): string;
    toJSON(): any;
    equals(other: Dataset | undefined): boolean;
    hide(): Dataset;
    changeData(data: Datum[]): Dataset;
    basis(): boolean;
    hasExternal(): boolean;
    getFullType(): DatasetFullType;
    select(attrs: string[]): Dataset;
    apply(name: string, ex: Expression): Dataset;
    applyFn(name: string, exFn: ComputeFn, type: PlyType): Dataset;
    filter(ex: Expression): Dataset;
    filterFn(exFn: ComputeFn): Dataset;
    sort(ex: Expression, direction: Direction): Dataset;
    sortFn(exFn: ComputeFn, direction: Direction): Dataset;
    limit(limit: number): Dataset;
    count(): int;
    sum(ex: Expression): number;
    sumFn(exFn: ComputeFn): number;
    average(ex: Expression): number;
    averageFn(exFn: ComputeFn): number;
    min(ex: Expression): number;
    minFn(exFn: ComputeFn): number;
    max(ex: Expression): number;
    maxFn(exFn: ComputeFn): number;
    countDistinct(ex: Expression): number;
    countDistinctFn(exFn: ComputeFn): number;
    quantile(ex: Expression, quantile: number): number;
    quantileFn(exFn: ComputeFn, quantile: number): number;
    collect(ex: Expression): Set;
    collectFn(exFn: ComputeFn): Set;
    split(splits: Record<string, Expression>, datasetName: string): Dataset;
    splitFn(splitFns: Record<string, ComputeFn>, datasetName: string): Dataset;
    getReadyExternals(limit?: number): DatasetExternalAlterations;
    applyReadyExternals(alterations: DatasetExternalAlterations): Dataset;
    getKeyLookup(): Record<string, Datum>;
    join(other: Dataset): Dataset;
    leftJoin(other: Dataset): Dataset;
    fullJoin(other: Dataset, compare: (v1: any, v2: any) => number): Dataset;
    findDatumByAttribute(attribute: string, value: any): Datum | undefined;
    getColumns(options?: FlattenOptions): AttributeInfo[];
    private _flattenHelper;
    flatten(options?: FlattenOptions): Dataset;
    toTabular(tabulatorOptions: TabulatorOptions): string;
    toCSV(tabulatorOptions?: TabulatorOptions): string;
    toTSV(tabulatorOptions?: TabulatorOptions): string;
    rows(): number;
    depthFirstTrimTo(n: number): Dataset;
}
