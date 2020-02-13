import { Duration, Timezone } from 'chronoshift';
import { PlywoodRequester } from 'plywood-base-api';
import { ReadableStream, Transform } from 'readable-stream';
import { AttributeInfo, AttributeJSs, Attributes, Datum, PlywoodValue } from '../datatypes/index';
import { ExpressionJS, ComputeContext } from '../expressions/baseExpression';
import { ApplyExpression, ChainableUnaryExpression, Expression, FilterExpression, LimitExpression, SelectExpression, SortExpression, SplitExpression } from '../expressions/index';
import { DatasetFullType, PlyType, PlyTypeSimple } from '../types';
import { CustomDruidAggregations, CustomDruidTransforms } from './utils/druidTypes';
export declare class TotalContainer {
    datum: Datum;
    constructor(d: Datum);
    toJS(): any;
}
export interface NextFn<Q> {
    (prevQuery: Q, prevResultLength: number, prevMeta: any): Q;
}
export interface QueryAndPostTransform<T> {
    query: T;
    context?: Record<string, any>;
    postTransform: Transform;
    next?: NextFn<T>;
}
export interface Inflater {
    (d: Datum): void;
}
export declare type QuerySelection = "any" | "no-top-n" | "group-by-only";
export declare type IntrospectionDepth = "deep" | "default" | "shallow";
export interface IntrospectOptions {
    depth?: IntrospectionDepth;
    deep?: boolean;
}
export declare type QueryMode = "raw" | "value" | "total" | "split";
export interface ExternalValue {
    engine?: string;
    version?: string;
    suppress?: boolean;
    source?: string | string[];
    rollup?: boolean;
    attributes?: Attributes;
    attributeOverrides?: Attributes;
    derivedAttributes?: Record<string, Expression>;
    delegates?: External[];
    concealBuckets?: boolean;
    mode?: QueryMode;
    dataName?: string;
    rawAttributes?: Attributes;
    filter?: Expression;
    valueExpression?: Expression;
    select?: SelectExpression;
    split?: SplitExpression;
    applies?: ApplyExpression[];
    sort?: SortExpression;
    limit?: LimitExpression;
    havingFilter?: Expression;
    timeAttribute?: string;
    customAggregations?: CustomDruidAggregations;
    customTransforms?: CustomDruidTransforms;
    allowEternity?: boolean;
    allowSelectQueries?: boolean;
    introspectionStrategy?: string;
    exactResultsOnly?: boolean;
    querySelection?: QuerySelection;
    context?: Record<string, any>;
    requester?: PlywoodRequester<any>;
}
export interface ExternalJS {
    engine: string;
    version?: string;
    source?: string | string[];
    rollup?: boolean;
    attributes?: AttributeJSs;
    attributeOverrides?: AttributeJSs;
    derivedAttributes?: Record<string, ExpressionJS>;
    filter?: ExpressionJS;
    rawAttributes?: AttributeJSs;
    concealBuckets?: boolean;
    timeAttribute?: string;
    customAggregations?: CustomDruidAggregations;
    customTransforms?: CustomDruidTransforms;
    allowEternity?: boolean;
    allowSelectQueries?: boolean;
    introspectionStrategy?: string;
    exactResultsOnly?: boolean;
    querySelection?: QuerySelection;
    context?: Record<string, any>;
}
export interface ApplySegregation {
    aggregateApplies: ApplyExpression[];
    postAggregateApplies: ApplyExpression[];
}
export interface AttributesAndApplies {
    attributes?: Attributes;
    applies?: ApplyExpression[];
}
export declare abstract class External {
    static type: string;
    static SEGMENT_NAME: string;
    static VALUE_NAME: string;
    static isExternal(candidate: any): candidate is External;
    static extractVersion(v: string): string;
    static versionLessThan(va: string, vb: string): boolean;
    static deduplicateExternals(externals: External[]): External[];
    static addExtraFilter(ex: Expression, extraFilter: Expression): Expression;
    static makeZeroDatum(applies: ApplyExpression[]): Datum;
    static normalizeAndAddApply(attributesAndApplies: AttributesAndApplies, apply: ApplyExpression): AttributesAndApplies;
    static segregationAggregateApplies(applies: ApplyExpression[]): ApplySegregation;
    static getCommonFilterFromExternals(externals: External[]): Expression;
    static getMergedDerivedAttributesFromExternals(externals: External[]): Record<string, Expression>;
    static getInteligentInflater(expression: Expression, label: string): Inflater;
    static getSimpleInflater(type: PlyType, label: string): Inflater;
    static booleanInflaterFactory(label: string): Inflater;
    static timeRangeInflaterFactory(label: string, duration: Duration, timezone: Timezone): Inflater;
    static numberRangeInflaterFactory(label: string, rangeSize: number): Inflater;
    static numberInflaterFactory(label: string): Inflater;
    static timeInflaterFactory(label: string): Inflater;
    static setStringInflaterFactory(label: string): Inflater;
    static setCardinalityInflaterFactory(label: string): Inflater;
    static typeCheckDerivedAttributes(derivedAttributes: Record<string, Expression>, typeContext: DatasetFullType): Record<string, Expression>;
    static valuePostTransformFactory(): Transform;
    static inflateArrays(d: Datum, attributes: Attributes): void;
    static postTransformFactory(inflaters: Inflater[], attributes: Attributes, keys: string[], zeroTotalApplies: ApplyExpression[]): Transform;
    static performQueryAndPostTransform(queryAndPostTransform: QueryAndPostTransform<any>, requester: PlywoodRequester<any>, engine: string, rawQueries: any[] | null, computeContext: ComputeContext): ReadableStream;
    static buildValueFromStream(stream: ReadableStream): Promise<PlywoodValue>;
    static valuePromiseToStream(valuePromise: Promise<PlywoodValue>): ReadableStream;
    static jsToValue(parameters: ExternalJS, requester: PlywoodRequester<any>): ExternalValue;
    static classMap: Record<string, typeof External>;
    static register(ex: typeof External): void;
    static getConstructorFor(engine: string): typeof External;
    static uniteValueExternalsIntoTotal(keyExternals: {
        key: string;
        external?: External;
    }[]): External;
    static fromJS(parameters: ExternalJS, requester?: PlywoodRequester<any>): External;
    static fromValue(parameters: ExternalValue): External;
    engine: string;
    version: string;
    source: string | string[];
    suppress: boolean;
    rollup: boolean;
    attributes: Attributes;
    attributeOverrides: Attributes;
    derivedAttributes: Record<string, Expression>;
    delegates: External[];
    concealBuckets: boolean;
    rawAttributes: Attributes;
    requester: PlywoodRequester<any>;
    mode: QueryMode;
    filter: Expression;
    valueExpression: Expression;
    select: SelectExpression;
    split: SplitExpression;
    dataName: string;
    applies: ApplyExpression[];
    sort: SortExpression;
    limit: LimitExpression;
    havingFilter: Expression;
    constructor(parameters: ExternalValue, dummy?: any);
    protected _ensureEngine(engine: string): void;
    protected _ensureMinVersion(minVersion: string): void;
    valueOf(): ExternalValue;
    toJS(): ExternalJS;
    toJSON(): ExternalJS;
    toString(): string;
    equals(other: External | undefined): boolean;
    equalBaseAndFilter(other: External): boolean;
    equalBase(other: External): boolean;
    changeVersion(version: string): External;
    attachRequester(requester: PlywoodRequester<any>): External;
    versionBefore(neededVersion: string): boolean;
    protected capability(cap: string): boolean;
    getAttributesInfo(attributeName: string): AttributeInfo;
    updateAttribute(newAttribute: AttributeInfo): External;
    show(): External;
    hasAttribute(name: string): boolean;
    expressionDefined(ex: Expression): boolean;
    bucketsConcealed(ex: Expression): boolean;
    abstract canHandleFilter(filter: FilterExpression): boolean;
    abstract canHandleSort(sort: SortExpression): boolean;
    addDelegate(delegate: External): External;
    getBase(): External;
    getRaw(): External;
    makeTotal(applies: ApplyExpression[]): External;
    addExpression(ex: Expression): External;
    private _addFilterExpression;
    private _addSelectExpression;
    private _addSplitExpression;
    private _addApplyExpression;
    private _addSortExpression;
    private _addLimitExpression;
    private _addAggregateExpression;
    private _addPostAggregateExpression;
    prePush(ex: ChainableUnaryExpression): External;
    valueExpressionWithinFilter(withinFilter: Expression): Expression;
    toValueApply(): ApplyExpression;
    sortOnLabel(): boolean;
    getQuerySplit(): SplitExpression;
    getQueryFilter(): Expression;
    inlineDerivedAttributes(expression: Expression): Expression;
    getSelectedAttributes(): Attributes;
    getValueType(): PlyTypeSimple;
    addNextExternalToDatum(datum: Datum): void;
    getDelegate(): External;
    simulateValue(lastNode: boolean, simulatedQueries: any[], externalForNext?: External): PlywoodValue | TotalContainer;
    getQueryAndPostTransform(): QueryAndPostTransform<any>;
    queryValue(lastNode: boolean, rawQueries: any[], computeContext: ComputeContext, externalForNext?: External): Promise<PlywoodValue | TotalContainer>;
    protected queryBasicValueStream(rawQueries: any[] | null, computeContext: ComputeContext): ReadableStream;
    queryValueStream(lastNode: boolean, rawQueries: any[] | null, env: ComputeContext, externalForNext?: External): ReadableStream;
    needsIntrospect(): boolean;
    protected abstract getIntrospectAttributes(depth: IntrospectionDepth): Promise<Attributes>;
    introspect(options?: IntrospectOptions): Promise<External>;
    getRawFullType(skipDerived?: boolean): DatasetFullType;
    getFullType(): DatasetFullType;
}
