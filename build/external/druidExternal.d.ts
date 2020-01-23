import * as Druid from 'druid.d.ts';
import { PlywoodRequester } from 'plywood-base-api';
import { Transform, ReadableStream } from 'readable-stream';
import { Attributes, PlywoodRange } from '../datatypes/index';
import { ApplyExpression, Expression, FilterExpression, SortExpression, SplitExpression, TimeShiftExpression, ComputeContext } from '../expressions/index';
import { ExtendableError } from '../helper/utils';
import { External, ExternalJS, ExternalValue, Inflater, IntrospectionDepth, NextFn, QuerySelection, QueryAndPostTransform } from './baseExternal';
import { CustomDruidAggregations, CustomDruidTransforms } from './utils/druidTypes';
export declare class InvalidResultError extends ExtendableError {
    result: any;
    constructor(message: string, result: any);
}
export interface GranularityInflater {
    granularity: Druid.Granularity;
    inflater: Inflater;
}
export interface DimensionInflater {
    virtualColumn?: Druid.VirtualColumn;
    dimension: Druid.DimensionSpec;
    inflater?: Inflater;
}
export interface DimensionInflaterHaving extends DimensionInflater {
    having?: Expression;
}
export interface DruidSplit {
    queryType: string;
    timestampLabel?: string;
    virtualColumns?: Druid.VirtualColumn[];
    granularity: Druid.Granularity | string;
    dimension?: Druid.DimensionSpec;
    dimensions?: Druid.DimensionSpec[];
    leftoverHavingFilter?: Expression;
    postTransform: Transform;
}
export declare class DruidExternal extends External {
    static engine: string;
    static type: string;
    static DUMMY_NAME: string;
    static TIME_ATTRIBUTE: string;
    static VALID_INTROSPECTION_STRATEGIES: string[];
    static DEFAULT_INTROSPECTION_STRATEGY: string;
    static SELECT_INIT_LIMIT: number;
    static SELECT_MAX_LIMIT: number;
    static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any>): DruidExternal;
    static getSourceList(requester: PlywoodRequester<any>): Promise<string[]>;
    static getVersion(requester: PlywoodRequester<any>): Promise<string>;
    static isTimestampCompatibleSort(sort: SortExpression, label: string): boolean;
    static timeBoundaryPostTransformFactory(applies?: ApplyExpression[]): Transform;
    static selectNextFactory(limit: number, descending: boolean): NextFn<Druid.Query>;
    static generateMaker(aggregation: Druid.Aggregation): Expression;
    static columnMetadataToRange(columnMetadata: Druid.ColumnMetadata): null | PlywoodRange;
    static segmentMetadataPostProcess(timeAttribute: string, res: Druid.SegmentMetadataResults): Attributes;
    static introspectPostProcessFactory(timeAttribute: string, res: Druid.DatasourceIntrospectResult[]): Attributes;
    static movePagingIdentifiers(pagingIdentifiers: Druid.PagingIdentifiers, increment: number): Druid.PagingIdentifiers;
    timeAttribute: string;
    customAggregations: CustomDruidAggregations;
    customTransforms: CustomDruidTransforms;
    allowEternity: boolean;
    allowSelectQueries: boolean;
    introspectionStrategy: string;
    exactResultsOnly: boolean;
    querySelection: QuerySelection;
    context: Record<string, any>;
    constructor(parameters: ExternalValue);
    valueOf(): ExternalValue;
    toJS(): ExternalJS;
    equals(other: DruidExternal | undefined): boolean;
    canHandleFilter(filter: FilterExpression): boolean;
    canHandleSort(sort: SortExpression): boolean;
    getQuerySelection(): QuerySelection;
    getDruidDataSource(): Druid.DataSource;
    isTimeRef(ex: Expression): boolean;
    splitExpressionToGranularityInflater(splitExpression: Expression, label: string): GranularityInflater | null;
    makeOutputName(name: string): string;
    topNCompatibleSort(): boolean;
    expressionToDimensionInflater(expression: Expression, label: string): DimensionInflater;
    expressionToDimensionInflaterHaving(expression: Expression, label: string, havingFilter: Expression): DimensionInflaterHaving;
    splitToDruid(split: SplitExpression): DruidSplit;
    isMinMaxTimeExpression(applyExpression: Expression): boolean;
    getTimeBoundaryQueryAndPostTransform(): QueryAndPostTransform<Druid.Query>;
    nestedGroupByIfNeeded(): QueryAndPostTransform<Druid.Query> | null;
    getQueryAndPostTransform(): QueryAndPostTransform<Druid.Query>;
    protected getIntrospectAttributesWithSegmentMetadata(depth: IntrospectionDepth): Promise<Attributes>;
    protected getIntrospectAttributesWithGet(): Promise<Attributes>;
    protected getIntrospectAttributes(depth: IntrospectionDepth): Promise<Attributes>;
    private groupAppliesByTimeFilterValue;
    getJoinDecompositionShortcut(): {
        external1: DruidExternal;
        external2: DruidExternal;
        timeShift?: TimeShiftExpression;
        waterfallFilterExpression?: SplitExpression;
    } | null;
    protected queryBasicValueStream(rawQueries: any[] | null, computeContext: ComputeContext): ReadableStream;
}
