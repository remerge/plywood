export interface CustomDruidTransform {
    extractionFn: Druid.ExtractionFn;
}
export declare type CustomDruidTransforms = Record<string, CustomDruidTransform>;
export interface CustomDruidAggregation {
    aggregation?: Druid.Aggregation;
    aggregations?: Druid.Aggregation[];
    postAggregation?: Druid.PostAggregation;
    accessType?: string;
}
export declare type CustomDruidAggregations = Record<string, CustomDruidAggregation>;
