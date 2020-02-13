import { Expression } from '../../expressions';
import { CustomDruidTransforms } from './druidTypes';
export interface DruidExtractionFnBuilderOptions {
    version: string;
    customTransforms: CustomDruidTransforms;
}
export declare class DruidExtractionFnBuilder {
    static CASE_TO_DRUID: Record<string, string>;
    static TIME_PART_TO_FORMAT: Record<string, string>;
    static TIME_PART_TO_EXPR: Record<string, string>;
    static composeFns(f: Druid.ExtractionFn | null, g: Druid.ExtractionFn | null): Druid.ExtractionFn | null;
    static getLastFn(fn: Druid.ExtractionFn): Druid.ExtractionFn;
    static wrapFunctionTryCatch(lines: string[]): string;
    version: string;
    customTransforms: CustomDruidTransforms;
    allowJavaScript: boolean;
    constructor(options: DruidExtractionFnBuilderOptions, allowJavaScript: boolean);
    expressionToExtractionFn(expression: Expression): Druid.ExtractionFn | null;
    private expressionToExtractionFnPure;
    private literalToExtractionFn;
    private refToExtractionFn;
    private concatToExtractionFn;
    private timeFloorToExtractionFn;
    private timePartToExtractionFn;
    private numberBucketToExtractionFn;
    private substrToExtractionFn;
    private transformCaseToExtractionFn;
    private lengthToExtractionFn;
    private extractToExtractionFn;
    private lookupToExtractionFn;
    private fallbackToExtractionFn;
    private customTransformToExtractionFn;
    private castToExtractionFn;
    private overlapToExtractionFn;
    private expressionToJavaScriptExtractionFn;
    private versionBefore;
}
