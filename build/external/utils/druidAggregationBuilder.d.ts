import { AttributeInfo } from '../../datatypes/index';
import { ApplyExpression, Expression } from '../../expressions';
import { CustomDruidAggregations, CustomDruidTransforms } from './druidTypes';
export interface AggregationsAndPostAggregations {
    aggregations: Druid.Aggregation[];
    postAggregations: Druid.PostAggregation[];
}
export interface DruidAggregationBuilderOptions {
    version: string;
    rawAttributes: AttributeInfo[];
    timeAttribute: string;
    derivedAttributes: Record<string, Expression>;
    customAggregations: CustomDruidAggregations;
    customTransforms: CustomDruidTransforms;
    rollup: boolean;
    exactResultsOnly: boolean;
    allowEternity: boolean;
}
export declare class DruidAggregationBuilder {
    static AGGREGATE_TO_FUNCTION: Record<string, Function>;
    static AGGREGATE_TO_ZERO: Record<string, string>;
    static APPROX_HISTOGRAM_TUNINGS: string[];
    static QUANTILES_DOUBLES_TUNINGS: string[];
    static addOptionsToAggregation(aggregation: Druid.Aggregation, expression: Expression): void;
    version: string;
    rawAttributes: AttributeInfo[];
    timeAttribute: string;
    derivedAttributes: Record<string, Expression>;
    customAggregations: CustomDruidAggregations;
    customTransforms: CustomDruidTransforms;
    rollup: boolean;
    exactResultsOnly: boolean;
    allowEternity: boolean;
    constructor(options: DruidAggregationBuilderOptions);
    makeAggregationsAndPostAggregations(applies: ApplyExpression[]): AggregationsAndPostAggregations;
    private applyToAggregation;
    private applyToPostAggregation;
    private filterAggregateIfNeeded;
    private expressionToAggregation;
    private countToAggregation;
    private sumMinMaxToAggregation;
    private getCardinalityExpressions;
    private countDistinctToAggregation;
    private customAggregateToAggregation;
    private quantileToAggregation;
    private makeJavaScriptAggregation;
    private getAccessTypeForAggregation;
    private getAccessType;
    private expressionToPostAggregation;
    private expressionToLegacyPostAggregation;
    private switchToRollupCount;
    private getRollupCountName;
    private inlineDerivedAttributes;
    private inlineDerivedAttributesInAggregate;
    getAttributesInfo(attributeName: string): AttributeInfo;
    private versionBefore;
}
