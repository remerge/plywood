import { AttributeInfo } from '../../datatypes/index';
import { Expression } from '../../expressions';
import { CustomDruidTransforms } from './druidTypes';
export interface DruidFilterAndIntervals {
    filter: Druid.Filter;
    intervals: Druid.Intervals;
}
export interface DruidFilterBuilderOptions {
    version: string;
    rawAttributes: AttributeInfo[];
    timeAttribute: string;
    allowEternity: boolean;
    customTransforms: CustomDruidTransforms;
}
export declare class DruidFilterBuilder {
    static TIME_ATTRIBUTE: string;
    static TRUE_INTERVAL: string;
    version: string;
    rawAttributes: AttributeInfo[];
    timeAttribute: string;
    allowEternity: boolean;
    customTransforms: CustomDruidTransforms;
    constructor(options: DruidFilterBuilderOptions);
    filterToDruid(filter: Expression): DruidFilterAndIntervals;
    timeFilterToIntervals(filter: Expression): Druid.Intervals;
    timelessFilterToFilter(filter: Expression): Druid.Filter;
    private makeJavaScriptFilter;
    private valueToIntervals;
    private makeSelectorFilter;
    private makeInFilter;
    private makeBoundFilter;
    private makeIntervalFilter;
    private makeRegexFilter;
    private makeContainsFilter;
    private makeExpressionFilter;
    private getSingleReferenceAttributeInfo;
    private getDimensionNameForAttributeInfo;
    private versionBefore;
    getAttributesInfo(attributeName: string): AttributeInfo;
    isTimeRef(ex: Expression): boolean;
}
