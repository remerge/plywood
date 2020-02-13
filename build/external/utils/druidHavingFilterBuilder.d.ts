import { AttributeInfo } from '../../datatypes/index';
import { Expression } from '../../expressions';
import { CustomDruidTransforms } from './druidTypes';
export interface DruidHavingFilterBuilderOptions {
    version: string;
    attributes: AttributeInfo[];
    customTransforms: CustomDruidTransforms;
}
export declare class DruidHavingFilterBuilder {
    version: string;
    attributes: AttributeInfo[];
    customTransforms: CustomDruidTransforms;
    constructor(options: DruidHavingFilterBuilderOptions);
    filterToHavingFilter(filter: Expression): Druid.Having;
}
