import { AttributeInfo } from '../../datatypes';
import { Expression } from '../../expressions';
import { PlyType } from '../../types';
export interface DruidExpressionBuilderOptions {
    version: string;
    rawAttributes: AttributeInfo[];
    timeAttribute: string;
}
export declare class DruidExpressionBuilder {
    static TIME_PART_TO_FORMAT: Record<string, string>;
    static UNSAFE_CHAR: RegExp;
    static escape(str: string): string;
    static escapeVariable(name: string): string;
    static escapeLiteral(x: number | string | Date): string;
    static escapeLike(str: string): string;
    static expressionTypeToOutputType(type: PlyType): Druid.OutputType;
    version: string;
    rawAttributes: AttributeInfo[];
    timeAttribute: string;
    constructor(options: DruidExpressionBuilderOptions);
    expressionToDruidExpression(expression: Expression): string | null;
    private castToType;
    private overlapExpression;
    private checkDruid12;
    private checkDruid11;
    getAttributesInfo(attributeName: string): AttributeInfo;
    private versionBefore;
}
