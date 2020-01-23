import { Instance } from 'immutable-class';
import { Expression, ExpressionJS } from '../expressions';
import { FullType, PlyType } from '../types';
import { PlywoodRange, PlywoodRangeJS } from './range';
export declare type Attributes = AttributeInfo[];
export declare type AttributeJSs = AttributeInfoJS[];
export interface AttributeInfoValue {
    name: string;
    type?: PlyType;
    nativeType?: string;
    unsplitable?: boolean;
    maker?: Expression;
    cardinality?: number;
    range?: PlywoodRange;
    termsDelegate?: string;
}
export interface AttributeInfoJS {
    name: string;
    type?: PlyType;
    nativeType?: string;
    unsplitable?: boolean;
    maker?: ExpressionJS;
    cardinality?: number;
    range?: PlywoodRangeJS;
    termsDelegate?: string;
}
export declare class AttributeInfo implements Instance<AttributeInfoValue, AttributeInfoJS> {
    static isAttributeInfo(candidate: any): candidate is AttributeInfo;
    static NATIVE_TYPE_FROM_SPECIAL: Record<string, string>;
    static fromJS(parameters: AttributeInfoJS): AttributeInfo;
    static fromJSs(attributeJSs: AttributeJSs): Attributes;
    static toJSs(attributes: Attributes): AttributeJSs;
    static override(attributes: Attributes, attributeOverrides: Attributes): Attributes;
    name: string;
    nativeType: string;
    type: PlyType;
    datasetType?: Record<string, FullType>;
    unsplitable: boolean;
    maker?: Expression;
    cardinality?: number;
    range?: PlywoodRange;
    termsDelegate?: string;
    constructor(parameters: AttributeInfoValue);
    toString(): string;
    valueOf(): AttributeInfoValue;
    toJS(): AttributeInfoJS;
    toJSON(): AttributeInfoJS;
    equals(other: AttributeInfo | undefined): boolean;
    dropOriginInfo(): AttributeInfo;
    get(propertyName: string): any;
    deepGet(propertyName: string): any;
    change(propertyName: string, newValue: any): AttributeInfo;
    deepChange(propertyName: string, newValue: any): AttributeInfo;
    changeType(type: PlyType): AttributeInfo;
    getUnsplitable(): boolean;
    changeUnsplitable(unsplitable: boolean): AttributeInfo;
    changeRange(range: PlywoodRange): AttributeInfo;
}
