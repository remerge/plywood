import { AttributeInfo } from './attributeInfo';
import { Dataset, Datum, PlywoodValue } from './dataset';
export interface PlyBit {
    type: 'value' | 'init' | 'datum' | 'within';
    value?: PlywoodValue;
    attributes?: AttributeInfo[];
    keys?: string[];
    datum?: Datum;
    keyProp?: string;
    propValue?: PlywoodValue;
    attribute?: string;
    within?: PlyBit;
}
export interface PlywoodValueIterator {
    (): PlyBit | null;
}
export declare function iteratorFactory(value: PlywoodValue): PlywoodValueIterator;
export declare function datasetIteratorFactory(dataset: Dataset): PlywoodValueIterator;
export declare class PlywoodValueBuilder {
    private _value;
    private _attributes;
    private _keys;
    private _data;
    private _curAttribute;
    private _curValueBuilder;
    private _finalizeLastWithin;
    processBit(bit: PlyBit): void;
    getValue(): PlywoodValue;
}
