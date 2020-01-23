import { Timezone } from 'chronoshift';
import { Instance } from 'immutable-class';
import { PlyType } from '../types';
import { PlywoodValue } from './dataset';
import { PlywoodRange } from './range';
export interface SetValue {
    setType: PlyType;
    elements: Array<any>;
}
export interface SetJS {
    setType: PlyType;
    elements: Array<any>;
}
export declare class Set implements Instance<SetValue, SetJS> {
    static type: string;
    static EMPTY: Set;
    static unifyElements(elements: Array<PlywoodRange>): Array<PlywoodRange>;
    static intersectElements(elements1: Array<PlywoodRange>, elements2: Array<PlywoodRange>): Array<PlywoodRange>;
    static isSet(candidate: any): candidate is Set;
    static isAtomicType(type: PlyType): boolean;
    static isSetType(type: PlyType): boolean;
    static wrapSetType(type: PlyType): PlyType;
    static unwrapSetType(type: PlyType): PlyType;
    static cartesianProductOf<T>(...args: T[][]): T[][];
    static crossBinary(as: any, bs: any, fn: (a: any, b: any) => any): any;
    static crossBinaryBoolean(as: any, bs: any, fn: (a: any, b: any) => boolean): boolean;
    static crossUnary(as: any, fn: (a: any) => any): any;
    static crossUnaryBoolean(as: any, fn: (a: any) => boolean): boolean;
    static convertToSet(thing: any): Set;
    static unionCover(a: any, b: any): any;
    static intersectCover(a: any, b: any): any;
    static fromPlywoodValue(pv: PlywoodValue): Set;
    static fromJS(parameters: Array<any>): Set;
    static fromJS(parameters: SetJS): Set;
    setType: PlyType;
    elements: Array<any>;
    private keyFn;
    private hash;
    constructor(parameters: SetValue);
    valueOf(): SetValue;
    toJS(): SetJS;
    toJSON(): SetJS;
    toString(tz?: Timezone): string;
    equals(other: Set | undefined): boolean;
    changeElements(elements: any[]): Set;
    cardinality(): int;
    size(): int;
    empty(): boolean;
    isNullSet(): boolean;
    unifyElements(): Set;
    simplifyCover(): PlywoodValue;
    getType(): PlyType;
    upgradeType(): Set;
    downgradeType(): Set;
    extent(): PlywoodRange;
    union(other: Set): Set;
    intersect(other: Set): Set;
    overlap(other: Set): boolean;
    has(value: any): boolean;
    contains(value: any): boolean;
    add(value: any): Set;
    remove(value: any): Set;
    toggle(value: any): Set;
}
