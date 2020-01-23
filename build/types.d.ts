import { Timezone } from 'chronoshift';
export declare type PlyTypeSingleValue = 'NULL' | 'BOOLEAN' | 'NUMBER' | 'TIME' | 'STRING';
export declare type PlyTypeSimple = PlyTypeSingleValue | 'NUMBER_RANGE' | 'TIME_RANGE' | 'STRING_RANGE' | 'SET' | 'SET/NULL' | 'SET/BOOLEAN' | 'SET/NUMBER' | 'SET/TIME' | 'SET/STRING' | 'SET/NUMBER_RANGE' | 'SET/TIME_RANGE' | 'SET/STRING_RANGE';
export declare type PlyType = PlyTypeSimple | 'DATASET';
export interface SimpleFullType {
    type: PlyTypeSimple;
}
export interface DatasetFullType {
    type: 'DATASET';
    datasetType: Record<string, FullType>;
    parent?: DatasetFullType;
}
export declare type FullType = SimpleFullType | DatasetFullType;
export interface Environment {
    timezone?: Timezone;
}
