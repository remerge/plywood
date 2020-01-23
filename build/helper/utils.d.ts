import { ReadableStream, WritableStream } from 'readable-stream';
export declare function repeat(str: string, times: int): string;
export declare function indentBy(str: string, indent: int): string;
export declare function dictEqual(dictA: Record<string, any>, dictB: Record<string, any>): boolean;
export declare function shallowCopy<T>(thing: T): T;
export declare function deduplicateSort(a: string[]): string[];
export declare function mapLookup<T, U>(thing: Record<string, T>, fn: (x: T) => U): Record<string, U>;
export declare function emptyLookup(lookup: Record<string, any>): boolean;
export declare function nonEmptyLookup(lookup: Record<string, any>): boolean;
export declare function clip(x: number): number;
export declare function safeAdd(num: number, delta: number): number;
export declare function safeRange(num: number, delta: number): {
    start: number;
    end: number;
};
export declare function continuousFloorExpression(variable: string, floorFn: string, size: number, offset: number): string;
export declare class ExtendableError extends Error {
    stack: string;
    constructor(message: string);
}
export declare function pluralIfNeeded(n: number, thing: string): string;
export declare function pipeWithError(src: ReadableStream, dest: WritableStream): any;
