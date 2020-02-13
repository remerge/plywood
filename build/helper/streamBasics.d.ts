import { Readable } from 'readable-stream';
export declare class ReadableError extends Readable {
    constructor(message: string | Error, options?: any);
    protected _read(): void;
}
