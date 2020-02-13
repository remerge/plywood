import { PassThrough, Stream } from 'readable-stream';
export declare class StreamConcat extends PassThrough {
    next: () => Stream;
    currentStream: Stream;
    streamIndex: number;
    constructor(options: any);
    private _nextStream;
}
