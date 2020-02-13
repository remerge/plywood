import { PlywoodRequester } from 'plywood-base-api';
export interface CallbackParameters {
    name?: string;
    queryNumber?: number;
    query?: any;
    context?: any;
    time?: number;
    data?: any[];
    error?: Error;
}
export interface VerboseRequesterParameters<T> {
    requester: PlywoodRequester<T>;
    name?: string;
    printLine?: (line: string) => void;
    onQuery?: (param: CallbackParameters) => void;
    onSuccess?: (param: CallbackParameters) => void;
    onError?: (param: CallbackParameters) => void;
}
export declare function verboseRequesterFactory<T>(parameters: VerboseRequesterParameters<T>): PlywoodRequester<any>;
