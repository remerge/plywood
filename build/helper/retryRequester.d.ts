import { PlywoodRequester } from 'plywood-base-api';
export interface RetryRequesterParameters<T> {
    requester: PlywoodRequester<T>;
    delay?: number;
    retry?: int;
    retryOnTimeout?: boolean;
}
export declare function retryRequesterFactory<T>(parameters: RetryRequesterParameters<T>): PlywoodRequester<T>;
