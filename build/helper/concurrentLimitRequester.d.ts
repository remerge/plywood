import { PlywoodRequester } from 'plywood-base-api';
export interface ConcurrentLimitRequesterParameters<T> {
    requester: PlywoodRequester<T>;
    concurrentLimit: int;
}
export declare function concurrentLimitRequesterFactory<T>(parameters: ConcurrentLimitRequesterParameters<T>): PlywoodRequester<T>;
