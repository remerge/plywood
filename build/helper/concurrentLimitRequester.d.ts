import { PlywoodRequester } from 'plywood-base-api';
import { Gauge } from 'prom-client';
export interface ConcurrentLimitRequesterParameters<T> {
    requester: PlywoodRequester<T>;
    concurrentLimit: int;
    concurrentRequests: Gauge;
}
export declare function concurrentLimitRequesterFactory<T>(parameters: ConcurrentLimitRequesterParameters<T>): PlywoodRequester<T>;
