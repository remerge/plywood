import { PlywoodRequester } from 'plywood-base-api';
interface ConcurrentRequestStore {
  inc(): void;
  dec(): void;
  requests(): number;
}
export interface ConcurrentLimitRequesterParameters<T> {
    requester: PlywoodRequester<T>;
    concurrentLimit: int;
    concurrentRequests: ConcurrentRequestStore;
}
export declare function concurrentLimitRequesterFactory<T>(parameters: ConcurrentLimitRequesterParameters<T>): PlywoodRequester<T>;
