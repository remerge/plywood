import { PassThrough } from 'readable-stream';
import { pipeWithError } from './utils';
export function concurrentLimitRequesterFactory(parameters) {
    var requester = parameters.requester;
    var concurrentLimit = parameters.concurrentLimit || 5;
    if (typeof concurrentLimit !== "number")
        throw new TypeError("concurrentLimit should be a number");
    var requestQueue = [];
    var outstandingRequests = 0;
    function requestFinished() {
        outstandingRequests--;
        if (!(requestQueue.length && outstandingRequests < concurrentLimit))
            return;
        var queueItem = requestQueue.shift();
        outstandingRequests++;
        var stream = requester(queueItem.request);
        stream.on('error', requestFinished);
        stream.on('end', requestFinished);
        pipeWithError(stream, queueItem.stream);
    }
    return function (request) {
        if (outstandingRequests < concurrentLimit) {
            outstandingRequests++;
            var stream = requester(request);
            stream.on('error', requestFinished);
            stream.on('end', requestFinished);
            return stream;
        }
        else {
            var stream = new PassThrough({ objectMode: true });
            requestQueue.push({
                request: request,
                stream: stream
            });
            return stream;
        }
    };
}
