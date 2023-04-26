import { PassThrough } from 'readable-stream';
import { pipeWithError } from './utils';
function generateRequestId() {
    return Math.random().toString(26).slice(2);
}
export function concurrentLimitRequesterFactory(parameters) {
    var requester = parameters.requester;
    var concurrentLimit = parameters.concurrentLimit || 5;
    if (typeof concurrentLimit !== "number")
        throw new TypeError("concurrentLimit should be a number");
    var requestQueue = [];
    var outstandingRequests = 0;
    setInterval(function () {
        console.log("Concurrent Limit: " + outstandingRequests + " / " + concurrentLimit + ". Queue length: " + requestQueue.length);
    }, 60 * 1000);
    function requestFinished() {
        outstandingRequests--;
        if (!(requestQueue.length && outstandingRequests < concurrentLimit))
            return;
        var queueItem = requestQueue.shift();
        outstandingRequests++;
        var requestId = generateRequestId();
        console.log("Starting request " + requestId + " (" + outstandingRequests + "/" + concurrentLimit + "): " + queueItem.request.query.queryType);
        var stream = requester(queueItem.request);
        var requestFinishedOnce = getOnceCallback(function () {
            console.log("Request finished " + requestId);
            requestFinished();
        });
        stream.on('error', requestFinishedOnce);
        stream.on('end', requestFinishedOnce);
        pipeWithError(stream, queueItem.stream);
    }
    return function (request) {
        if (outstandingRequests < concurrentLimit) {
            outstandingRequests++;
            var requestId_1 = generateRequestId();
            console.log("Starting request " + requestId_1 + " (" + outstandingRequests + "/" + concurrentLimit + "): " + request.query.queryType);
            var stream = requester(request);
            var requestFinishedOnce = getOnceCallback(function () {
                console.log("Request finished " + requestId_1);
                requestFinished();
            });
            stream.on('error', requestFinishedOnce);
            stream.on('end', requestFinishedOnce);
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
function getOnceCallback(callback) {
    var called = false;
    return function () {
        if (!called) {
            called = true;
            callback();
        }
    };
}
