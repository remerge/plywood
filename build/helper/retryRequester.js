import { PassThrough } from 'readable-stream';
export function retryRequesterFactory(parameters) {
    var requester = parameters.requester;
    var delay = parameters.delay || 500;
    var retry = parameters.retry || 3;
    var retryOnTimeout = Boolean(parameters.retryOnTimeout);
    if (typeof delay !== "number")
        throw new TypeError("delay should be a number");
    if (typeof retry !== "number")
        throw new TypeError("retry should be a number");
    return function (request) {
        var tries = 0;
        var ended = false;
        var output = new PassThrough({ objectMode: true });
        function tryRequest() {
            tries++;
            var seenData = false;
            var errored = false;
            var rs = requester(request);
            rs.on('error', function (e) {
                errored = true;
                if (seenData || tries > retry || (e.message === "timeout" && !retryOnTimeout)) {
                    rs.unpipe(output);
                    output.emit('error', e);
                    ended = true;
                    output.end();
                }
                else {
                    setTimeout(tryRequest, delay);
                }
            });
            rs.on('meta', function (m) { output.emit('meta', m); });
            rs.on('data', function (d) { seenData = true; });
            rs.on('end', function () {
                if (ended || errored)
                    return;
                output.end();
            });
            rs.pipe(output, { end: false });
        }
        tryRequest();
        return output;
    };
}
