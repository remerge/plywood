export function verboseRequesterFactory(parameters) {
    var requester = parameters.requester;
    var myName = parameters.name || 'rq' + String(Math.random()).substr(2, 5);
    if (parameters.preQuery) {
        console.warn('verboseRequesterFactory option preQuery has been renamed to onQuery');
        parameters.onQuery = parameters.preQuery;
    }
    var printLine = parameters.printLine || (function (line) {
        console['log'](line);
    });
    var onQuery = parameters.onQuery || (function (param) {
        printLine("vvvvvvvvvvvvvvvvvvvvvvvvvv");
        var ctx = param.context ? " [context: " + JSON.stringify(param.context) + "]" : '';
        printLine("Requester " + param.name + " sending query " + param.queryNumber + ":" + ctx);
        printLine(JSON.stringify(param.query, null, 2));
        printLine("^^^^^^^^^^^^^^^^^^^^^^^^^^");
    });
    var onSuccess = parameters.onSuccess || (function (param) {
        printLine("vvvvvvvvvvvvvvvvvvvvvvvvvv");
        printLine("Requester " + param.name + " got result from query " + param.queryNumber + ": (in " + param.time + "ms)");
        printLine(JSON.stringify(param.data, null, 2));
        printLine("^^^^^^^^^^^^^^^^^^^^^^^^^^");
    });
    var onError = parameters.onError || (function (param) {
        printLine("vvvvvvvvvvvvvvvvvvvvvvvvvv");
        printLine("Requester " + param.name + " got error in query " + param.queryNumber + ": " + param.error.message + " (in " + param.time + "ms)");
        printLine("^^^^^^^^^^^^^^^^^^^^^^^^^^");
    });
    var curQueryNumber = 0;
    return function (request) {
        curQueryNumber++;
        var myQueryNumber = curQueryNumber;
        onQuery({
            name: myName,
            queryNumber: myQueryNumber,
            query: request.query,
            context: request.context
        });
        var startTime = Date.now();
        var stream = requester(request);
        var errorSeen = false;
        stream.on('error', function (error) {
            errorSeen = true;
            onError({
                name: myName,
                queryNumber: myQueryNumber,
                query: request.query,
                context: request.context,
                time: Date.now() - startTime,
                error: error
            });
        });
        var data = [];
        stream.on('data', function (datum) {
            data.push(JSON.parse(JSON.stringify(datum)));
        });
        stream.on('end', function () {
            if (errorSeen)
                return;
            onSuccess({
                name: myName,
                queryNumber: myQueryNumber,
                query: request.query,
                context: request.context,
                time: Date.now() - startTime,
                data: data
            });
        });
        return stream;
    };
}
