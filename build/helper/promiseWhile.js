export function promiseWhile(condition, action) {
    var loop = function () {
        if (!condition())
            return Promise.resolve(null);
        return Promise.resolve(action()).then(loop);
    };
    return Promise.resolve(null).then(loop);
}
