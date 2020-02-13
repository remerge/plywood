export function basicExecutorFactory(parameters) {
    var datasets = parameters.datasets;
    return function (ex, opt, computeContext) {
        if (opt === void 0) { opt = {}; }
        if (computeContext === void 0) { computeContext = {}; }
        return ex.compute(datasets, opt, computeContext);
    };
}
