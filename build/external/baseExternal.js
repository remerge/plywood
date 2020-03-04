import * as tslib_1 from "tslib";
import { Timezone } from 'chronoshift';
import * as hasOwnProp from 'has-own-prop';
import { immutableArraysEqual, immutableLookupsEqual, NamedArray, SimpleArray } from 'immutable-class';
import { Transform, Writable, PassThrough } from 'readable-stream';
import { AttributeInfo, Dataset, NumberRange, PlywoodValueBuilder } from '../datatypes/index';
import { Set } from '../datatypes/set';
import { StringRange } from '../datatypes/stringRange';
import { TimeRange } from '../datatypes/timeRange';
import { iteratorFactory } from '../datatypes/valueStream';
import { $, AndExpression, ApplyExpression, ChainableExpression, ChainableUnaryExpression, Expression, ExternalExpression, FilterExpression, LimitExpression, NumberBucketExpression, RefExpression, SelectExpression, SortExpression, SplitExpression, TimeBucketExpression, TimeFloorExpression } from '../expressions/index';
import { ReadableError } from '../helper/streamBasics';
import { StreamConcat } from '../helper/streamConcat';
import { nonEmptyLookup, pipeWithError, safeRange } from '../helper/utils';
var TotalContainer = (function () {
    function TotalContainer(d) {
        this.datum = d;
    }
    TotalContainer.prototype.toJS = function () {
        return {
            datum: Dataset.datumToJS(this.datum)
        };
    };
    return TotalContainer;
}());
export { TotalContainer };
function makeDate(thing) {
    var dt = new Date(thing);
    if (isNaN(dt.valueOf()))
        dt = new Date(Number(thing));
    return dt;
}
function nullMap(xs, fn) {
    if (!xs)
        return null;
    var res = [];
    for (var _i = 0, xs_1 = xs; _i < xs_1.length; _i++) {
        var x = xs_1[_i];
        var y = fn(x);
        if (y)
            res.push(y);
    }
    return res.length ? res : null;
}
function filterToAnds(filter) {
    if (filter.equals(Expression.TRUE))
        return [];
    if (filter instanceof AndExpression)
        return filter.getExpressionList();
    return [filter];
}
function filterDiff(strongerFilter, weakerFilter) {
    var strongerFilterAnds = filterToAnds(strongerFilter);
    var weakerFilterAnds = filterToAnds(weakerFilter);
    if (weakerFilterAnds.length > strongerFilterAnds.length)
        return null;
    for (var i = 0; i < weakerFilterAnds.length; i++) {
        if (!(weakerFilterAnds[i].equals(strongerFilterAnds[i])))
            return null;
    }
    return Expression.and(strongerFilterAnds.slice(weakerFilterAnds.length));
}
function getCommonFilter(filter1, filter2) {
    var filter1Ands = filterToAnds(filter1);
    var filter2Ands = filterToAnds(filter2);
    var minLength = Math.min(filter1Ands.length, filter2Ands.length);
    var commonExpressions = [];
    for (var i = 0; i < minLength; i++) {
        if (!filter1Ands[i].equals(filter2Ands[i]))
            break;
        commonExpressions.push(filter1Ands[i]);
    }
    return Expression.and(commonExpressions);
}
function mergeDerivedAttributes(derivedAttributes1, derivedAttributes2) {
    var derivedAttributes = Object.create(null);
    for (var k in derivedAttributes1) {
        derivedAttributes[k] = derivedAttributes1[k];
    }
    for (var k in derivedAttributes2) {
        if (hasOwnProp(derivedAttributes, k) && !derivedAttributes[k].equals(derivedAttributes2[k])) {
            throw new Error("can not currently redefine conflicting " + k);
        }
        derivedAttributes[k] = derivedAttributes2[k];
    }
    return derivedAttributes;
}
function getSampleValue(valueType, ex) {
    switch (valueType) {
        case 'NULL':
            return null;
        case 'BOOLEAN':
            return true;
        case 'NUMBER':
            return 4;
        case 'NUMBER_RANGE':
            if (ex instanceof NumberBucketExpression) {
                return new NumberRange({
                    start: ex.offset,
                    end: ex.offset + ex.size
                });
            }
            else {
                return new NumberRange({ start: 0, end: 1 });
            }
        case 'TIME':
            return new Date('2015-03-14T00:00:00Z');
        case 'TIME_RANGE':
            if (ex instanceof TimeBucketExpression) {
                var timezone = ex.timezone || Timezone.UTC;
                var start = ex.duration.floor(new Date('2015-03-14T00:00:00Z'), timezone);
                return new TimeRange({
                    start: start,
                    end: ex.duration.shift(start, timezone, 1)
                });
            }
            else {
                return new TimeRange({ start: new Date('2015-03-14T00:00:00Z'), end: new Date('2015-03-15T00:00:00Z') });
            }
        case 'STRING':
            if (ex instanceof RefExpression) {
                return 'some_' + ex.name;
            }
            else {
                return 'something';
            }
        case 'SET/STRING':
            if (ex instanceof RefExpression) {
                return Set.fromJS([ex.name + '1']);
            }
            else {
                return Set.fromJS(['something']);
            }
        case 'STRING_RANGE':
            if (ex instanceof RefExpression) {
                return StringRange.fromJS({ start: 'some_' + ex.name, end: null });
            }
            else {
                return StringRange.fromJS({ start: 'something', end: null });
            }
        default:
            throw new Error("unsupported simulation on: " + valueType);
    }
}
function immutableAdd(obj, key, value) {
    var newObj = Object.create(null);
    for (var k in obj)
        newObj[k] = obj[k];
    newObj[key] = value;
    return newObj;
}
function findApplyByExpression(applies, expression) {
    for (var _i = 0, applies_1 = applies; _i < applies_1.length; _i++) {
        var apply = applies_1[_i];
        if (apply.expression.equals(expression))
            return apply;
    }
    return null;
}
var External = (function () {
    function External(parameters, dummy) {
        if (dummy === void 0) { dummy = null; }
        this.attributes = null;
        this.attributeOverrides = null;
        if (dummy !== dummyObject) {
            throw new TypeError("can not call `new External` directly use External.fromJS instead");
        }
        this.engine = parameters.engine;
        var version = null;
        if (parameters.version) {
            version = External.extractVersion(parameters.version);
            if (!version)
                throw new Error("invalid version " + parameters.version);
        }
        this.version = version;
        this.source = parameters.source;
        this.suppress = Boolean(parameters.suppress);
        this.rollup = Boolean(parameters.rollup);
        if (parameters.attributes) {
            this.attributes = parameters.attributes;
        }
        if (parameters.attributeOverrides) {
            this.attributeOverrides = parameters.attributeOverrides;
        }
        this.derivedAttributes = parameters.derivedAttributes || {};
        if (parameters.delegates) {
            this.delegates = parameters.delegates;
        }
        this.concealBuckets = parameters.concealBuckets;
        this.rawAttributes = parameters.rawAttributes || parameters.attributes || [];
        this.requester = parameters.requester;
        this.mode = parameters.mode || 'raw';
        this.filter = parameters.filter || Expression.TRUE;
        if (this.rawAttributes.length) {
            this.derivedAttributes = External.typeCheckDerivedAttributes(this.derivedAttributes, this.getRawFullType(true));
            this.filter = this.filter.changeInTypeContext(this.getRawFullType());
        }
        switch (this.mode) {
            case 'raw':
                this.select = parameters.select;
                this.sort = parameters.sort;
                this.limit = parameters.limit;
                break;
            case 'value':
                this.valueExpression = parameters.valueExpression;
                break;
            case 'total':
                this.applies = parameters.applies || [];
                break;
            case 'split':
                this.select = parameters.select;
                this.dataName = parameters.dataName;
                this.split = parameters.split;
                if (!this.split)
                    throw new Error('must have split action in split mode');
                this.applies = parameters.applies || [];
                this.sort = parameters.sort;
                this.limit = parameters.limit;
                this.havingFilter = parameters.havingFilter || Expression.TRUE;
                break;
        }
    }
    External.isExternal = function (candidate) {
        return candidate instanceof External;
    };
    External.extractVersion = function (v) {
        if (!v)
            return null;
        var m = v.match(/^\d+\.\d+\.\d+(?:-[\w\-]+)?/);
        return m ? m[0] : null;
    };
    External.versionLessThan = function (va, vb) {
        var pa = va.split('-')[0].split('.');
        var pb = vb.split('-')[0].split('.');
        if (pa[0] !== pb[0])
            return Number(pa[0]) < Number(pb[0]);
        if (pa[1] !== pb[1])
            return Number(pa[1]) < Number(pb[1]);
        return Number(pa[2]) < Number(pb[2]);
    };
    External.deduplicateExternals = function (externals) {
        if (externals.length < 2)
            return externals;
        var uniqueExternals = [externals[0]];
        function addToUniqueExternals(external) {
            for (var _i = 0, uniqueExternals_1 = uniqueExternals; _i < uniqueExternals_1.length; _i++) {
                var uniqueExternal = uniqueExternals_1[_i];
                if (uniqueExternal.equalBaseAndFilter(external))
                    return;
            }
            uniqueExternals.push(external);
        }
        for (var i = 1; i < externals.length; i++)
            addToUniqueExternals(externals[i]);
        return uniqueExternals;
    };
    External.addExtraFilter = function (ex, extraFilter) {
        if (extraFilter.equals(Expression.TRUE))
            return ex;
        return ex.substitute(function (ex) {
            if (ex instanceof RefExpression && ex.type === 'DATASET' && ex.name === External.SEGMENT_NAME) {
                return ex.filter(extraFilter);
            }
            return null;
        });
    };
    External.makeZeroDatum = function (applies) {
        var newDatum = Object.create(null);
        for (var _i = 0, applies_2 = applies; _i < applies_2.length; _i++) {
            var apply = applies_2[_i];
            var applyName = apply.name;
            if (applyName[0] === '_')
                continue;
            newDatum[applyName] = 0;
        }
        return newDatum;
    };
    External.normalizeAndAddApply = function (attributesAndApplies, apply) {
        var attributes = attributesAndApplies.attributes, applies = attributesAndApplies.applies;
        var expressions = Object.create(null);
        for (var _i = 0, applies_3 = applies; _i < applies_3.length; _i++) {
            var existingApply = applies_3[_i];
            expressions[existingApply.name] = existingApply.expression;
        }
        apply = apply.changeExpression(apply.expression.resolveWithExpressions(expressions, 'leave').simplify());
        return {
            attributes: NamedArray.overrideByName(attributes, new AttributeInfo({ name: apply.name, type: apply.expression.type })),
            applies: NamedArray.overrideByName(applies, apply)
        };
    };
    External.segregationAggregateApplies = function (applies) {
        var aggregateApplies = [];
        var postAggregateApplies = [];
        var nameIndex = 0;
        var appliesToSegregate = [];
        for (var _i = 0, applies_4 = applies; _i < applies_4.length; _i++) {
            var apply = applies_4[_i];
            var applyExpression = apply.expression;
            if (applyExpression.isAggregate()) {
                aggregateApplies.push(apply);
            }
            else {
                appliesToSegregate.push(apply);
            }
        }
        for (var _a = 0, appliesToSegregate_1 = appliesToSegregate; _a < appliesToSegregate_1.length; _a++) {
            var apply = appliesToSegregate_1[_a];
            var newExpression = apply.expression.substitute(function (ex) {
                if (ex.isAggregate()) {
                    var existingApply = findApplyByExpression(aggregateApplies, ex);
                    if (existingApply) {
                        return $(existingApply.name, ex.type);
                    }
                    else {
                        var name_1 = '!T_' + (nameIndex++);
                        aggregateApplies.push(Expression._.apply(name_1, ex));
                        return $(name_1, ex.type);
                    }
                }
                return null;
            });
            postAggregateApplies.push(apply.changeExpression(newExpression));
        }
        return {
            aggregateApplies: aggregateApplies,
            postAggregateApplies: postAggregateApplies
        };
    };
    External.getCommonFilterFromExternals = function (externals) {
        if (!externals.length)
            throw new Error('must have externals');
        var commonFilter = externals[0].filter;
        for (var i = 1; i < externals.length; i++) {
            commonFilter = getCommonFilter(commonFilter, externals[i].filter);
        }
        return commonFilter;
    };
    External.getMergedDerivedAttributesFromExternals = function (externals) {
        if (!externals.length)
            throw new Error('must have externals');
        var derivedAttributes = externals[0].derivedAttributes;
        for (var i = 1; i < externals.length; i++) {
            derivedAttributes = mergeDerivedAttributes(derivedAttributes, externals[i].derivedAttributes);
        }
        return derivedAttributes;
    };
    External.getInteligentInflater = function (expression, label) {
        if (expression instanceof NumberBucketExpression) {
            return External.numberRangeInflaterFactory(label, expression.size);
        }
        else if (expression instanceof TimeBucketExpression) {
            return External.timeRangeInflaterFactory(label, expression.duration, expression.timezone);
        }
        else {
            return External.getSimpleInflater(expression.type, label);
        }
    };
    External.getSimpleInflater = function (type, label) {
        switch (type) {
            case 'BOOLEAN': return External.booleanInflaterFactory(label);
            case 'NULL': return External.nullInflaterFactory(label);
            case 'NUMBER': return External.numberInflaterFactory(label);
            case 'STRING': return External.stringInflaterFactory(label);
            case 'TIME': return External.timeInflaterFactory(label);
            default: return null;
        }
    };
    External.booleanInflaterFactory = function (label) {
        return function (d) {
            if (typeof d[label] === 'undefined') {
                d[label] = null;
                return;
            }
            var v = '' + d[label];
            switch (v) {
                case 'null':
                    d[label] = null;
                    break;
                case '0':
                case 'false':
                    d[label] = false;
                    break;
                case '1':
                case 'true':
                    d[label] = true;
                    break;
                default:
                    throw new Error("got strange result from boolean: " + v);
            }
        };
    };
    External.timeRangeInflaterFactory = function (label, duration, timezone) {
        return function (d) {
            var v = d[label];
            if ('' + v === "null") {
                d[label] = null;
                return;
            }
            var start = makeDate(v);
            d[label] = new TimeRange({ start: start, end: duration.shift(start, timezone) });
        };
    };
    External.nullInflaterFactory = function (label) {
        return function (d) {
            var v = d[label];
            if ('' + v === "null" || typeof v === 'undefined') {
                d[label] = null;
            }
        };
    };
    External.numberRangeInflaterFactory = function (label, rangeSize) {
        return function (d) {
            var v = d[label];
            if ('' + v === "null") {
                d[label] = null;
                return;
            }
            var start = Number(v);
            d[label] = new NumberRange(safeRange(start, rangeSize));
        };
    };
    External.numberInflaterFactory = function (label) {
        return function (d) {
            var v = d[label];
            if ('' + v === "null") {
                d[label] = null;
                return;
            }
            v = Number(v);
            d[label] = isNaN(v) ? null : v;
        };
    };
    External.stringInflaterFactory = function (label) {
        return function (d) {
            var v = d[label];
            if (typeof v === 'undefined') {
                d[label] = null;
            }
        };
    };
    External.timeInflaterFactory = function (label) {
        return function (d) {
            var v = d[label];
            if ('' + v === "null" || typeof v === 'undefined') {
                d[label] = null;
                return;
            }
            d[label] = makeDate(v);
        };
    };
    External.setStringInflaterFactory = function (label) {
        return function (d) {
            var v = d[label];
            if ('' + v === "null") {
                d[label] = null;
                return;
            }
            if (typeof v === 'string')
                v = [v];
            d[label] = Set.fromJS({
                setType: 'STRING',
                elements: v
            });
        };
    };
    External.setCardinalityInflaterFactory = function (label) {
        return function (d) {
            var v = d[label];
            d[label] = Array.isArray(v) ? v.length : 1;
        };
    };
    External.typeCheckDerivedAttributes = function (derivedAttributes, typeContext) {
        var changed = false;
        var newDerivedAttributes = {};
        for (var k in derivedAttributes) {
            var ex = derivedAttributes[k];
            var newEx = ex.changeInTypeContext(typeContext);
            if (ex !== newEx)
                changed = true;
            newDerivedAttributes[k] = newEx;
        }
        return changed ? newDerivedAttributes : derivedAttributes;
    };
    External.valuePostTransformFactory = function () {
        var valueSeen = false;
        return new Transform({
            objectMode: true,
            transform: function (d, encoding, callback) {
                valueSeen = true;
                callback(null, { type: 'value', value: d[External.VALUE_NAME] });
            },
            flush: function (callback) {
                callback(null, valueSeen ? null : { type: 'value', value: 0 });
            }
        });
    };
    External.inflateArrays = function (d, attributes) {
        for (var _i = 0, attributes_1 = attributes; _i < attributes_1.length; _i++) {
            var attribute = attributes_1[_i];
            var attributeName = attribute.name;
            if (Array.isArray(d[attributeName])) {
                d[attributeName] = Set.fromJS(d[attributeName]);
            }
        }
    };
    External.postTransformFactory = function (inflaters, attributes, keys, zeroTotalApplies) {
        var valueSeen = false;
        return new Transform({
            objectMode: true,
            transform: function (d, encoding, callback) {
                if (!valueSeen) {
                    this.push({
                        type: 'init',
                        attributes: attributes,
                        keys: keys
                    });
                    valueSeen = true;
                }
                for (var _i = 0, inflaters_1 = inflaters; _i < inflaters_1.length; _i++) {
                    var inflater = inflaters_1[_i];
                    inflater(d);
                }
                External.inflateArrays(d, attributes);
                callback(null, {
                    type: 'datum',
                    datum: d
                });
            },
            flush: function (callback) {
                if (!valueSeen) {
                    this.push({
                        type: 'init',
                        attributes: attributes,
                        keys: null
                    });
                    if (zeroTotalApplies) {
                        this.push({
                            type: 'datum',
                            datum: External.makeZeroDatum(zeroTotalApplies)
                        });
                    }
                }
                callback();
            }
        });
    };
    External.performQueryAndPostTransform = function (queryAndPostTransform, requester, engine, rawQueries, computeContext) {
        if (!requester) {
            return new ReadableError('must have a requester to make queries');
        }
        var query = queryAndPostTransform.query, context = queryAndPostTransform.context, postTransform = queryAndPostTransform.postTransform, next = queryAndPostTransform.next;
        if (!query || !postTransform) {
            return new ReadableError('no query or postTransform');
        }
        context = tslib_1.__assign({}, context, computeContext);
        if (next) {
            var streamNumber_1 = 0;
            var meta_1 = null;
            var numResults_1;
            var resultStream = new StreamConcat({
                objectMode: true,
                next: function () {
                    if (streamNumber_1)
                        query = next(query, numResults_1, meta_1);
                    if (!query)
                        return null;
                    streamNumber_1++;
                    if (rawQueries)
                        rawQueries.push({ engine: engine, query: query });
                    var stream = requester({ query: query, context: context });
                    meta_1 = null;
                    stream.on('meta', function (m) { return meta_1 = m; });
                    numResults_1 = 0;
                    stream.on('data', function () { return numResults_1++; });
                    return stream;
                }
            });
            return pipeWithError(resultStream, postTransform);
        }
        else {
            if (rawQueries)
                rawQueries.push({ engine: engine, query: query });
            return pipeWithError(requester({ query: query, context: context }), postTransform);
        }
    };
    External.buildValueFromStream = function (stream) {
        return new Promise(function (resolve, reject) {
            var pvb = new PlywoodValueBuilder();
            var target = new Writable({
                objectMode: true,
                write: function (chunk, encoding, callback) {
                    pvb.processBit(chunk);
                    callback(null);
                }
            })
                .on('finish', function () {
                resolve(pvb.getValue());
            });
            stream.pipe(target);
            stream.on('error', function (e) {
                stream.unpipe(target);
                reject(e);
            });
        });
    };
    External.valuePromiseToStream = function (valuePromise) {
        var pt = new PassThrough({ objectMode: true });
        valuePromise
            .then(function (v) {
            var i = iteratorFactory(v);
            var bit;
            while (bit = i()) {
                pt.write(bit);
            }
            pt.end();
        })
            .catch(function (e) {
            pt.emit('error', e);
        });
        return pt;
    };
    External.jsToValue = function (parameters, requester) {
        var value = {
            engine: parameters.engine,
            version: parameters.version,
            source: parameters.source,
            suppress: true,
            rollup: parameters.rollup,
            concealBuckets: Boolean(parameters.concealBuckets),
            requester: requester
        };
        if (parameters.attributes) {
            value.attributes = AttributeInfo.fromJSs(parameters.attributes);
        }
        if (parameters.attributeOverrides) {
            value.attributeOverrides = AttributeInfo.fromJSs(parameters.attributeOverrides);
        }
        if (parameters.derivedAttributes) {
            value.derivedAttributes = Expression.expressionLookupFromJS(parameters.derivedAttributes);
        }
        value.filter = parameters.filter ? Expression.fromJS(parameters.filter) : Expression.TRUE;
        return value;
    };
    External.register = function (ex) {
        var engine = ex.engine.replace(/^\w/, function (s) { return s.toLowerCase(); });
        External.classMap[engine] = ex;
    };
    External.getConstructorFor = function (engine) {
        var ClassFn = External.classMap[engine];
        if (!ClassFn)
            throw new Error("unsupported engine '" + engine + "'");
        return ClassFn;
    };
    External.uniteValueExternalsIntoTotal = function (keyExternals) {
        if (keyExternals.length === 0)
            return null;
        var applies = [];
        var baseExternal = null;
        for (var _i = 0, keyExternals_1 = keyExternals; _i < keyExternals_1.length; _i++) {
            var keyExternal = keyExternals_1[_i];
            var key = keyExternal.key;
            var external_1 = keyExternal.external;
            if (!baseExternal)
                baseExternal = external_1;
            applies.push(Expression._.apply(key, new ExternalExpression({ external: external_1 })));
        }
        return keyExternals[0].external.getBase().makeTotal(applies);
    };
    External.fromJS = function (parameters, requester) {
        if (requester === void 0) { requester = null; }
        if (!hasOwnProp(parameters, "engine")) {
            throw new Error("external `engine` must be defined");
        }
        var engine = parameters.engine;
        if (typeof engine !== "string")
            throw new Error("engine must be a string");
        var ClassFn = External.getConstructorFor(engine);
        if (!requester && hasOwnProp(parameters, 'requester')) {
            console.warn("'requester' parameter should be passed as context (2nd argument)");
            requester = parameters.requester;
        }
        if (parameters.source == null) {
            parameters.source = parameters.dataSource != null ? parameters.dataSource : parameters.table;
        }
        return ClassFn.fromJS(parameters, requester);
    };
    External.fromValue = function (parameters) {
        var engine = parameters.engine;
        var ClassFn = External.getConstructorFor(engine);
        return new ClassFn(parameters);
    };
    External.prototype._ensureEngine = function (engine) {
        if (!this.engine) {
            this.engine = engine;
            return;
        }
        if (this.engine !== engine) {
            throw new TypeError("incorrect engine '" + this.engine + "' (needs to be: '" + engine + "')");
        }
    };
    External.prototype._ensureMinVersion = function (minVersion) {
        if (this.version && External.versionLessThan(this.version, minVersion)) {
            throw new Error("only " + this.engine + " versions >= " + minVersion + " are supported");
        }
    };
    External.prototype.valueOf = function () {
        var value = {
            engine: this.engine,
            version: this.version,
            source: this.source,
            rollup: this.rollup,
            mode: this.mode
        };
        if (this.suppress)
            value.suppress = this.suppress;
        if (this.attributes)
            value.attributes = this.attributes;
        if (this.attributeOverrides)
            value.attributeOverrides = this.attributeOverrides;
        if (nonEmptyLookup(this.derivedAttributes))
            value.derivedAttributes = this.derivedAttributes;
        if (this.delegates)
            value.delegates = this.delegates;
        value.concealBuckets = this.concealBuckets;
        if (this.mode !== 'raw' && this.rawAttributes) {
            value.rawAttributes = this.rawAttributes;
        }
        if (this.requester) {
            value.requester = this.requester;
        }
        if (this.dataName) {
            value.dataName = this.dataName;
        }
        value.filter = this.filter;
        if (this.valueExpression) {
            value.valueExpression = this.valueExpression;
        }
        if (this.select) {
            value.select = this.select;
        }
        if (this.split) {
            value.split = this.split;
        }
        if (this.applies) {
            value.applies = this.applies;
        }
        if (this.sort) {
            value.sort = this.sort;
        }
        if (this.limit) {
            value.limit = this.limit;
        }
        if (this.havingFilter) {
            value.havingFilter = this.havingFilter;
        }
        return value;
    };
    External.prototype.toJS = function () {
        var js = {
            engine: this.engine,
            source: this.source
        };
        if (this.version)
            js.version = this.version;
        if (this.rollup)
            js.rollup = true;
        if (this.attributes)
            js.attributes = AttributeInfo.toJSs(this.attributes);
        if (this.attributeOverrides)
            js.attributeOverrides = AttributeInfo.toJSs(this.attributeOverrides);
        if (nonEmptyLookup(this.derivedAttributes))
            js.derivedAttributes = Expression.expressionLookupToJS(this.derivedAttributes);
        if (this.concealBuckets)
            js.concealBuckets = true;
        if (this.mode !== 'raw' && this.rawAttributes)
            js.rawAttributes = AttributeInfo.toJSs(this.rawAttributes);
        if (!this.filter.equals(Expression.TRUE)) {
            js.filter = this.filter.toJS();
        }
        return js;
    };
    External.prototype.toJSON = function () {
        return this.toJS();
    };
    External.prototype.toString = function () {
        var mode = this.mode;
        switch (mode) {
            case 'raw':
                return "ExternalRaw(" + this.filter + ")";
            case 'value':
                return "ExternalValue(" + this.valueExpression + ")";
            case 'total':
                return "ExternalTotal(" + this.applies.length + ")";
            case 'split':
                return "ExternalSplit(" + this.split + ", " + this.applies.length + ")";
            default:
                throw new Error("unknown mode: " + mode);
        }
    };
    External.prototype.equals = function (other) {
        return this.equalBaseAndFilter(other) &&
            immutableLookupsEqual(this.derivedAttributes, other.derivedAttributes) &&
            immutableArraysEqual(this.attributes, other.attributes) &&
            immutableArraysEqual(this.delegates, other.delegates) &&
            this.concealBuckets === other.concealBuckets &&
            Boolean(this.requester) === Boolean(other.requester);
    };
    External.prototype.equalBaseAndFilter = function (other) {
        return this.equalBase(other) &&
            this.filter.equals(other.filter);
    };
    External.prototype.equalBase = function (other) {
        return other instanceof External &&
            this.engine === other.engine &&
            String(this.source) === String(other.source) &&
            this.version === other.version &&
            this.rollup === other.rollup &&
            this.mode === other.mode;
    };
    External.prototype.changeVersion = function (version) {
        var value = this.valueOf();
        value.version = version;
        return External.fromValue(value);
    };
    External.prototype.attachRequester = function (requester) {
        var value = this.valueOf();
        value.requester = requester;
        return External.fromValue(value);
    };
    External.prototype.versionBefore = function (neededVersion) {
        var version = this.version;
        return version && External.versionLessThan(version, neededVersion);
    };
    External.prototype.capability = function (cap) {
        return false;
    };
    External.prototype.getAttributesInfo = function (attributeName) {
        var attributeInfo = NamedArray.get(this.rawAttributes, attributeName);
        if (!attributeInfo)
            throw new Error("could not get attribute info for '" + attributeName + "'");
        return attributeInfo;
    };
    External.prototype.updateAttribute = function (newAttribute) {
        if (!this.attributes)
            return this;
        var value = this.valueOf();
        value.attributes = AttributeInfo.override(value.attributes, [newAttribute]);
        return External.fromValue(value);
    };
    External.prototype.show = function () {
        var value = this.valueOf();
        value.suppress = false;
        return External.fromValue(value);
    };
    External.prototype.hasAttribute = function (name) {
        var _a = this, attributes = _a.attributes, rawAttributes = _a.rawAttributes, derivedAttributes = _a.derivedAttributes;
        if (SimpleArray.find(rawAttributes || attributes, function (a) { return a.name === name; }))
            return true;
        return hasOwnProp(derivedAttributes, name);
    };
    External.prototype.expressionDefined = function (ex) {
        return ex.definedInTypeContext(this.getFullType());
    };
    External.prototype.bucketsConcealed = function (ex) {
        var _this = this;
        return ex.every(function (ex, index, depth, nestDiff) {
            if (nestDiff)
                return true;
            if (ex instanceof RefExpression) {
                var refAttributeInfo = _this.getAttributesInfo(ex.name);
                if (refAttributeInfo && refAttributeInfo.maker instanceof TimeFloorExpression) {
                    return refAttributeInfo.maker.alignsWith(ex);
                }
            }
            else if (ex instanceof ChainableExpression) {
                var refExpression = ex.operand;
                if (refExpression instanceof RefExpression) {
                    var refAttributeInfo = _this.getAttributesInfo(refExpression.name);
                    if (refAttributeInfo && refAttributeInfo.maker instanceof TimeFloorExpression) {
                        return refAttributeInfo.maker.alignsWith(ex);
                    }
                }
            }
            return null;
        });
    };
    External.prototype.addDelegate = function (delegate) {
        var value = this.valueOf();
        if (!value.delegates)
            value.delegates = [];
        value.delegates = value.delegates.concat(delegate);
        return External.fromValue(value);
    };
    External.prototype.getBase = function () {
        var value = this.valueOf();
        value.suppress = true;
        value.mode = 'raw';
        value.dataName = null;
        if (this.mode !== 'raw')
            value.attributes = value.rawAttributes;
        value.rawAttributes = null;
        value.filter = null;
        value.applies = [];
        value.split = null;
        value.sort = null;
        value.limit = null;
        value.delegates = nullMap(value.delegates, function (e) { return e.getBase(); });
        return External.fromValue(value);
    };
    External.prototype.getRaw = function () {
        if (this.mode === 'raw')
            return this;
        var value = this.valueOf();
        value.suppress = true;
        value.mode = 'raw';
        value.dataName = null;
        value.attributes = value.rawAttributes;
        value.rawAttributes = null;
        value.applies = [];
        value.split = null;
        value.sort = null;
        value.limit = null;
        value.delegates = nullMap(value.delegates, function (e) { return e.getRaw(); });
        return External.fromValue(value);
    };
    External.prototype.makeTotal = function (applies) {
        if (this.mode !== 'raw')
            return null;
        if (!applies.length)
            throw new Error('must have applies');
        var externals = [];
        for (var _i = 0, applies_5 = applies; _i < applies_5.length; _i++) {
            var apply = applies_5[_i];
            var applyExpression = apply.expression;
            if (applyExpression instanceof ExternalExpression) {
                externals.push(applyExpression.external);
            }
        }
        var commonFilter = External.getCommonFilterFromExternals(externals);
        var value = this.valueOf();
        value.mode = 'total';
        value.suppress = false;
        value.rawAttributes = value.attributes;
        value.derivedAttributes = External.getMergedDerivedAttributesFromExternals(externals);
        value.filter = commonFilter;
        value.attributes = [];
        value.applies = [];
        value.delegates = nullMap(value.delegates, function (e) { return e.makeTotal(applies); });
        var totalExternal = External.fromValue(value);
        for (var _a = 0, applies_6 = applies; _a < applies_6.length; _a++) {
            var apply = applies_6[_a];
            totalExternal = totalExternal._addApplyExpression(apply);
            if (!totalExternal)
                return null;
        }
        return totalExternal;
    };
    External.prototype.addExpression = function (ex) {
        if (ex instanceof FilterExpression) {
            return this._addFilterExpression(ex);
        }
        if (ex instanceof SelectExpression) {
            return this._addSelectExpression(ex);
        }
        if (ex instanceof SplitExpression) {
            return this._addSplitExpression(ex);
        }
        if (ex instanceof ApplyExpression) {
            return this._addApplyExpression(ex);
        }
        if (ex instanceof SortExpression) {
            return this._addSortExpression(ex);
        }
        if (ex instanceof LimitExpression) {
            return this._addLimitExpression(ex);
        }
        if (ex.isAggregate()) {
            return this._addAggregateExpression(ex);
        }
        return this._addPostAggregateExpression(ex);
    };
    External.prototype._addFilterExpression = function (filter) {
        var expression = filter.expression;
        if (!expression.resolvedWithoutExternals())
            return null;
        if (!this.expressionDefined(expression))
            return null;
        var value = this.valueOf();
        switch (this.mode) {
            case 'raw':
                if (this.concealBuckets && !this.bucketsConcealed(expression))
                    return null;
                if (!this.canHandleFilter(filter))
                    return null;
                if (value.filter.equals(Expression.TRUE)) {
                    value.filter = expression;
                }
                else {
                    value.filter = value.filter.and(expression);
                }
                break;
            case 'split':
                if (this.limit)
                    return null;
                value.havingFilter = value.havingFilter.and(expression).simplify();
                break;
            default:
                return null;
        }
        value.delegates = nullMap(value.delegates, function (e) { return e._addFilterExpression(filter); });
        return External.fromValue(value);
    };
    External.prototype._addSelectExpression = function (selectExpression) {
        var mode = this.mode;
        if (mode !== 'raw' && mode !== 'split')
            return null;
        var datasetType = this.getFullType().datasetType;
        var attributes = selectExpression.attributes;
        for (var _i = 0, attributes_2 = attributes; _i < attributes_2.length; _i++) {
            var attribute = attributes_2[_i];
            if (!datasetType[attribute])
                return null;
        }
        var value = this.valueOf();
        value.suppress = false;
        value.select = selectExpression;
        value.delegates = nullMap(value.delegates, function (e) { return e._addSelectExpression(selectExpression); });
        if (mode === 'split') {
            value.applies = value.applies.filter(function (apply) { return attributes.indexOf(apply.name) !== -1; });
            value.attributes = value.attributes.filter(function (attribute) { return attributes.indexOf(attribute.name) !== -1; });
        }
        return External.fromValue(value);
    };
    External.prototype._addSplitExpression = function (split) {
        if (this.mode !== 'raw')
            return null;
        var splitKeys = split.keys;
        for (var _i = 0, splitKeys_1 = splitKeys; _i < splitKeys_1.length; _i++) {
            var splitKey = splitKeys_1[_i];
            var splitExpression = split.splits[splitKey];
            if (!this.expressionDefined(splitExpression))
                return null;
            if (this.concealBuckets && !this.bucketsConcealed(splitExpression))
                return null;
        }
        var value = this.valueOf();
        value.suppress = false;
        value.mode = 'split';
        value.dataName = split.dataName;
        value.split = split;
        value.rawAttributes = value.attributes;
        value.attributes = split.mapSplits(function (name, expression) { return new AttributeInfo({ name: name, type: Set.unwrapSetType(expression.type) }); });
        value.delegates = nullMap(value.delegates, function (e) { return e._addSplitExpression(split); });
        return External.fromValue(value);
    };
    External.prototype._addApplyExpression = function (apply) {
        var expression = apply.expression;
        if (expression.type === 'DATASET')
            return null;
        if (!expression.resolved())
            return null;
        if (!this.expressionDefined(expression))
            return null;
        var value;
        if (this.mode === 'raw') {
            value = this.valueOf();
            value.derivedAttributes = immutableAdd(value.derivedAttributes, apply.name, apply.expression);
        }
        else {
            if (this.split && this.split.hasKey(apply.name))
                return null;
            var applyExpression = apply.expression;
            if (applyExpression instanceof ExternalExpression) {
                apply = apply.changeExpression(applyExpression.external.valueExpressionWithinFilter(this.filter));
            }
            value = this.valueOf();
            var added = External.normalizeAndAddApply(value, apply);
            value.applies = added.applies;
            value.attributes = added.attributes;
        }
        value.delegates = nullMap(value.delegates, function (e) { return e._addApplyExpression(apply); });
        return External.fromValue(value);
    };
    External.prototype._addSortExpression = function (sort) {
        if (this.limit)
            return null;
        if (!this.canHandleSort(sort))
            return null;
        var value = this.valueOf();
        value.sort = sort;
        value.delegates = nullMap(value.delegates, function (e) { return e._addSortExpression(sort); });
        return External.fromValue(value);
    };
    External.prototype._addLimitExpression = function (limit) {
        var value = this.valueOf();
        value.suppress = false;
        if (!value.limit || limit.value < value.limit.value) {
            value.limit = limit;
        }
        value.delegates = nullMap(value.delegates, function (e) { return e._addLimitExpression(limit); });
        return External.fromValue(value);
    };
    External.prototype._addAggregateExpression = function (aggregate) {
        if (this.mode === 'split') {
            if (aggregate.type !== 'NUMBER')
                return null;
            var valueExpression_1 = $(External.SEGMENT_NAME, 'DATASET').performAction(this.split.getAction());
            this.applies.forEach(function (apply) {
                valueExpression_1 = valueExpression_1.performAction(apply.getAction());
            });
            valueExpression_1 = valueExpression_1.performAction(aggregate);
            var value = this.valueOf();
            value.mode = 'value';
            value.suppress = false;
            value.valueExpression = valueExpression_1;
            value.attributes = null;
            value.delegates = nullMap(value.delegates, function (e) { return e._addAggregateExpression(aggregate); });
            return External.fromValue(value);
        }
        if (this.mode !== 'raw' || this.limit)
            return null;
        if (aggregate instanceof ChainableExpression) {
            if (aggregate instanceof ChainableUnaryExpression) {
                if (!this.expressionDefined(aggregate.expression))
                    return null;
            }
            var value = this.valueOf();
            value.mode = 'value';
            value.suppress = false;
            value.valueExpression = aggregate.changeOperand($(External.SEGMENT_NAME, 'DATASET'));
            value.rawAttributes = value.attributes;
            value.attributes = null;
            value.delegates = nullMap(value.delegates, function (e) { return e._addAggregateExpression(aggregate); });
            return External.fromValue(value);
        }
        else {
            return null;
        }
    };
    External.prototype._addPostAggregateExpression = function (action) {
        if (this.mode !== 'value')
            throw new Error('must be in value mode to call addPostAggregateExpression');
        if (action instanceof ChainableExpression) {
            if (!action.operand.equals(Expression._))
                return null;
            var commonFilter = this.filter;
            var newValueExpression = void 0;
            if (action instanceof ChainableUnaryExpression) {
                var actionExpression = action.expression;
                if (actionExpression instanceof ExternalExpression) {
                    var otherExternal = actionExpression.external;
                    if (!this.equalBase(otherExternal))
                        return null;
                    commonFilter = getCommonFilter(commonFilter, otherExternal.filter);
                    var newExpression = action.changeExpression(otherExternal.valueExpressionWithinFilter(commonFilter));
                    newValueExpression = this.valueExpressionWithinFilter(commonFilter).performAction(newExpression);
                }
                else if (!actionExpression.hasExternal()) {
                    newValueExpression = this.valueExpression.performAction(action);
                }
                else {
                    return null;
                }
            }
            else {
                newValueExpression = this.valueExpression.performAction(action);
            }
            var value = this.valueOf();
            value.valueExpression = newValueExpression;
            value.filter = commonFilter;
            value.delegates = nullMap(value.delegates, function (e) { return e._addPostAggregateExpression(action); });
            return External.fromValue(value);
        }
        else {
            return null;
        }
    };
    External.prototype.prePush = function (ex) {
        if (this.mode !== 'value')
            return null;
        if (ex.type === 'DATASET')
            return null;
        if (!ex.operand.noRefs() || !ex.expression.equals(Expression._))
            return null;
        var value = this.valueOf();
        value.valueExpression = ex.changeExpression(value.valueExpression);
        value.delegates = nullMap(value.delegates, function (e) { return e.prePush(ex); });
        return External.fromValue(value);
    };
    External.prototype.valueExpressionWithinFilter = function (withinFilter) {
        if (this.mode !== 'value')
            return null;
        var extraFilter = filterDiff(this.filter, withinFilter);
        if (!extraFilter)
            throw new Error('not within the segment');
        return External.addExtraFilter(this.valueExpression, extraFilter);
    };
    External.prototype.toValueApply = function () {
        if (this.mode !== 'value')
            return null;
        return Expression._.apply(External.VALUE_NAME, this.valueExpression);
    };
    External.prototype.sortOnLabel = function () {
        var sort = this.sort;
        if (!sort)
            return false;
        var sortOn = sort.expression.name;
        if (!this.split || !this.split.hasKey(sortOn))
            return false;
        var applies = this.applies;
        for (var _i = 0, applies_7 = applies; _i < applies_7.length; _i++) {
            var apply = applies_7[_i];
            if (apply.name === sortOn)
                return false;
        }
        return true;
    };
    External.prototype.getQuerySplit = function () {
        var _this = this;
        return this.split.transformExpressions(function (ex) {
            return _this.inlineDerivedAttributes(ex);
        });
    };
    External.prototype.getQueryFilter = function () {
        var filter = this.inlineDerivedAttributes(this.filter).simplify();
        if (filter instanceof RefExpression && !this.capability('filter-on-attribute')) {
            filter = filter.is(true);
        }
        return filter;
    };
    External.prototype.inlineDerivedAttributes = function (expression) {
        var derivedAttributes = this.derivedAttributes;
        return expression.substitute(function (refEx) {
            if (refEx instanceof RefExpression) {
                var refName = refEx.name;
                return derivedAttributes[refName] || null;
            }
            else {
                return null;
            }
        });
    };
    External.prototype.getSelectedAttributes = function () {
        var _a = this, mode = _a.mode, select = _a.select, attributes = _a.attributes, derivedAttributes = _a.derivedAttributes;
        if (mode === 'raw') {
            for (var k in derivedAttributes) {
                attributes = attributes.concat(new AttributeInfo({ name: k, type: derivedAttributes[k].type }));
            }
        }
        if (!select)
            return attributes;
        var selectAttributes = select.attributes;
        return selectAttributes.map(function (s) { return NamedArray.findByName(attributes, s); });
    };
    External.prototype.getValueType = function () {
        var valueExpression = this.valueExpression;
        if (!valueExpression)
            return null;
        return valueExpression.type;
    };
    External.prototype.addNextExternalToDatum = function (datum) {
        var _a = this, mode = _a.mode, dataName = _a.dataName, split = _a.split;
        if (mode !== 'split')
            throw new Error('must be in split mode to addNextExternalToDatum');
        datum[dataName] = this.getRaw()._addFilterExpression(Expression._.filter(split.filterFromDatum(datum)));
    };
    External.prototype.getDelegate = function () {
        var _a = this, mode = _a.mode, delegates = _a.delegates;
        if (!delegates || !delegates.length || mode === 'raw')
            return null;
        return delegates[0];
    };
    External.prototype.simulateValue = function (lastNode, simulatedQueries, externalForNext) {
        if (externalForNext === void 0) { externalForNext = null; }
        var mode = this.mode;
        if (!externalForNext)
            externalForNext = this;
        var delegate = this.getDelegate();
        if (delegate) {
            return delegate.simulateValue(lastNode, simulatedQueries, externalForNext);
        }
        simulatedQueries.push(this.getQueryAndPostTransform().query);
        if (mode === 'value') {
            var valueExpression = this.valueExpression;
            return getSampleValue(valueExpression.type, valueExpression);
        }
        var keys = null;
        var datum = {};
        if (mode === 'raw') {
            var attributes = this.attributes;
            for (var _i = 0, attributes_3 = attributes; _i < attributes_3.length; _i++) {
                var attribute = attributes_3[_i];
                datum[attribute.name] = getSampleValue(attribute.type, null);
            }
        }
        else {
            if (mode === 'split') {
                this.split.mapSplits(function (name, expression) {
                    datum[name] = getSampleValue(Set.unwrapSetType(expression.type), expression);
                });
                keys = this.split.mapSplits(function (name) { return name; });
            }
            var applies = this.applies;
            for (var _a = 0, applies_8 = applies; _a < applies_8.length; _a++) {
                var apply = applies_8[_a];
                datum[apply.name] = getSampleValue(apply.expression.type, apply.expression);
            }
        }
        if (mode === 'total') {
            return new TotalContainer(datum);
        }
        if (!lastNode && mode === 'split') {
            externalForNext.addNextExternalToDatum(datum);
        }
        return new Dataset({
            keys: keys,
            data: [datum]
        });
    };
    External.prototype.getQueryAndPostTransform = function () {
        throw new Error("can not call getQueryAndPostTransform directly");
    };
    External.prototype.queryValue = function (lastNode, rawQueries, computeContext, externalForNext) {
        if (externalForNext === void 0) { externalForNext = null; }
        var stream = this.queryValueStream(lastNode, rawQueries, computeContext, externalForNext);
        var valuePromise = External.buildValueFromStream(stream);
        if (this.mode === 'total') {
            return valuePromise.then(function (v) {
                return (v instanceof Dataset && v.data.length === 1) ? new TotalContainer(v.data[0]) : v;
            });
        }
        return valuePromise;
    };
    External.prototype.queryBasicValueStream = function (rawQueries, computeContext) {
        var _a = this, engine = _a.engine, requester = _a.requester;
        var queryAndPostTransform;
        try {
            queryAndPostTransform = this.getQueryAndPostTransform();
        }
        catch (e) {
            return new ReadableError(e);
        }
        return External.performQueryAndPostTransform(queryAndPostTransform, requester, engine, rawQueries, computeContext);
    };
    External.prototype.queryValueStream = function (lastNode, rawQueries, env, externalForNext) {
        if (externalForNext === void 0) { externalForNext = null; }
        if (!externalForNext)
            externalForNext = this;
        var delegate = this.getDelegate();
        if (delegate) {
            return delegate.queryValueStream(lastNode, rawQueries, env, externalForNext);
        }
        var finalStream = this.queryBasicValueStream(rawQueries, env);
        if (!lastNode && this.mode === 'split') {
            finalStream = pipeWithError(finalStream, new Transform({
                objectMode: true,
                transform: function (chunk, enc, callback) {
                    if (chunk.type === 'datum')
                        externalForNext.addNextExternalToDatum(chunk.datum);
                    callback(null, chunk);
                }
            }));
        }
        return finalStream;
    };
    External.prototype.needsIntrospect = function () {
        return !this.rawAttributes.length;
    };
    External.prototype.introspect = function (options) {
        var _this = this;
        if (options === void 0) { options = {}; }
        if (!this.requester) {
            return Promise.reject(new Error('must have a requester to introspect'));
        }
        if (!this.version) {
            return this.constructor.getVersion(this.requester).then(function (version) {
                version = External.extractVersion(version);
                if (!version)
                    throw new Error('external version not found, please specify explicitly');
                return _this.changeVersion(version).introspect(options);
            });
        }
        var depth = options.depth || (options.deep ? 'deep' : 'default');
        return this.getIntrospectAttributes(depth)
            .then(function (attributes) {
            var value = _this.valueOf();
            if (value.attributeOverrides) {
                attributes = AttributeInfo.override(attributes, value.attributeOverrides);
            }
            if (value.attributes) {
                attributes = AttributeInfo.override(value.attributes, attributes);
            }
            value.attributes = attributes;
            return External.fromValue(value);
        });
    };
    External.prototype.getRawFullType = function (skipDerived) {
        if (skipDerived === void 0) { skipDerived = false; }
        var _a = this, rawAttributes = _a.rawAttributes, derivedAttributes = _a.derivedAttributes;
        if (!rawAttributes.length)
            throw new Error("dataset has not been introspected");
        var myDatasetType = {};
        for (var _i = 0, rawAttributes_1 = rawAttributes; _i < rawAttributes_1.length; _i++) {
            var rawAttribute = rawAttributes_1[_i];
            var attrName = rawAttribute.name;
            myDatasetType[attrName] = {
                type: rawAttribute.type
            };
        }
        if (!skipDerived) {
            for (var name_2 in derivedAttributes) {
                myDatasetType[name_2] = {
                    type: derivedAttributes[name_2].type
                };
            }
        }
        return {
            type: 'DATASET',
            datasetType: myDatasetType
        };
    };
    External.prototype.getFullType = function () {
        var _a = this, mode = _a.mode, attributes = _a.attributes;
        if (mode === 'value')
            throw new Error('not supported for value mode yet');
        var myFullType = this.getRawFullType();
        if (mode !== 'raw') {
            var splitDatasetType = {};
            splitDatasetType[this.dataName || External.SEGMENT_NAME] = myFullType;
            for (var _i = 0, attributes_4 = attributes; _i < attributes_4.length; _i++) {
                var attribute = attributes_4[_i];
                var attrName = attribute.name;
                splitDatasetType[attrName] = {
                    type: attribute.type
                };
            }
            myFullType = {
                type: 'DATASET',
                datasetType: splitDatasetType
            };
        }
        return myFullType;
    };
    External.type = 'EXTERNAL';
    External.SEGMENT_NAME = '__SEGMENT__';
    External.VALUE_NAME = '__VALUE__';
    External.classMap = {};
    return External;
}());
export { External };
