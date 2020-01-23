'use strict';

var tslib_1 = require('tslib');

var hasOwnProp = require('has-own-prop');
var toArray = require('stream-to-array');

var readableStream = require('readable-stream');
var Readable = readableStream.Readable;
var Writable = readableStream.Writable;
var Transform = readableStream.Transform;
var PassThrough = readableStream.PassThrough;

var immutableClass = require('immutable-class');
var generalEqual = immutableClass.generalEqual;
var generalLookupsEqual = immutableClass.generalLookupsEqual;
var isImmutableClass = immutableClass.isImmutableClass;
var immutableEqual = immutableClass.immutableEqual;
var immutableArraysEqual = immutableClass.immutableArraysEqual;
var immutableLookupsEqual = immutableClass.immutableLookupsEqual;
var SimpleArray = immutableClass.SimpleArray;
var NamedArray = immutableClass.NamedArray;

var Chronoshift = require('chronoshift');
var Timezone = Chronoshift.Timezone;
var Duration = Chronoshift.Duration;
var moment = Chronoshift.moment;
var isDate = Chronoshift.isDate;
var parseISODate = Chronoshift.parseISODate;

var dummyObject = {};

var version = exports.version = '0.21.2';
var promiseWhile = exports.promiseWhile = function(condition, action) {
    var loop = function () {
        if (!condition())
            return Promise.resolve(null);
        return Promise.resolve(action()).then(loop);
    };
    return Promise.resolve(null).then(loop);
}
var ReadableError = (function (_super) {
    tslib_1.__extends(ReadableError, _super);
    function ReadableError(message, options) {
        if (options === void 0) { options = {}; }
        var _this = _super.call(this, options) || this;
        var err = typeof message === 'string' ? new Error(message) : message;
        setTimeout(function () {
            _this.emit('error', err);
        }, 1);
        return _this;
    }
    ;
    ReadableError.prototype._read = function () {
    };
    return ReadableError;
}(Readable));
exports.ReadableError = ReadableError;
var StreamConcat = (function (_super) {
    tslib_1.__extends(StreamConcat, _super);
    function StreamConcat(options) {
        var _this = _super.call(this, options) || this;
        _this.next = options.next;
        _this.currentStream = null;
        _this.streamIndex = 0;
        _this._nextStream();
        return _this;
    }
    ;
    StreamConcat.prototype._nextStream = function () {
        var _this = this;
        this.currentStream = null;
        this.currentStream = this.next();
        if (this.currentStream == null) {
            this.push(null);
        }
        else {
            this.currentStream.pipe(this, { end: false });
            this.currentStream.on('error', function (e) { return _this.emit('error', e); });
            this.currentStream.on('end', this._nextStream.bind(this));
        }
    };
    return StreamConcat;
}(PassThrough));
exports.StreamConcat = StreamConcat;
var repeat = exports.repeat = function(str, times) {
    return new Array(times + 1).join(str);
}
var indentBy = exports.indentBy = function(str, indent) {
    var spaces = repeat(' ', indent);
    return str.split('\n').map(function (x) { return spaces + x; }).join('\n');
}
var dictEqual = exports.dictEqual = function(dictA, dictB) {
    if (dictA === dictB)
        return true;
    if (!dictA !== !dictB)
        return false;
    var keys = Object.keys(dictA);
    if (keys.length !== Object.keys(dictB).length)
        return false;
    for (var _i = 0, keys_1 = keys; _i < keys_1.length; _i++) {
        var key = keys_1[_i];
        if (dictA[key] !== dictB[key])
            return false;
    }
    return true;
}
var shallowCopy = exports.shallowCopy = function(thing) {
    var newThing = {};
    for (var k in thing) {
        if (hasOwnProp(thing, k))
            newThing[k] = thing[k];
    }
    return newThing;
}
var deduplicateSort = exports.deduplicateSort = function(a) {
    a = a.sort();
    var newA = [];
    var last = null;
    for (var _i = 0, a_1 = a; _i < a_1.length; _i++) {
        var v = a_1[_i];
        if (v !== last)
            newA.push(v);
        last = v;
    }
    return newA;
}
var mapLookup = exports.mapLookup = function(thing, fn) {
    var newThing = Object.create(null);
    for (var k in thing) {
        if (hasOwnProp(thing, k))
            newThing[k] = fn(thing[k]);
    }
    return newThing;
}
var emptyLookup = exports.emptyLookup = function(lookup) {
    for (var k in lookup) {
        if (hasOwnProp(lookup, k))
            return false;
    }
    return true;
}
var nonEmptyLookup = exports.nonEmptyLookup = function(lookup) {
    return !emptyLookup(lookup);
}
var clip = exports.clip = function(x) {
    var rx = Math.round(x);
    return Math.abs(x - rx) < 1e-5 ? rx : x;
}
var safeAdd = exports.safeAdd = function(num, delta) {
    var stringDelta = String(delta);
    var dotIndex = stringDelta.indexOf(".");
    if (dotIndex === -1 || stringDelta.length === 18) {
        return num + delta;
    }
    else {
        var scale = Math.pow(10, stringDelta.length - dotIndex - 1);
        return (num * scale + delta * scale) / scale;
    }
}
var safeRange = exports.safeRange = function(num, delta) {
    var stringDelta = String(delta);
    var dotIndex = stringDelta.indexOf(".");
    if (dotIndex === -1 || stringDelta.length === 18) {
        return {
            start: num,
            end: num + delta
        };
    }
    else {
        var scale = Math.pow(10, stringDelta.length - dotIndex - 1);
        num = clip(num * scale) / scale;
        return {
            start: num,
            end: (num * scale + delta * scale) / scale
        };
    }
}
var continuousFloorExpression = exports.continuousFloorExpression = function(variable, floorFn, size, offset) {
    var expr = variable;
    if (offset !== 0) {
        expr = expr + " - " + offset;
    }
    if (offset !== 0 && size !== 1) {
        expr = "(" + expr + ")";
    }
    if (size !== 1) {
        expr = expr + " / " + size;
    }
    expr = floorFn + "(" + expr + ")";
    if (size !== 1) {
        expr = expr + " * " + size;
    }
    if (offset !== 0) {
        expr = expr + " + " + offset;
    }
    return expr;
}
var ExtendableError = (function (_super) {
    tslib_1.__extends(ExtendableError, _super);
    function ExtendableError(message) {
        var _this = _super.call(this, message) || this;
        _this.name = _this.constructor.name;
        _this.message = message;
        if (typeof Error.captureStackTrace === 'function') {
            Error.captureStackTrace(_this, _this.constructor);
        }
        else {
            _this.stack = new Error(message).stack;
        }
        return _this;
    }
    return ExtendableError;
}(Error));
exports.ExtendableError = ExtendableError;
var pluralIfNeeded = exports.pluralIfNeeded = function(n, thing) {
    return n + " " + thing + (n === 1 ? '' : 's');
}
var pipeWithError = exports.pipeWithError = function(src, dest) {
    src.pipe(dest);
    src.on('error', function (e) { return dest.emit('error', e); });
    return dest;
}
var getValueType = exports.getValueType = function(value) {
    var typeofValue = typeof value;
    if (typeofValue === 'object') {
        if (value === null) {
            return 'NULL';
        }
        else if (isDate(value)) {
            return 'TIME';
        }
        else if (hasOwnProp(value, 'start') && hasOwnProp(value, 'end')) {
            if (isDate(value.start) || isDate(value.end))
                return 'TIME_RANGE';
            if (typeof value.start === 'number' || typeof value.end === 'number')
                return 'NUMBER_RANGE';
            if (typeof value.start === 'string' || typeof value.end === 'string')
                return 'STRING_RANGE';
            throw new Error("unrecognizable range");
        }
        else {
            var ctrType = value.constructor.type;
            if (!ctrType) {
                if (value instanceof Expression) {
                    throw new Error("expression used as datum value " + value);
                }
                else {
                    throw new Error("can not have an object without a type: " + JSON.stringify(value));
                }
            }
            if (ctrType === 'SET')
                ctrType += '/' + value.setType;
            return ctrType;
        }
    }
    else {
        if (typeofValue !== 'boolean' && typeofValue !== 'number' && typeofValue !== 'string') {
            throw new TypeError('unsupported JS type ' + typeofValue);
        }
        return typeofValue.toUpperCase();
    }
}
var getFullType = exports.getFullType = function(value) {
    var myType = getValueType(value);
    return myType === 'DATASET' ? value.getFullType() : { type: myType };
}
var getFullTypeFromDatum = exports.getFullTypeFromDatum = function(datum) {
    var datasetType = {};
    for (var k in datum) {
        if (!hasOwnProp(datum, k))
            continue;
        datasetType[k] = getFullType(datum[k]);
    }
    return {
        type: 'DATASET',
        datasetType: datasetType
    };
}
function timeFromJS(v) {
    switch (typeof v) {
        case 'string':
        case 'number':
            return new Date(v);
        case 'object':
            if (v.toISOString)
                return v;
            if (v === null)
                return null;
            if (v.value)
                return new Date(v.value);
            throw new Error("can not interpret " + JSON.stringify(v) + " as TIME");
        default:
            throw new Error("can not interpret " + v + " as TIME");
    }
}
var valueFromJS = exports.valueFromJS = function(v, typeOverride) {
    if (typeOverride === void 0) { typeOverride = null; }
    if (v == null) {
        return null;
    }
    else if (Array.isArray(v)) {
        if (v.length && typeof v[0] !== 'object') {
            return Set.fromJS(v);
        }
        else {
            return Dataset.fromJS(v);
        }
    }
    else {
        var typeofV = typeof v;
        if (typeofV === 'object') {
            switch (typeOverride || v.type) {
                case 'NUMBER':
                    var n = Number(v.value);
                    if (isNaN(n))
                        throw new Error("bad number value '" + v.value + "'");
                    return n;
                case 'NUMBER_RANGE':
                    return NumberRange.fromJS(v);
                case 'STRING_RANGE':
                    return StringRange.fromJS(v);
                case 'TIME':
                    return timeFromJS(v);
                case 'TIME_RANGE':
                    return TimeRange.fromJS(v);
                case 'SET':
                    return Set.fromJS(v);
                case 'DATASET':
                    return Dataset.fromJS(v);
                default:
                    if (String(typeOverride).indexOf('SET') === 0 || Array.isArray(v.elements)) {
                        return Set.fromJS(v);
                    }
                    if (v.toISOString) {
                        return v;
                    }
                    if (typeOverride) {
                        throw new Error("unknown type " + typeOverride + " on " + JSON.stringify(v));
                    }
                    else {
                        throw new Error("can not have an object without a 'type' as a datum value: " + JSON.stringify(v));
                    }
            }
        }
        else if (typeofV === 'string' && typeOverride === 'TIME') {
            return new Date(v);
        }
        else if (typeofV === 'number' && isNaN(v)) {
            return null;
        }
    }
    return v;
}
var valueToJS = exports.valueToJS = function(v) {
    if (v == null) {
        return null;
    }
    else {
        var typeofV = typeof v;
        if (typeofV === 'object') {
            if (v.toISOString) {
                return v;
            }
            else if (v.toJS) {
                return v.toJS();
            }
            else {
                throw new Error("can not convert " + JSON.stringify(v) + " to JS");
            }
        }
        else if (typeofV === 'number' && !isFinite(v)) {
            return String(v);
        }
    }
    return v;
}
var datumHasExternal = exports.datumHasExternal = function(datum) {
    for (var name_1 in datum) {
        var value = datum[name_1];
        if (value instanceof External)
            return true;
        if (value instanceof Dataset && value.hasExternal())
            return true;
    }
    return false;
}
var introspectDatum = exports.introspectDatum = function(datum) {
    var promises = [];
    var newDatum = Object.create(null);
    Object.keys(datum)
        .forEach(function (name) {
        var v = datum[name];
        if (v instanceof External && v.needsIntrospect()) {
            promises.push(v.introspect().then(function (introspectedExternal) {
                newDatum[name] = introspectedExternal;
            }));
        }
        else {
            newDatum[name] = v;
        }
    });
    return Promise.all(promises).then(function () { return newDatum; });
}
var failIfIntrospectNeededInDatum = exports.failIfIntrospectNeededInDatum = function(datum) {
    Object.keys(datum)
        .forEach(function (name) {
        var v = datum[name];
        if (v instanceof External && v.needsIntrospect()) {
            throw new Error('Can not have un-introspected external');
        }
    });
}
var check;
var AttributeInfo = (function () {
    function AttributeInfo(parameters) {
        if (typeof parameters.name !== "string") {
            throw new Error("name must be a string");
        }
        this.name = parameters.name;
        this.type = parameters.type || 'NULL';
        if (!RefExpression.validType(this.type))
            throw new Error("invalid type: " + this.type);
        this.unsplitable = Boolean(parameters.unsplitable);
        this.maker = parameters.maker;
        if (parameters.nativeType)
            this.nativeType = parameters.nativeType;
        if (parameters.cardinality)
            this.cardinality = parameters.cardinality;
        if (parameters.range)
            this.range = parameters.range;
        if (parameters.termsDelegate)
            this.termsDelegate = parameters.termsDelegate;
    }
    AttributeInfo.isAttributeInfo = function (candidate) {
        return candidate instanceof AttributeInfo;
    };
    AttributeInfo.fromJS = function (parameters) {
        if (typeof parameters !== "object") {
            throw new Error("unrecognizable attributeMeta");
        }
        var value = {
            name: parameters.name
        };
        if (parameters.type)
            value.type = parameters.type;
        var nativeType = parameters.nativeType;
        if (!nativeType && hasOwnProp(parameters, 'special')) {
            nativeType = AttributeInfo.NATIVE_TYPE_FROM_SPECIAL[parameters.special];
            value.type = 'NULL';
        }
        value.nativeType = nativeType;
        if (parameters.unsplitable)
            value.unsplitable = true;
        var maker = parameters.maker || parameters.makerAction;
        if (maker)
            value.maker = Expression.fromJS(maker);
        if (parameters.cardinality)
            value.cardinality = parameters.cardinality;
        if (parameters.range)
            value.range = Range.fromJS(parameters.range);
        if (parameters.termsDelegate)
            value.termsDelegate = parameters.termsDelegate;
        return new AttributeInfo(value);
    };
    AttributeInfo.fromJSs = function (attributeJSs) {
        if (!Array.isArray(attributeJSs))
            throw new TypeError("invalid attributeJSs");
        return attributeJSs.map(function (attributeJS) { return AttributeInfo.fromJS(attributeJS); });
    };
    AttributeInfo.toJSs = function (attributes) {
        return attributes.map(function (attribute) { return attribute.toJS(); });
    };
    AttributeInfo.override = function (attributes, attributeOverrides) {
        return NamedArray.overridesByName(attributes, attributeOverrides);
    };
    AttributeInfo.prototype.toString = function () {
        var nativeType = this.nativeType ? "[" + this.nativeType + "]" : '';
        return this.name + "::" + this.type + nativeType;
    };
    AttributeInfo.prototype.valueOf = function () {
        return {
            name: this.name,
            type: this.type,
            unsplitable: this.unsplitable,
            nativeType: this.nativeType,
            maker: this.maker,
            cardinality: this.cardinality,
            range: this.range,
            termsDelegate: this.termsDelegate
        };
    };
    AttributeInfo.prototype.toJS = function () {
        var js = {
            name: this.name,
            type: this.type
        };
        if (this.nativeType)
            js.nativeType = this.nativeType;
        if (this.unsplitable)
            js.unsplitable = true;
        if (this.maker)
            js.maker = this.maker.toJS();
        if (this.cardinality)
            js.cardinality = this.cardinality;
        if (this.range)
            js.range = this.range.toJS();
        if (this.termsDelegate)
            js.termsDelegate = this.termsDelegate;
        return js;
    };
    AttributeInfo.prototype.toJSON = function () {
        return this.toJS();
    };
    AttributeInfo.prototype.equals = function (other) {
        return other instanceof AttributeInfo &&
            this.name === other.name &&
            this.type === other.type &&
            this.nativeType === other.nativeType &&
            this.unsplitable === other.unsplitable &&
            immutableEqual(this.maker, other.maker) &&
            this.cardinality === other.cardinality &&
            immutableEqual(this.range, other.range) &&
            this.termsDelegate === other.termsDelegate;
    };
    AttributeInfo.prototype.dropOriginInfo = function () {
        var value = this.valueOf();
        delete value.maker;
        delete value.nativeType;
        value.unsplitable = false;
        delete value.cardinality;
        delete value.range;
        return new AttributeInfo(value);
    };
    AttributeInfo.prototype.get = function (propertyName) {
        return this[propertyName];
    };
    AttributeInfo.prototype.deepGet = function (propertyName) {
        return this.get(propertyName);
    };
    AttributeInfo.prototype.change = function (propertyName, newValue) {
        var v = this.valueOf();
        if (!hasOwnProp(v, propertyName)) {
            throw new Error("Unknown property: " + propertyName);
        }
        v[propertyName] = newValue;
        return new AttributeInfo(v);
    };
    AttributeInfo.prototype.deepChange = function (propertyName, newValue) {
        return this.change(propertyName, newValue);
    };
    AttributeInfo.prototype.changeType = function (type) {
        var value = this.valueOf();
        value.type = type;
        return new AttributeInfo(value);
    };
    AttributeInfo.prototype.getUnsplitable = function () {
        return this.unsplitable;
    };
    AttributeInfo.prototype.changeUnsplitable = function (unsplitable) {
        var value = this.valueOf();
        value.unsplitable = unsplitable;
        return new AttributeInfo(value);
    };
    AttributeInfo.prototype.changeRange = function (range) {
        var value = this.valueOf();
        value.range = range;
        return new AttributeInfo(value);
    };
    AttributeInfo.NATIVE_TYPE_FROM_SPECIAL = {
        unique: 'hyperUnique',
        theta: 'thetaSketch',
        histogram: 'approximateHistogram'
    };
    return AttributeInfo;
}());
exports.AttributeInfo = AttributeInfo;
check = AttributeInfo;
var BOUNDS_REG_EXP = /^[\[(][\])]$/;
var Range = (function () {
    function Range(start, end, bounds) {
        if (bounds) {
            if (!BOUNDS_REG_EXP.test(bounds)) {
                throw new Error("invalid bounds " + bounds);
            }
        }
        else {
            bounds = Range.DEFAULT_BOUNDS;
        }
        if (start !== null && end !== null && this._endpointEqual(start, end)) {
            if (bounds !== '[]') {
                start = end = this._zeroEndpoint();
            }
            if (bounds === '(]' || bounds === '()')
                this.bounds = '[)';
        }
        else {
            if (start !== null && end !== null && end < start) {
                throw new Error('must have start <= end');
            }
            if (start === null && bounds[0] === '[') {
                bounds = '(' + bounds[1];
            }
            if (end === null && bounds[1] === ']') {
                bounds = bounds[0] + ')';
            }
        }
        this.start = start;
        this.end = end;
        this.bounds = bounds;
    }
    Range.isRange = function (candidate) {
        return candidate instanceof Range;
    };
    Range.isRangeType = function (type) {
        return type && type.indexOf('_RANGE') > 0;
    };
    Range.unwrapRangeType = function (type) {
        if (!type)
            return null;
        return Range.isRangeType(type) ? type.substr(0, type.length - 6) : type;
    };
    Range.register = function (ctr) {
        var rangeType = ctr.type.replace('_RANGE', '').toLowerCase();
        Range.classMap[rangeType] = ctr;
    };
    Range.fromJS = function (parameters) {
        var ctr;
        if (typeof parameters.start === 'number' || typeof parameters.end === 'number') {
            ctr = 'number';
        }
        else if (typeof parameters.start === 'string' || typeof parameters.end === 'string') {
            ctr = 'string';
        }
        else {
            ctr = 'time';
        }
        return Range.classMap[ctr].fromJS(parameters);
    };
    Range.prototype._zeroEndpoint = function () {
        return 0;
    };
    Range.prototype._endpointEqual = function (a, b) {
        return a === b;
    };
    Range.prototype._endpointToString = function (a, tz) {
        return String(a);
    };
    Range.prototype._equalsHelper = function (other) {
        return Boolean(other) &&
            this.bounds === other.bounds &&
            this._endpointEqual(this.start, other.start) &&
            this._endpointEqual(this.end, other.end);
    };
    Range.prototype.toJSON = function () {
        return this.toJS();
    };
    Range.prototype.toString = function (tz) {
        var bounds = this.bounds;
        return '[' + (bounds[0] === '(' ? '~' : '') + this._endpointToString(this.start, tz) + ',' + this._endpointToString(this.end, tz) + (bounds[1] === ')' ? '' : '!') + ']';
    };
    Range.prototype.compare = function (other) {
        var myStart = this.start;
        var otherStart = other.start;
        return myStart < otherStart ? -1 : (otherStart < myStart ? 1 : 0);
    };
    Range.prototype.openStart = function () {
        return this.bounds[0] === '(';
    };
    Range.prototype.openEnd = function () {
        return this.bounds[1] === ')';
    };
    Range.prototype.empty = function () {
        return this._endpointEqual(this.start, this.end) && this.bounds === '[)';
    };
    Range.prototype.degenerate = function () {
        return this._endpointEqual(this.start, this.end) && this.bounds === '[]';
    };
    Range.prototype.contains = function (val) {
        if (val instanceof Range) {
            var valStart = val.start;
            var valEnd = val.end;
            var valBound = val.bounds;
            if (valBound[0] === '[') {
                if (!this.containsValue(valStart))
                    return false;
            }
            else {
                if (!this.containsValue(valStart) && valStart.valueOf() !== this.start.valueOf())
                    return false;
            }
            if (valBound[1] === ']') {
                if (!this.containsValue(valEnd))
                    return false;
            }
            else {
                if (!this.containsValue(valEnd) && valEnd.valueOf() !== this.end.valueOf())
                    return false;
            }
            return true;
        }
        else {
            return this.containsValue(val);
        }
    };
    Range.prototype.validMemberType = function (val) {
        return typeof val === 'number';
    };
    Range.prototype.containsValue = function (val) {
        if (val === null)
            return false;
        val = val.valueOf();
        if (!this.validMemberType(val))
            return false;
        var start = this.start;
        var end = this.end;
        var bounds = this.bounds;
        if (bounds[0] === '[') {
            if (val < start)
                return false;
        }
        else {
            if (start !== null && val <= start)
                return false;
        }
        if (bounds[1] === ']') {
            if (end < val)
                return false;
        }
        else {
            if (end !== null && end <= val)
                return false;
        }
        return true;
    };
    Range.prototype.intersects = function (other) {
        return this.containsValue(other.start)
            || this.containsValue(other.end)
            || other.containsValue(this.start)
            || other.containsValue(this.end)
            || this._equalsHelper(other);
    };
    Range.prototype.adjacent = function (other) {
        return (this._endpointEqual(this.end, other.start) && this.openEnd() !== other.openStart())
            || (this._endpointEqual(this.start, other.end) && this.openStart() !== other.openEnd());
    };
    Range.prototype.mergeable = function (other) {
        return this.intersects(other) || this.adjacent(other);
    };
    Range.prototype.union = function (other) {
        if (!this.mergeable(other))
            return null;
        return this.extend(other);
    };
    Range.prototype.extent = function () {
        return this;
    };
    Range.prototype.extend = function (other) {
        var thisStart = this.start;
        var thisEnd = this.end;
        var otherStart = other.start;
        var otherEnd = other.end;
        var start;
        var startBound;
        if (thisStart === null || otherStart === null) {
            start = null;
            startBound = '(';
        }
        else if (thisStart < otherStart) {
            start = thisStart;
            startBound = this.bounds[0];
        }
        else {
            start = otherStart;
            startBound = other.bounds[0];
        }
        var end;
        var endBound;
        if (thisEnd === null || otherEnd === null) {
            end = null;
            endBound = ')';
        }
        else if (thisEnd < otherEnd) {
            end = otherEnd;
            endBound = other.bounds[1];
        }
        else {
            end = thisEnd;
            endBound = this.bounds[1];
        }
        return new this.constructor({ start: start, end: end, bounds: startBound + endBound });
    };
    Range.prototype.intersect = function (other) {
        if (!this.mergeable(other))
            return null;
        var thisStart = this.start;
        var thisEnd = this.end;
        var otherStart = other.start;
        var otherEnd = other.end;
        var start;
        var startBound;
        if (thisStart === null || otherStart === null) {
            if (otherStart === null) {
                start = thisStart;
                startBound = this.bounds[0];
            }
            else {
                start = otherStart;
                startBound = other.bounds[0];
            }
        }
        else if (otherStart < thisStart) {
            start = thisStart;
            startBound = this.bounds[0];
        }
        else {
            start = otherStart;
            startBound = other.bounds[0];
        }
        var end;
        var endBound;
        if (thisEnd === null || otherEnd === null) {
            if (thisEnd == null) {
                end = otherEnd;
                endBound = other.bounds[1];
            }
            else {
                end = thisEnd;
                endBound = this.bounds[1];
            }
        }
        else if (otherEnd < thisEnd) {
            end = otherEnd;
            endBound = other.bounds[1];
        }
        else {
            end = thisEnd;
            endBound = this.bounds[1];
        }
        return new this.constructor({ start: start, end: end, bounds: startBound + endBound });
    };
    Range.prototype.isFinite = function () {
        return this.start !== null && this.end !== null;
    };
    Range.DEFAULT_BOUNDS = '[)';
    Range.classMap = {};
    return Range;
}());
exports.Range = Range;
function finiteOrNull(n) {
    return (isNaN(n) || isFinite(n)) ? n : null;
}
var check;
var NumberRange = (function (_super) {
    tslib_1.__extends(NumberRange, _super);
    function NumberRange(parameters) {
        var _this = this;
        if (isNaN(parameters.start))
            throw new TypeError('`start` must be a number');
        if (isNaN(parameters.end))
            throw new TypeError('`end` must be a number');
        _this = _super.call(this, parameters.start, parameters.end, parameters.bounds) || this;
        return _this;
    }
    NumberRange.isNumberRange = function (candidate) {
        return candidate instanceof NumberRange;
    };
    NumberRange.numberBucket = function (num, size, offset) {
        var start = Math.floor((num - offset) / size) * size + offset;
        return new NumberRange({
            start: start,
            end: start + size,
            bounds: Range.DEFAULT_BOUNDS
        });
    };
    NumberRange.fromNumber = function (n) {
        return new NumberRange({ start: n, end: n, bounds: '[]' });
    };
    NumberRange.fromJS = function (parameters) {
        if (typeof parameters !== "object") {
            throw new Error("unrecognizable numberRange");
        }
        var start = parameters.start;
        var end = parameters.end;
        return new NumberRange({
            start: start === null ? null : finiteOrNull(Number(start)),
            end: end === null ? null : finiteOrNull(Number(end)),
            bounds: parameters.bounds
        });
    };
    NumberRange.prototype.valueOf = function () {
        return {
            start: this.start,
            end: this.end,
            bounds: this.bounds
        };
    };
    NumberRange.prototype.toJS = function () {
        var js = {
            start: this.start,
            end: this.end
        };
        if (this.bounds !== Range.DEFAULT_BOUNDS)
            js.bounds = this.bounds;
        return js;
    };
    NumberRange.prototype.equals = function (other) {
        return other instanceof NumberRange && this._equalsHelper(other);
    };
    NumberRange.prototype.midpoint = function () {
        return (this.start + this.end) / 2;
    };
    NumberRange.prototype.rebaseOnStart = function (newStart) {
        var _a = this, start = _a.start, end = _a.end, bounds = _a.bounds;
        if (!start)
            return this;
        return new NumberRange({
            start: newStart,
            end: end ? end - start + newStart : end,
            bounds: bounds
        });
    };
    NumberRange.type = 'NUMBER_RANGE';
    return NumberRange;
}(Range));
exports.NumberRange = NumberRange;
check = NumberRange;
Range.register(NumberRange);
function toDate(date, name) {
    if (date === null)
        return null;
    var typeofDate = typeof date;
    if (typeofDate === "undefined")
        throw new TypeError("timeRange must have a " + name);
    if (typeofDate === 'string') {
        var parsedDate = parseISODate(date, Expression.defaultParserTimezone);
        if (!parsedDate)
            throw new Error("could not parse '" + date + "' as date");
        date = parsedDate;
    }
    else if (typeofDate === 'number') {
        date = new Date(date);
    }
    if (!date.getDay)
        throw new TypeError("timeRange must have a " + name + " that is a Date");
    return date;
}
var START_OF_TIME = "1000";
var END_OF_TIME = "3000";
function dateToIntervalPart(date) {
    return date.toISOString()
        .replace('.000Z', 'Z')
        .replace(':00Z', 'Z')
        .replace(':00Z', 'Z');
}
var check;
var TimeRange = (function (_super) {
    tslib_1.__extends(TimeRange, _super);
    function TimeRange(parameters) {
        return _super.call(this, parameters.start, parameters.end, parameters.bounds) || this;
    }
    TimeRange.isTimeRange = function (candidate) {
        return candidate instanceof TimeRange;
    };
    TimeRange.intervalFromDate = function (date) {
        return dateToIntervalPart(date) + '/' + dateToIntervalPart(new Date(date.valueOf() + 1));
    };
    TimeRange.timeBucket = function (date, duration, timezone) {
        if (!date)
            return null;
        var start = duration.floor(date, timezone);
        return new TimeRange({
            start: start,
            end: duration.shift(start, timezone, 1),
            bounds: Range.DEFAULT_BOUNDS
        });
    };
    TimeRange.fromTime = function (t) {
        return new TimeRange({ start: t, end: t, bounds: '[]' });
    };
    TimeRange.fromJS = function (parameters) {
        if (typeof parameters !== "object") {
            throw new Error("unrecognizable timeRange");
        }
        return new TimeRange({
            start: toDate(parameters.start, 'start'),
            end: toDate(parameters.end, 'end'),
            bounds: parameters.bounds
        });
    };
    TimeRange.prototype._zeroEndpoint = function () {
        return new Date(0);
    };
    TimeRange.prototype._endpointEqual = function (a, b) {
        if (a === null) {
            return b === null;
        }
        else {
            return b !== null && a.valueOf() === b.valueOf();
        }
    };
    TimeRange.prototype._endpointToString = function (a, tz) {
        return a ? Timezone.formatDateWithTimezone(a, tz) : 'null';
    };
    TimeRange.prototype.valueOf = function () {
        return {
            start: this.start,
            end: this.end,
            bounds: this.bounds
        };
    };
    TimeRange.prototype.toJS = function () {
        var js = {
            start: this.start,
            end: this.end
        };
        if (this.bounds !== Range.DEFAULT_BOUNDS)
            js.bounds = this.bounds;
        return js;
    };
    TimeRange.prototype.equals = function (other) {
        return other instanceof TimeRange && this._equalsHelper(other);
    };
    TimeRange.prototype.toInterval = function () {
        var _a = this, start = _a.start, end = _a.end, bounds = _a.bounds;
        var interval = [START_OF_TIME, END_OF_TIME];
        if (start) {
            if (bounds[0] === '(')
                start = new Date(start.valueOf() + 1);
            interval[0] = dateToIntervalPart(start);
        }
        if (end) {
            if (bounds[1] === ']')
                end = new Date(end.valueOf() + 1);
            interval[1] = dateToIntervalPart(end);
        }
        return interval.join("/");
    };
    TimeRange.prototype.midpoint = function () {
        return new Date((this.start.valueOf() + this.end.valueOf()) / 2);
    };
    TimeRange.prototype.changeToNumber = function () {
        return new NumberRange({
            bounds: this.bounds,
            start: this.start ? this.start.valueOf() : null,
            end: this.end ? this.end.valueOf() : null
        });
    };
    TimeRange.prototype.isAligned = function (duration, timezone) {
        var _a = this, start = _a.start, end = _a.end;
        return (!start || duration.isAligned(start, timezone)) && (!end || duration.isAligned(end, timezone));
    };
    TimeRange.prototype.rebaseOnStart = function (newStart) {
        var _a = this, start = _a.start, end = _a.end, bounds = _a.bounds;
        if (!start)
            return this;
        return new TimeRange({
            start: newStart,
            end: end ? new Date(end.valueOf() - start.valueOf() + newStart.valueOf()) : end,
            bounds: bounds
        });
    };
    TimeRange.type = 'TIME_RANGE';
    return TimeRange;
}(Range));
exports.TimeRange = TimeRange;
check = TimeRange;
Range.register(TimeRange);
var check;
var StringRange = (function (_super) {
    tslib_1.__extends(StringRange, _super);
    function StringRange(parameters) {
        var _this = this;
        var start = parameters.start, end = parameters.end;
        if (typeof start !== 'string' && start !== null)
            throw new TypeError('`start` must be a string');
        if (typeof end !== 'string' && end !== null)
            throw new TypeError('`end` must be a string');
        _this = _super.call(this, start, end, parameters.bounds) || this;
        return _this;
    }
    StringRange.isStringRange = function (candidate) {
        return candidate instanceof StringRange;
    };
    StringRange.fromString = function (s) {
        return new StringRange({ start: s, end: s, bounds: '[]' });
    };
    StringRange.fromJS = function (parameters) {
        if (typeof parameters !== "object") {
            throw new Error("unrecognizable StringRange");
        }
        var start = parameters.start;
        var end = parameters.end;
        var bounds = parameters.bounds;
        return new StringRange({
            start: start, end: end, bounds: bounds
        });
    };
    StringRange.prototype.valueOf = function () {
        return {
            start: this.start,
            end: this.end,
            bounds: this.bounds
        };
    };
    StringRange.prototype.toJS = function () {
        var js = {
            start: this.start,
            end: this.end
        };
        if (this.bounds !== Range.DEFAULT_BOUNDS)
            js.bounds = this.bounds;
        return js;
    };
    StringRange.prototype.equals = function (other) {
        return other instanceof StringRange && this._equalsHelper(other);
    };
    StringRange.prototype.midpoint = function () {
        throw new Error("midpoint not supported in string range");
    };
    StringRange.prototype._zeroEndpoint = function () {
        return "";
    };
    StringRange.prototype.validMemberType = function (val) {
        return typeof val === 'string';
    };
    StringRange.type = 'STRING_RANGE';
    return StringRange;
}(Range));
exports.StringRange = StringRange;
check = StringRange;
Range.register(StringRange);
function dateString(date) {
    return date.toISOString();
}
function arrayFromJS(xs, setType) {
    return xs.map(function (x) { return valueFromJS(x, setType); });
}
var typeUpgrades = {
    'NUMBER': 'NUMBER_RANGE',
    'TIME': 'TIME_RANGE',
    'STRING': 'STRING_RANGE'
};
var check;
var Set = (function () {
    function Set(parameters) {
        var setType = parameters.setType;
        this.setType = setType;
        var keyFn = setType === 'TIME' ? dateString : String;
        this.keyFn = keyFn;
        var elements = parameters.elements;
        var newElements = null;
        var hash = Object.create(null);
        for (var i = 0; i < elements.length; i++) {
            var element = elements[i];
            var key = keyFn(element);
            if (hash[key]) {
                if (!newElements)
                    newElements = elements.slice(0, i);
            }
            else {
                hash[key] = element;
                if (newElements)
                    newElements.push(element);
            }
        }
        if (newElements) {
            elements = newElements;
        }
        this.elements = elements;
        this.hash = hash;
    }
    Set.unifyElements = function (elements) {
        var newElements = Object.create(null);
        for (var _i = 0, elements_1 = elements; _i < elements_1.length; _i++) {
            var accumulator = elements_1[_i];
            var newElementsKeys_2 = Object.keys(newElements);
            for (var _a = 0, newElementsKeys_1 = newElementsKeys_2; _a < newElementsKeys_1.length; _a++) {
                var newElementsKey = newElementsKeys_1[_a];
                var newElement = newElements[newElementsKey];
                var unionElement = accumulator.union(newElement);
                if (unionElement) {
                    accumulator = unionElement;
                    delete newElements[newElementsKey];
                }
            }
            newElements[accumulator.toString()] = accumulator;
        }
        var newElementsKeys = Object.keys(newElements);
        return newElementsKeys.length < elements.length ? newElementsKeys.map(function (k) { return newElements[k]; }) : elements;
    };
    Set.intersectElements = function (elements1, elements2) {
        var newElements = [];
        for (var _i = 0, elements1_1 = elements1; _i < elements1_1.length; _i++) {
            var element1 = elements1_1[_i];
            for (var _a = 0, elements2_1 = elements2; _a < elements2_1.length; _a++) {
                var element2 = elements2_1[_a];
                var intersect = element1.intersect(element2);
                if (intersect)
                    newElements.push(intersect);
            }
        }
        return newElements;
    };
    Set.isSet = function (candidate) {
        return candidate instanceof Set;
    };
    Set.isAtomicType = function (type) {
        return type && type !== 'NULL' && type.indexOf('SET/') === -1;
    };
    Set.isSetType = function (type) {
        return type && type.indexOf('SET/') === 0;
    };
    Set.wrapSetType = function (type) {
        if (!type)
            return null;
        return Set.isSetType(type) ? type : ('SET/' + type);
    };
    Set.unwrapSetType = function (type) {
        if (!type)
            return null;
        return Set.isSetType(type) ? type.substr(4) : type;
    };
    Set.cartesianProductOf = function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        return args.reduce(function (a, b) {
            return [].concat.apply([], a.map(function (x) {
                return b.map(function (y) {
                    return x.concat([y]);
                });
            }));
        }, [[]]);
    };
    Set.crossBinary = function (as, bs, fn) {
        if (as instanceof Set || bs instanceof Set) {
            var aElements = as instanceof Set ? as.elements : [as];
            var bElements = bs instanceof Set ? bs.elements : [bs];
            var cp = Set.cartesianProductOf(aElements, bElements);
            return Set.fromJS(cp.map(function (v) { return fn(v[0], v[1]); }));
        }
        else {
            return fn(as, bs);
        }
    };
    Set.crossBinaryBoolean = function (as, bs, fn) {
        if (as instanceof Set || bs instanceof Set) {
            var aElements = as instanceof Set ? as.elements : [as];
            var bElements = bs instanceof Set ? bs.elements : [bs];
            var cp = Set.cartesianProductOf(aElements, bElements);
            return cp.some(function (v) { return fn(v[0], v[1]); });
        }
        else {
            return fn(as, bs);
        }
    };
    Set.crossUnary = function (as, fn) {
        if (as instanceof Set) {
            var aElements = as instanceof Set ? as.elements : [as];
            return Set.fromJS(aElements.map(function (a) { return fn(a); }));
        }
        else {
            return fn(as);
        }
    };
    Set.crossUnaryBoolean = function (as, fn) {
        if (as instanceof Set) {
            var aElements = as instanceof Set ? as.elements : [as];
            return aElements.some(function (a) { return fn(a); });
        }
        else {
            return fn(as);
        }
    };
    Set.convertToSet = function (thing) {
        var thingType = getValueType(thing);
        if (Set.isSetType(thingType))
            return thing;
        return Set.fromJS({ setType: thingType, elements: [thing] });
    };
    Set.unionCover = function (a, b) {
        var aSet = Set.convertToSet(a);
        var bSet = Set.convertToSet(b);
        var aSetType = aSet.setType;
        var bSetType = bSet.setType;
        if (typeUpgrades[aSetType] === bSetType) {
            aSet = aSet.upgradeType();
        }
        else if (typeUpgrades[bSetType] === aSetType) {
            bSet = bSet.upgradeType();
        }
        else if (aSetType !== bSetType) {
            return null;
        }
        return aSet.union(bSet).simplifyCover();
    };
    Set.intersectCover = function (a, b) {
        var aSet = Set.convertToSet(a);
        var bSet = Set.convertToSet(b);
        var aSetType = aSet.setType;
        var bSetType = bSet.setType;
        if (typeUpgrades[aSetType] === bSetType) {
            aSet = aSet.upgradeType();
        }
        else if (typeUpgrades[bSetType] === aSetType) {
            bSet = bSet.upgradeType();
        }
        else if (aSetType !== bSetType) {
            return null;
        }
        return aSet.intersect(bSet).simplifyCover();
    };
    Set.fromPlywoodValue = function (pv) {
        return pv instanceof Set ? pv : Set.fromJS([pv]);
    };
    Set.fromJS = function (parameters) {
        if (Array.isArray(parameters)) {
            parameters = { elements: parameters };
        }
        if (typeof parameters !== "object") {
            throw new Error("unrecognizable set");
        }
        var setType = parameters.setType;
        var elements = parameters.elements;
        if (!setType) {
            setType = getValueType(elements.length ? elements[0] : null);
            if (setType === 'NULL' && elements.length > 1)
                setType = getValueType(elements[1]);
        }
        return new Set({
            setType: setType,
            elements: arrayFromJS(elements, setType)
        });
    };
    Set.prototype.valueOf = function () {
        return {
            setType: this.setType,
            elements: this.elements
        };
    };
    Set.prototype.toJS = function () {
        return {
            setType: this.setType,
            elements: this.elements.map(valueToJS)
        };
    };
    Set.prototype.toJSON = function () {
        return this.toJS();
    };
    Set.prototype.toString = function (tz) {
        var setType = this.setType;
        var stringFn = null;
        if (setType === "NULL")
            return "null";
        if (setType === "TIME_RANGE") {
            stringFn = function (e) { return e ? e.toString(tz) : 'null'; };
        }
        else if (setType === "TIME") {
            stringFn = function (e) { return e ? Timezone.formatDateWithTimezone(e, tz) : 'null'; };
        }
        else {
            stringFn = String;
        }
        return "" + this.elements.map(stringFn).join(", ");
    };
    Set.prototype.equals = function (other) {
        return other instanceof Set &&
            this.setType === other.setType &&
            this.elements.length === other.elements.length &&
            this.elements.slice().sort().join('') === other.elements.slice().sort().join('');
    };
    Set.prototype.changeElements = function (elements) {
        if (this.elements === elements)
            return this;
        var value = this.valueOf();
        value.elements = elements;
        return new Set(value);
    };
    Set.prototype.cardinality = function () {
        return this.size();
    };
    Set.prototype.size = function () {
        return this.elements.length;
    };
    Set.prototype.empty = function () {
        return this.elements.length === 0;
    };
    Set.prototype.isNullSet = function () {
        return this.setType === 'NULL';
    };
    Set.prototype.unifyElements = function () {
        return Range.isRangeType(this.setType) ? this.changeElements(Set.unifyElements(this.elements)) : this;
    };
    Set.prototype.simplifyCover = function () {
        var simpleSet = this.unifyElements().downgradeType();
        var simpleSetElements = simpleSet.elements;
        return simpleSetElements.length === 1 ? simpleSetElements[0] : simpleSet;
    };
    Set.prototype.getType = function () {
        return ('SET/' + this.setType);
    };
    Set.prototype.upgradeType = function () {
        if (this.setType === 'NUMBER') {
            return Set.fromJS({
                setType: 'NUMBER_RANGE',
                elements: this.elements.map(NumberRange.fromNumber)
            });
        }
        else if (this.setType === 'TIME') {
            return Set.fromJS({
                setType: 'TIME_RANGE',
                elements: this.elements.map(TimeRange.fromTime)
            });
        }
        else if (this.setType === 'STRING') {
            return Set.fromJS({
                setType: 'STRING_RANGE',
                elements: this.elements.map(StringRange.fromString)
            });
        }
        else {
            return this;
        }
    };
    Set.prototype.downgradeType = function () {
        if (!Range.isRangeType(this.setType))
            return this;
        var elements = this.elements;
        var simpleElements = [];
        for (var _i = 0, elements_2 = elements; _i < elements_2.length; _i++) {
            var element = elements_2[_i];
            if (element.degenerate()) {
                simpleElements.push(element.start);
            }
            else {
                return this;
            }
        }
        return Set.fromJS(simpleElements);
    };
    Set.prototype.extent = function () {
        var setType = this.setType;
        if (hasOwnProp(typeUpgrades, setType)) {
            return this.upgradeType().extent();
        }
        if (!Range.isRangeType(setType))
            return null;
        var elements = this.elements;
        var extent = elements[0] || null;
        for (var i = 1; i < elements.length; i++) {
            extent = extent.extend(elements[i]);
        }
        return extent;
    };
    Set.prototype.union = function (other) {
        if (this.empty())
            return other;
        if (other.empty())
            return this;
        if (this.setType !== other.setType)
            throw new TypeError("can not union sets of different types");
        return this.changeElements(this.elements.concat(other.elements)).unifyElements();
    };
    Set.prototype.intersect = function (other) {
        if (this.empty() || other.empty())
            return Set.EMPTY;
        var setType = this.setType;
        if (this.setType !== other.setType) {
            throw new TypeError("can not intersect sets of different types");
        }
        var thisElements = this.elements;
        var newElements;
        if (setType === 'NUMBER_RANGE' || setType === 'TIME_RANGE' || setType === 'STRING_RANGE') {
            var otherElements = other.elements;
            newElements = Set.intersectElements(thisElements, otherElements);
        }
        else {
            newElements = [];
            for (var _i = 0, thisElements_1 = thisElements; _i < thisElements_1.length; _i++) {
                var el = thisElements_1[_i];
                if (!other.contains(el))
                    continue;
                newElements.push(el);
            }
        }
        return this.changeElements(newElements);
    };
    Set.prototype.overlap = function (other) {
        if (this.empty() || other.empty())
            return false;
        if (this.setType !== other.setType) {
            throw new TypeError("can determine overlap sets of different types");
        }
        var thisElements = this.elements;
        for (var _i = 0, thisElements_2 = thisElements; _i < thisElements_2.length; _i++) {
            var el = thisElements_2[_i];
            if (!other.contains(el))
                continue;
            return true;
        }
        return false;
    };
    Set.prototype.has = function (value) {
        var key = this.keyFn(value);
        return hasOwnProp(this.hash, key) && generalEqual(this.hash[key], value);
    };
    Set.prototype.contains = function (value) {
        var _this = this;
        if (value instanceof Set) {
            return value.elements.every(function (element) { return _this.contains(element); });
        }
        if (Range.isRangeType(this.setType)) {
            if (value instanceof Range && this.has(value))
                return true;
            return this.elements.some(function (element) { return element.contains(value); });
        }
        else {
            return this.has(value);
        }
    };
    Set.prototype.add = function (value) {
        var setType = this.setType;
        var valueType = getValueType(value);
        if (setType === 'NULL')
            setType = valueType;
        if (valueType !== 'NULL' && setType !== valueType)
            throw new Error('value type must match');
        if (this.contains(value))
            return this;
        return new Set({
            setType: setType,
            elements: this.elements.concat([value])
        });
    };
    Set.prototype.remove = function (value) {
        if (!this.contains(value))
            return this;
        var keyFn = this.keyFn;
        var key = keyFn(value);
        return new Set({
            setType: this.setType,
            elements: this.elements.filter(function (element) { return keyFn(element) !== key; })
        });
    };
    Set.prototype.toggle = function (value) {
        return this.contains(value) ? this.remove(value) : this.add(value);
    };
    Set.type = 'SET';
    return Set;
}());
exports.Set = Set;
check = Set;
Set.EMPTY = Set.fromJS([]);
var fillExpressionExternalAlteration = exports.fillExpressionExternalAlteration = function(alteration, filler) {
    for (var k in alteration) {
        var thing = alteration[k];
        if (Array.isArray(thing)) {
            fillDatasetExternalAlterations(thing, filler);
        }
        else {
            thing.result = filler(thing.external, Boolean(thing.terminal));
        }
    }
}
var sizeOfExpressionExternalAlteration = exports.sizeOfExpressionExternalAlteration = function(alteration) {
    var count = 0;
    for (var k in alteration) {
        var thing = alteration[k];
        if (Array.isArray(thing)) {
            count += sizeOfDatasetExternalAlterations(thing);
        }
        else {
            count++;
        }
    }
    return count;
}
var fillDatasetExternalAlterations = exports.fillDatasetExternalAlterations = function(alterations, filler) {
    for (var _i = 0, alterations_1 = alterations; _i < alterations_1.length; _i++) {
        var alteration = alterations_1[_i];
        if (alteration.external) {
            alteration.result = filler(alteration.external, alteration.terminal);
        }
        else if (alteration.datasetAlterations) {
            fillDatasetExternalAlterations(alteration.datasetAlterations, filler);
        }
        else if (alteration.expressionAlterations) {
            fillExpressionExternalAlteration(alteration.expressionAlterations, filler);
        }
        else {
            throw new Error('fell through');
        }
    }
}
var sizeOfDatasetExternalAlterations = exports.sizeOfDatasetExternalAlterations = function(alterations) {
    var count = 0;
    for (var _i = 0, alterations_2 = alterations; _i < alterations_2.length; _i++) {
        var alteration = alterations_2[_i];
        if (alteration.external) {
            count += 1;
        }
        else if (alteration.datasetAlterations) {
            count += sizeOfDatasetExternalAlterations(alteration.datasetAlterations);
        }
        else if (alteration.expressionAlterations) {
            count += sizeOfExpressionExternalAlteration(alteration.expressionAlterations);
        }
        else {
            throw new Error('fell through');
        }
    }
    return count;
}
var directionFns = {
    ascending: function (a, b) {
        if (a == null) {
            return b == null ? 0 : -1;
        }
        else {
            if (a.compare)
                return a.compare(b);
            if (b == null)
                return 1;
        }
        return a < b ? -1 : a > b ? 1 : a >= b ? 0 : NaN;
    },
    descending: function (a, b) {
        if (b == null) {
            return a == null ? 0 : -1;
        }
        else {
            if (b.compare)
                return b.compare(a);
            if (a == null)
                return 1;
        }
        return b < a ? -1 : b > a ? 1 : b >= a ? 0 : NaN;
    }
};
function removeLineBreaks(v) {
    return v.replace(/(?:\r\n|\r|\n)/g, ' ');
}
var typeOrder = {
    'NULL': 0,
    'TIME': 1,
    'TIME_RANGE': 2,
    'SET/TIME': 3,
    'SET/TIME_RANGE': 4,
    'STRING': 5,
    'SET/STRING': 6,
    'BOOLEAN': 7,
    'NUMBER': 8,
    'NUMBER_RANGE': 9,
    'SET/NUMBER': 10,
    'SET/NUMBER_RANGE': 11,
    'DATASET': 12
};
function isBoolean(b) {
    return b === true || b === false;
}
function isNumber(n) {
    return n !== null && !isNaN(Number(n));
}
function isString(str) {
    return typeof str === "string";
}
function getAttributeInfo(name, attributeValue) {
    if (attributeValue == null)
        return null;
    if (isDate(attributeValue)) {
        return new AttributeInfo({ name: name, type: 'TIME' });
    }
    else if (isBoolean(attributeValue)) {
        return new AttributeInfo({ name: name, type: 'BOOLEAN' });
    }
    else if (isNumber(attributeValue)) {
        return new AttributeInfo({ name: name, type: 'NUMBER' });
    }
    else if (isString(attributeValue)) {
        return new AttributeInfo({ name: name, type: 'STRING' });
    }
    else if (attributeValue instanceof NumberRange) {
        return new AttributeInfo({ name: name, type: 'NUMBER_RANGE' });
    }
    else if (attributeValue instanceof StringRange) {
        return new AttributeInfo({ name: name, type: 'STRING_RANGE' });
    }
    else if (attributeValue instanceof TimeRange) {
        return new AttributeInfo({ name: name, type: 'TIME_RANGE' });
    }
    else if (attributeValue instanceof Set) {
        return new AttributeInfo({ name: name, type: attributeValue.getType() });
    }
    else if (attributeValue instanceof Dataset || attributeValue instanceof External) {
        return new AttributeInfo({ name: name, type: 'DATASET' });
    }
    else {
        throw new Error("Could not introspect " + attributeValue);
    }
}
function joinDatums(datumA, datumB) {
    var newDatum = Object.create(null);
    for (var k in datumB) {
        newDatum[k] = datumB[k];
    }
    for (var k in datumA) {
        newDatum[k] = datumA[k];
    }
    return newDatum;
}
function copy(obj) {
    var newObj = {};
    var k;
    for (k in obj) {
        if (hasOwnProp(obj, k))
            newObj[k] = obj[k];
    }
    return newObj;
}
var check;
var Dataset = (function () {
    function Dataset(parameters) {
        this.attributes = null;
        if (parameters.suppress === true)
            this.suppress = true;
        this.keys = parameters.keys || [];
        var data = parameters.data;
        if (!Array.isArray(data)) {
            throw new TypeError("must have a `data` array");
        }
        this.data = data;
        var attributes = parameters.attributes;
        if (!attributes)
            attributes = Dataset.getAttributesFromData(data);
        this.attributes = attributes;
    }
    Dataset.datumToLine = function (datum, attributes, timezone, formatter, finalizer, separator) {
        return attributes.map(function (c) {
            var value = datum[c.name];
            var fmtrType = value != null ? c.type : 'NULL';
            var fmtr = formatter[fmtrType] || Dataset.DEFAULT_FORMATTER[fmtrType];
            var formatted = String(fmtr(value, timezone));
            return finalizer(formatted);
        }).join(separator);
    };
    Dataset.isDataset = function (candidate) {
        return candidate instanceof Dataset;
    };
    Dataset.datumFromJS = function (js, attributeLookup) {
        if (attributeLookup === void 0) { attributeLookup = {}; }
        if (typeof js !== 'object')
            throw new TypeError("datum must be an object");
        var datum = Object.create(null);
        for (var k in js) {
            if (!hasOwnProp(js, k))
                continue;
            datum[k] = valueFromJS(js[k], hasOwnProp(attributeLookup, k) ? attributeLookup[k].type : null);
        }
        return datum;
    };
    Dataset.datumToJS = function (datum) {
        var js = {};
        for (var k in datum) {
            var v = datum[k];
            if (v && v.suppress)
                continue;
            js[k] = valueToJS(v);
        }
        return js;
    };
    Dataset.getAttributesFromData = function (data) {
        if (!data.length)
            return [];
        var attributeNamesToIntrospect = Object.keys(data[0]);
        var attributes = [];
        for (var _i = 0, data_1 = data; _i < data_1.length; _i++) {
            var datum = data_1[_i];
            var attributeNamesStillToIntrospect = [];
            for (var _a = 0, attributeNamesToIntrospect_1 = attributeNamesToIntrospect; _a < attributeNamesToIntrospect_1.length; _a++) {
                var attributeNameToIntrospect = attributeNamesToIntrospect_1[_a];
                var attributeInfo = getAttributeInfo(attributeNameToIntrospect, datum[attributeNameToIntrospect]);
                if (attributeInfo) {
                    attributes.push(attributeInfo);
                }
                else {
                    attributeNamesStillToIntrospect.push(attributeNameToIntrospect);
                }
            }
            attributeNamesToIntrospect = attributeNamesStillToIntrospect;
            if (!attributeNamesToIntrospect.length)
                break;
        }
        for (var _b = 0, attributeNamesToIntrospect_2 = attributeNamesToIntrospect; _b < attributeNamesToIntrospect_2.length; _b++) {
            var attributeName = attributeNamesToIntrospect_2[_b];
            attributes.push(new AttributeInfo({ name: attributeName, type: 'STRING' }));
        }
        attributes.sort(function (a, b) {
            var typeDiff = typeOrder[a.type] - typeOrder[b.type];
            if (typeDiff)
                return typeDiff;
            return a.name.localeCompare(b.name);
        });
        return attributes;
    };
    Dataset.parseJSON = function (text) {
        text = text.trim();
        var firstChar = text[0];
        if (firstChar[0] === '[') {
            try {
                return JSON.parse(text);
            }
            catch (e) {
                throw new Error("could not parse");
            }
        }
        else if (firstChar[0] === '{') {
            return text.split(/\r?\n/).map(function (line, i) {
                try {
                    return JSON.parse(line);
                }
                catch (e) {
                    throw new Error("problem in line: " + i + ": '" + line + "'");
                }
            });
        }
        else {
            throw new Error("Unsupported start, starts with '" + firstChar[0] + "'");
        }
    };
    Dataset.fromJS = function (parameters) {
        if (Array.isArray(parameters)) {
            parameters = { data: parameters };
        }
        if (!Array.isArray(parameters.data)) {
            throw new Error('must have data');
        }
        var attributes = undefined;
        var attributeLookup = {};
        if (parameters.attributes) {
            attributes = AttributeInfo.fromJSs(parameters.attributes);
            for (var _i = 0, attributes_1 = attributes; _i < attributes_1.length; _i++) {
                var attribute = attributes_1[_i];
                attributeLookup[attribute.name] = attribute;
            }
        }
        return new Dataset({
            attributes: attributes,
            keys: parameters.keys || [],
            data: parameters.data.map(function (d) { return Dataset.datumFromJS(d, attributeLookup); })
        });
    };
    Dataset.prototype.valueOf = function () {
        var value = {
            keys: this.keys,
            attributes: this.attributes,
            data: this.data
        };
        if (this.suppress)
            value.suppress = true;
        return value;
    };
    Dataset.prototype.toJS = function () {
        var js = {};
        if (this.keys.length)
            js.keys = this.keys;
        if (this.attributes)
            js.attributes = AttributeInfo.toJSs(this.attributes);
        js.data = this.data.map(Dataset.datumToJS);
        return js;
    };
    Dataset.prototype.toString = function () {
        return "Dataset(" + this.data.length + ")";
    };
    Dataset.prototype.toJSON = function () {
        return this.toJS();
    };
    Dataset.prototype.equals = function (other) {
        return other instanceof Dataset &&
            this.data.length === other.data.length;
    };
    Dataset.prototype.hide = function () {
        var value = this.valueOf();
        value.suppress = true;
        return new Dataset(value);
    };
    Dataset.prototype.changeData = function (data) {
        var value = this.valueOf();
        value.data = data;
        return new Dataset(value);
    };
    Dataset.prototype.basis = function () {
        var data = this.data;
        return data.length === 1 && Object.keys(data[0]).length === 0;
    };
    Dataset.prototype.hasExternal = function () {
        if (!this.data.length)
            return false;
        return datumHasExternal(this.data[0]);
    };
    Dataset.prototype.getFullType = function () {
        var attributes = this.attributes;
        if (!attributes)
            throw new Error("dataset has not been introspected");
        var myDatasetType = {};
        for (var _i = 0, attributes_2 = attributes; _i < attributes_2.length; _i++) {
            var attribute = attributes_2[_i];
            var attrName = attribute.name;
            if (attribute.type === 'DATASET') {
                var v0 = void 0;
                if (this.data.length && (v0 = this.data[0][attrName]) && v0 instanceof Dataset) {
                    myDatasetType[attrName] = v0.getFullType();
                }
                else {
                    myDatasetType[attrName] = {
                        type: 'DATASET',
                        datasetType: {}
                    };
                }
            }
            else {
                myDatasetType[attrName] = {
                    type: attribute.type
                };
            }
        }
        return {
            type: 'DATASET',
            datasetType: myDatasetType
        };
    };
    Dataset.prototype.select = function (attrs) {
        var attributes = this.attributes;
        var newAttributes = [];
        var attrLookup = Object.create(null);
        for (var _i = 0, attrs_1 = attrs; _i < attrs_1.length; _i++) {
            var attr = attrs_1[_i];
            attrLookup[attr] = true;
            var existingAttribute = NamedArray.get(attributes, attr);
            if (existingAttribute)
                newAttributes.push(existingAttribute);
        }
        var data = this.data;
        var n = data.length;
        var newData = new Array(n);
        for (var i = 0; i < n; i++) {
            var datum = data[i];
            var newDatum = Object.create(null);
            for (var key in datum) {
                if (attrLookup[key]) {
                    newDatum[key] = datum[key];
                }
            }
            newData[i] = newDatum;
        }
        var value = this.valueOf();
        value.attributes = newAttributes;
        value.data = newData;
        return new Dataset(value);
    };
    Dataset.prototype.apply = function (name, ex) {
        if (typeof ex === 'function') {
            console.warn("Dataset#apply now takes Expressions use Dataset.applyFn instead");
            return this.applyFn(name, ex, arguments[2]);
        }
        return this.applyFn(name, ex.getFn(), ex.type);
    };
    Dataset.prototype.applyFn = function (name, exFn, type) {
        var data = this.data;
        var n = data.length;
        var newData = new Array(n);
        for (var i = 0; i < n; i++) {
            var datum = data[i];
            var newDatum = Object.create(null);
            for (var key in datum)
                newDatum[key] = datum[key];
            newDatum[name] = exFn(datum);
            newData[i] = newDatum;
        }
        var value = this.valueOf();
        value.attributes = NamedArray.overrideByName(value.attributes, new AttributeInfo({ name: name, type: type }));
        value.data = newData;
        return new Dataset(value);
    };
    Dataset.prototype.filter = function (ex) {
        if (typeof ex === 'function') {
            console.warn("Dataset#filter now takes Expressions use Dataset.filterFn instead");
            return this.filterFn(ex);
        }
        return this.filterFn(ex.getFn());
    };
    Dataset.prototype.filterFn = function (exFn) {
        var value = this.valueOf();
        value.data = value.data.filter(function (datum) { return exFn(datum); });
        return new Dataset(value);
    };
    Dataset.prototype.sort = function (ex, direction) {
        if (typeof ex === 'function') {
            console.warn("Dataset#sort now takes Expressions use Dataset.sortFn instead");
            return this.sortFn(ex, direction);
        }
        return this.sortFn(ex.getFn(), direction);
    };
    Dataset.prototype.sortFn = function (exFn, direction) {
        var value = this.valueOf();
        var directionFn = directionFns[direction];
        value.data = this.data.slice().sort(function (a, b) {
            return directionFn(exFn(a), exFn(b));
        });
        return new Dataset(value);
    };
    Dataset.prototype.limit = function (limit) {
        var data = this.data;
        if (data.length <= limit)
            return this;
        var value = this.valueOf();
        value.data = data.slice(0, limit);
        return new Dataset(value);
    };
    Dataset.prototype.count = function () {
        return this.data.length;
    };
    Dataset.prototype.sum = function (ex) {
        if (typeof ex === 'function') {
            console.warn("Dataset#sum now takes Expressions use Dataset.sumFn instead");
            return this.sumFn(ex);
        }
        return this.sumFn(ex.getFn());
    };
    Dataset.prototype.sumFn = function (exFn) {
        var data = this.data;
        var sum = 0;
        for (var _i = 0, data_2 = data; _i < data_2.length; _i++) {
            var datum = data_2[_i];
            sum += exFn(datum);
        }
        return sum;
    };
    Dataset.prototype.average = function (ex) {
        if (typeof ex === 'function') {
            console.warn("Dataset#average now takes Expressions use Dataset.averageFn instead");
            return this.averageFn(ex);
        }
        return this.averageFn(ex.getFn());
    };
    Dataset.prototype.averageFn = function (exFn) {
        var count = this.count();
        return count ? (this.sumFn(exFn) / count) : null;
    };
    Dataset.prototype.min = function (ex) {
        if (typeof ex === 'function') {
            console.warn("Dataset#min now takes Expressions use Dataset.minFn instead");
            return this.minFn(ex);
        }
        return this.minFn(ex.getFn());
    };
    Dataset.prototype.minFn = function (exFn) {
        var data = this.data;
        var min = Infinity;
        for (var _i = 0, data_3 = data; _i < data_3.length; _i++) {
            var datum = data_3[_i];
            var v = exFn(datum);
            if (v < min)
                min = v;
        }
        return min;
    };
    Dataset.prototype.max = function (ex) {
        if (typeof ex === 'function') {
            console.warn("Dataset#max now takes Expressions use Dataset.maxFn instead");
            return this.maxFn(ex);
        }
        return this.maxFn(ex.getFn());
    };
    Dataset.prototype.maxFn = function (exFn) {
        var data = this.data;
        var max = -Infinity;
        for (var _i = 0, data_4 = data; _i < data_4.length; _i++) {
            var datum = data_4[_i];
            var v = exFn(datum);
            if (max < v)
                max = v;
        }
        return max;
    };
    Dataset.prototype.countDistinct = function (ex) {
        if (typeof ex === 'function') {
            console.warn("Dataset#countDistinct now takes Expressions use Dataset.countDistinctFn instead");
            return this.countDistinctFn(ex);
        }
        return this.countDistinctFn(ex.getFn());
    };
    Dataset.prototype.countDistinctFn = function (exFn) {
        var data = this.data;
        var seen = Object.create(null);
        var count = 0;
        for (var _i = 0, data_5 = data; _i < data_5.length; _i++) {
            var datum = data_5[_i];
            var v = exFn(datum);
            if (!seen[v]) {
                seen[v] = 1;
                ++count;
            }
        }
        return count;
    };
    Dataset.prototype.quantile = function (ex, quantile) {
        if (typeof ex === 'function') {
            console.warn("Dataset#quantile now takes Expressions use Dataset.quantileFn instead");
            return this.quantileFn(ex, quantile);
        }
        return this.quantileFn(ex.getFn(), quantile);
    };
    Dataset.prototype.quantileFn = function (exFn, quantile) {
        var data = this.data;
        var vs = [];
        for (var _i = 0, data_6 = data; _i < data_6.length; _i++) {
            var datum = data_6[_i];
            var v = exFn(datum);
            if (v != null)
                vs.push(v);
        }
        vs.sort(function (a, b) { return a - b; });
        var n = vs.length;
        if (quantile === 0)
            return vs[0];
        if (quantile === 1)
            return vs[n - 1];
        var rank = n * quantile - 1;
        if (rank === Math.floor(rank)) {
            return (vs[rank] + vs[rank + 1]) / 2;
        }
        else {
            return vs[Math.ceil(rank)];
        }
    };
    Dataset.prototype.collect = function (ex) {
        if (typeof ex === 'function') {
            console.warn("Dataset#collect now takes Expressions use Dataset.collectFn instead");
            return this.collectFn(ex);
        }
        return this.collectFn(ex.getFn());
    };
    Dataset.prototype.collectFn = function (exFn) {
        return Set.fromJS(this.data.map(exFn));
    };
    Dataset.prototype.split = function (splits, datasetName) {
        var splitFns = {};
        for (var k in splits) {
            var ex = splits[k];
            if (typeof ex === 'function') {
                console.warn("Dataset#collect now takes Expressions use Dataset.collectFn instead");
                return this.split(splits, datasetName);
            }
            splitFns[k] = ex.getFn();
        }
        return this.splitFn(splitFns, datasetName);
    };
    Dataset.prototype.splitFn = function (splitFns, datasetName) {
        var _a = this, data = _a.data, attributes = _a.attributes;
        var keys = Object.keys(splitFns);
        var numberOfKeys = keys.length;
        var splitFnList = keys.map(function (k) { return splitFns[k]; });
        var splits = {};
        var datumGroups = {};
        var finalData = [];
        var finalDataset = [];
        function addDatum(datum, valueList) {
            var key = valueList.join(';_PLYw00d_;');
            if (hasOwnProp(datumGroups, key)) {
                datumGroups[key].push(datum);
            }
            else {
                var newDatum = Object.create(null);
                for (var i = 0; i < numberOfKeys; i++) {
                    newDatum[keys[i]] = valueList[i];
                }
                finalDataset.push(datumGroups[key] = [datum]);
                splits[key] = newDatum;
                finalData.push(newDatum);
            }
        }
        var _loop_1 = function (datum) {
            var valueList = splitFnList.map(function (splitFn) { return splitFn(datum); });
            var setIndex = [];
            var setElements = [];
            for (var i = 0; i < valueList.length; i++) {
                if (Set.isSet(valueList[i])) {
                    setIndex.push(i);
                    setElements.push(valueList[i].elements);
                }
            }
            var numSets = setIndex.length;
            if (numSets) {
                var cp = Set.cartesianProductOf.apply(Set, setElements);
                for (var _i = 0, cp_1 = cp; _i < cp_1.length; _i++) {
                    var v = cp_1[_i];
                    for (var j = 0; j < numSets; j++) {
                        valueList[setIndex[j]] = v[j];
                    }
                    addDatum(datum, valueList);
                }
            }
            else {
                addDatum(datum, valueList);
            }
        };
        for (var _i = 0, data_7 = data; _i < data_7.length; _i++) {
            var datum = data_7[_i];
            _loop_1(datum);
        }
        for (var i = 0; i < finalData.length; i++) {
            finalData[i][datasetName] = new Dataset({
                suppress: true,
                attributes: attributes,
                data: finalDataset[i]
            });
        }
        return new Dataset({
            keys: keys,
            data: finalData
        });
    };
    Dataset.prototype.getReadyExternals = function (limit) {
        if (limit === void 0) { limit = Infinity; }
        var externalAlterations = [];
        var _a = this, data = _a.data, attributes = _a.attributes;
        for (var i = 0; i < data.length; i++) {
            if (limit <= 0)
                break;
            var datum = data[i];
            var normalExternalAlterations = [];
            var valueExternalAlterations = [];
            for (var _i = 0, attributes_3 = attributes; _i < attributes_3.length; _i++) {
                var attribute = attributes_3[_i];
                var value = datum[attribute.name];
                if (value instanceof Expression) {
                    var subExpressionAlterations = value.getReadyExternals(limit);
                    var size = sizeOfExpressionExternalAlteration(subExpressionAlterations);
                    if (size) {
                        limit -= size;
                        normalExternalAlterations.push({
                            index: i,
                            key: attribute.name,
                            expressionAlterations: subExpressionAlterations
                        });
                    }
                }
                else if (value instanceof Dataset) {
                    var subDatasetAlterations = value.getReadyExternals(limit);
                    var size = sizeOfDatasetExternalAlterations(subDatasetAlterations);
                    if (size) {
                        limit -= size;
                        normalExternalAlterations.push({
                            index: i,
                            key: attribute.name,
                            datasetAlterations: subDatasetAlterations
                        });
                    }
                }
                else if (value instanceof External) {
                    if (!value.suppress) {
                        var externalAlteration = {
                            index: i,
                            key: attribute.name,
                            external: value,
                            terminal: true
                        };
                        if (value.mode === 'value') {
                            valueExternalAlterations.push(externalAlteration);
                        }
                        else {
                            limit--;
                            normalExternalAlterations.push(externalAlteration);
                        }
                    }
                }
            }
            if (valueExternalAlterations.length) {
                limit--;
                if (valueExternalAlterations.length === 1) {
                    externalAlterations.push(valueExternalAlterations[0]);
                }
                else {
                    externalAlterations.push({
                        index: i,
                        key: '',
                        external: External.uniteValueExternalsIntoTotal(valueExternalAlterations)
                    });
                }
            }
            if (normalExternalAlterations.length) {
                Array.prototype.push.apply(externalAlterations, normalExternalAlterations);
            }
        }
        return externalAlterations;
    };
    Dataset.prototype.applyReadyExternals = function (alterations) {
        var data = this.data;
        for (var _i = 0, alterations_3 = alterations; _i < alterations_3.length; _i++) {
            var alteration = alterations_3[_i];
            var datum = data[alteration.index];
            var key = alteration.key;
            if (alteration.external) {
                var result = alteration.result;
                if (result instanceof TotalContainer) {
                    var resultDatum = result.datum;
                    for (var k in resultDatum) {
                        datum[k] = resultDatum[k];
                    }
                }
                else {
                    datum[key] = result;
                }
            }
            else if (alteration.datasetAlterations) {
                datum[key] = datum[key].applyReadyExternals(alteration.datasetAlterations);
            }
            else if (alteration.expressionAlterations) {
                var exAlt = datum[key].applyReadyExternals(alteration.expressionAlterations);
                if (exAlt instanceof ExternalExpression) {
                    datum[key] = exAlt.external;
                }
                else if (exAlt instanceof LiteralExpression) {
                    datum[key] = exAlt.getLiteralValue();
                }
                else {
                    datum[key] = exAlt;
                }
            }
            else {
                throw new Error('fell through');
            }
        }
        for (var _a = 0, data_8 = data; _a < data_8.length; _a++) {
            var datum = data_8[_a];
            for (var key in datum) {
                var v = datum[key];
                if (v instanceof Expression) {
                    var simp = v.resolve(datum).simplify();
                    datum[key] = simp instanceof ExternalExpression ? simp.external : simp;
                }
            }
        }
        var value = this.valueOf();
        value.data = data;
        return new Dataset(value);
    };
    Dataset.prototype.getKeyLookup = function () {
        var _a = this, data = _a.data, keys = _a.keys;
        var thisKey = keys[0];
        if (!thisKey)
            throw new Error('join lhs must have a key (be a product of a split)');
        var mapping = Object.create(null);
        for (var i = 0; i < data.length; i++) {
            var datum = data[i];
            mapping[String(datum[thisKey])] = datum;
        }
        return mapping;
    };
    Dataset.prototype.join = function (other) {
        return this.leftJoin(other);
    };
    Dataset.prototype.leftJoin = function (other) {
        if (!other || !other.data.length)
            return this;
        var _a = this, data = _a.data, keys = _a.keys, attributes = _a.attributes;
        if (!data.length)
            return this;
        var thisKey = keys[0];
        if (!thisKey)
            throw new Error('join lhs must have a key (be a product of a split)');
        var otherLookup = other.getKeyLookup();
        var newData = data.map(function (datum) {
            var otherDatum = otherLookup[String(datum[thisKey])];
            if (!otherDatum)
                return datum;
            return joinDatums(datum, otherDatum);
        });
        return new Dataset({
            keys: keys,
            attributes: AttributeInfo.override(attributes, other.attributes),
            data: newData
        });
    };
    Dataset.prototype.fullJoin = function (other, compare) {
        if (!other || !other.data.length)
            return this;
        var _a = this, data = _a.data, keys = _a.keys, attributes = _a.attributes;
        if (!data.length)
            return other;
        var thisKey = keys[0];
        if (!thisKey)
            throw new Error('join lhs must have a key (be a product of a split)');
        if (thisKey !== other.keys[0])
            throw new Error('this and other keys must match');
        var otherData = other.data;
        var dataLength = data.length;
        var otherDataLength = otherData.length;
        var newData = [];
        var i = 0;
        var j = 0;
        while (i < dataLength || j < otherDataLength) {
            if (i < dataLength && j < otherDataLength) {
                var nextDatum = data[i];
                var nextOtherDatum = otherData[j];
                var cmp = compare(nextDatum[thisKey], nextOtherDatum[thisKey]);
                if (cmp < 0) {
                    newData.push(nextDatum);
                    i++;
                }
                else if (cmp > 0) {
                    newData.push(nextOtherDatum);
                    j++;
                }
                else {
                    newData.push(joinDatums(nextDatum, nextOtherDatum));
                    i++;
                    j++;
                }
            }
            else if (i === dataLength) {
                newData.push(otherData[j]);
                j++;
            }
            else {
                newData.push(data[i]);
                i++;
            }
        }
        return new Dataset({
            keys: keys,
            attributes: AttributeInfo.override(attributes, other.attributes),
            data: newData
        });
    };
    Dataset.prototype.findDatumByAttribute = function (attribute, value) {
        return SimpleArray.find(this.data, function (d) { return generalEqual(d[attribute], value); });
    };
    Dataset.prototype.getColumns = function (options) {
        if (options === void 0) { options = {}; }
        return this.flatten(options).attributes;
    };
    Dataset.prototype._flattenHelper = function (prefix, order, nestingName, nesting, context, primaryFlatAttributes, secondaryFlatAttributes, seenAttributes, flatData) {
        var _a = this, attributes = _a.attributes, data = _a.data, keys = _a.keys;
        var datasetAttributes = [];
        for (var _i = 0, attributes_4 = attributes; _i < attributes_4.length; _i++) {
            var attribute = attributes_4[_i];
            if (attribute.type === 'DATASET') {
                datasetAttributes.push(attribute.name);
            }
            else {
                var flatName = (prefix || '') + attribute.name;
                if (!seenAttributes[flatName]) {
                    var flatAttribute = new AttributeInfo({
                        name: flatName,
                        type: attribute.type
                    });
                    if (!secondaryFlatAttributes || (keys && keys.indexOf(attribute.name) > -1)) {
                        primaryFlatAttributes.push(flatAttribute);
                    }
                    else {
                        secondaryFlatAttributes.push(flatAttribute);
                    }
                    seenAttributes[flatName] = true;
                }
            }
        }
        for (var _b = 0, data_9 = data; _b < data_9.length; _b++) {
            var datum = data_9[_b];
            var flatDatum = context ? copy(context) : {};
            if (nestingName)
                flatDatum[nestingName] = nesting;
            var hasDataset = false;
            for (var _c = 0, attributes_5 = attributes; _c < attributes_5.length; _c++) {
                var attribute = attributes_5[_c];
                var v = datum[attribute.name];
                if (v instanceof Dataset) {
                    hasDataset = true;
                    continue;
                }
                var flatName = (prefix || '') + attribute.name;
                flatDatum[flatName] = v;
            }
            if (hasDataset) {
                if (order === 'preorder')
                    flatData.push(flatDatum);
                for (var _d = 0, datasetAttributes_1 = datasetAttributes; _d < datasetAttributes_1.length; _d++) {
                    var datasetAttribute = datasetAttributes_1[_d];
                    var nextPrefix = null;
                    if (prefix !== null)
                        nextPrefix = prefix + datasetAttribute + '.';
                    var dv = datum[datasetAttribute];
                    if (dv instanceof Dataset) {
                        dv._flattenHelper(nextPrefix, order, nestingName, nesting + 1, flatDatum, primaryFlatAttributes, secondaryFlatAttributes, seenAttributes, flatData);
                    }
                }
                if (order === 'postorder')
                    flatData.push(flatDatum);
            }
            else {
                flatData.push(flatDatum);
            }
        }
    };
    Dataset.prototype.flatten = function (options) {
        if (options === void 0) { options = {}; }
        var prefixColumns = options.prefixColumns;
        var order = options.order;
        var nestingName = options.nestingName;
        var columnOrdering = options.columnOrdering || 'as-seen';
        if (options.parentName) {
            throw new Error("parentName option is no longer supported");
        }
        if (options.orderedColumns) {
            throw new Error("orderedColumns option is no longer supported use .select() instead");
        }
        if (columnOrdering !== 'as-seen' && columnOrdering !== 'keys-first') {
            throw new Error("columnOrdering must be one of 'as-seen' or 'keys-first'");
        }
        var primaryFlatAttributes = [];
        var secondaryFlatAttributes = columnOrdering === 'keys-first' ? [] : null;
        var flatData = [];
        this._flattenHelper((prefixColumns ? '' : null), order, nestingName, 0, null, primaryFlatAttributes, secondaryFlatAttributes, {}, flatData);
        return new Dataset({
            attributes: primaryFlatAttributes.concat(secondaryFlatAttributes || []),
            data: flatData
        });
    };
    Dataset.prototype.toTabular = function (tabulatorOptions) {
        var formatter = tabulatorOptions.formatter || {};
        var timezone = tabulatorOptions.timezone || Timezone.UTC;
        var finalizer = tabulatorOptions.finalizer || String;
        var separator = tabulatorOptions.separator || ',';
        var attributeTitle = tabulatorOptions.attributeTitle || (function (a) { return a.name; });
        var _a = this.flatten(tabulatorOptions), data = _a.data, attributes = _a.attributes;
        if (tabulatorOptions.attributeFilter) {
            attributes = attributes.filter(tabulatorOptions.attributeFilter);
        }
        var lines = [];
        lines.push(attributes.map(function (c) { return finalizer(attributeTitle(c)); }).join(separator));
        for (var i = 0; i < data.length; i++) {
            lines.push(Dataset.datumToLine(data[i], attributes, timezone, formatter, finalizer, separator));
        }
        var lineBreak = tabulatorOptions.lineBreak || '\n';
        return lines.join(lineBreak) + (tabulatorOptions.finalLineBreak === 'include' && lines.length > 0 ? lineBreak : '');
    };
    Dataset.prototype.toCSV = function (tabulatorOptions) {
        if (tabulatorOptions === void 0) { tabulatorOptions = {}; }
        tabulatorOptions.finalizer = Dataset.CSV_FINALIZER;
        tabulatorOptions.separator = tabulatorOptions.separator || ',';
        tabulatorOptions.lineBreak = tabulatorOptions.lineBreak || '\r\n';
        tabulatorOptions.finalLineBreak = tabulatorOptions.finalLineBreak || 'suppress';
        tabulatorOptions.columnOrdering = tabulatorOptions.columnOrdering || 'keys-first';
        return this.toTabular(tabulatorOptions);
    };
    Dataset.prototype.toTSV = function (tabulatorOptions) {
        if (tabulatorOptions === void 0) { tabulatorOptions = {}; }
        tabulatorOptions.finalizer = Dataset.TSV_FINALIZER;
        tabulatorOptions.separator = tabulatorOptions.separator || '\t';
        tabulatorOptions.lineBreak = tabulatorOptions.lineBreak || '\r\n';
        tabulatorOptions.finalLineBreak = tabulatorOptions.finalLineBreak || 'suppress';
        tabulatorOptions.columnOrdering = tabulatorOptions.columnOrdering || 'keys-first';
        return this.toTabular(tabulatorOptions);
    };
    Dataset.prototype.rows = function () {
        var _a = this, data = _a.data, attributes = _a.attributes;
        var c = data.length;
        for (var _i = 0, data_10 = data; _i < data_10.length; _i++) {
            var datum = data_10[_i];
            for (var _b = 0, attributes_6 = attributes; _b < attributes_6.length; _b++) {
                var attribute = attributes_6[_b];
                var v = datum[attribute.name];
                if (v instanceof Dataset) {
                    c += v.rows();
                }
            }
        }
        return c;
    };
    Dataset.prototype.depthFirstTrimTo = function (n) {
        var mySize = this.rows();
        if (mySize < n)
            return this;
        var _a = this, data = _a.data, attributes = _a.attributes;
        var newData = [];
        for (var _i = 0, data_11 = data; _i < data_11.length; _i++) {
            var datum = data_11[_i];
            if (n <= 0)
                break;
            n--;
            var newDatum = {};
            var newDatumRows = 0;
            for (var _b = 0, attributes_7 = attributes; _b < attributes_7.length; _b++) {
                var attribute = attributes_7[_b];
                var attributeName = attribute.name;
                var v = datum[attributeName];
                if (v instanceof Dataset) {
                    var vTrim = v.depthFirstTrimTo(n);
                    newDatum[attributeName] = vTrim;
                    newDatumRows += vTrim.rows();
                }
                else if (typeof v !== 'undefined') {
                    newDatum[attributeName] = v;
                }
            }
            n -= newDatumRows;
            newData.push(newDatum);
        }
        return this.changeData(newData);
    };
    Dataset.type = 'DATASET';
    Dataset.DEFAULT_FORMATTER = {
        'NULL': function (v) { return isDate(v) ? v.toISOString() : '' + v; },
        'TIME': function (v, tz) { return Timezone.formatDateWithTimezone(v, tz); },
        'TIME_RANGE': function (v, tz) { return v.toString(tz); },
        'SET/TIME': function (v, tz) { return v.toString(tz); },
        'SET/TIME_RANGE': function (v, tz) { return v.toString(tz); },
        'STRING': function (v) { return '' + v; },
        'SET/STRING': function (v) { return '' + v; },
        'BOOLEAN': function (v) { return '' + v; },
        'NUMBER': function (v) { return '' + v; },
        'NUMBER_RANGE': function (v) { return '' + v; },
        'SET/NUMBER': function (v) { return '' + v; },
        'SET/NUMBER_RANGE': function (v) { return '' + v; },
        'DATASET': function (v) { return 'DATASET'; }
    };
    Dataset.CSV_FINALIZER = function (v) {
        v = removeLineBreaks(v);
        if (v.indexOf('"') === -1 && v.indexOf(",") === -1)
            return v;
        return "\"" + v.replace(/"/g, '""') + "\"";
    };
    Dataset.TSV_FINALIZER = function (v) {
        return removeLineBreaks(v).replace(/\t/g, "").replace(/"/g, '""');
    };
    return Dataset;
}());
exports.Dataset = Dataset;
check = Dataset;
var iteratorFactory = exports.iteratorFactory = function(value) {
    if (value instanceof Dataset)
        return datasetIteratorFactory(value);
    var nextBit = { type: 'value', value: value };
    return function () {
        var ret = nextBit;
        nextBit = null;
        return ret;
    };
}
var datasetIteratorFactory = exports.datasetIteratorFactory = function(dataset) {
    var curRowIndex = -2;
    var curRow = null;
    var cutRowDatasets = [];
    function nextSelfRow() {
        curRowIndex++;
        cutRowDatasets = [];
        var row = dataset.data[curRowIndex];
        if (row) {
            curRow = {};
            for (var k in row) {
                var v = row[k];
                if (v instanceof Dataset) {
                    cutRowDatasets.push({
                        attribute: k,
                        datasetIterator: datasetIteratorFactory(v)
                    });
                }
                else {
                    curRow[k] = v;
                }
            }
        }
        else {
            curRow = null;
        }
    }
    return function () {
        if (curRowIndex === -2) {
            curRowIndex++;
            var initEvent = {
                type: 'init',
                attributes: dataset.attributes
            };
            if (dataset.keys.length)
                initEvent.keys = dataset.keys;
            return initEvent;
        }
        var pb;
        while (cutRowDatasets.length && !pb) {
            pb = cutRowDatasets[0].datasetIterator();
            if (!pb)
                cutRowDatasets.shift();
        }
        if (pb) {
            return {
                type: 'within',
                attribute: cutRowDatasets[0].attribute,
                within: pb
            };
        }
        nextSelfRow();
        return curRow ? {
            type: 'datum',
            datum: curRow
        } : null;
    };
}
var PlywoodValueBuilder = (function () {
    function PlywoodValueBuilder() {
        this._value = null;
        this._curAttribute = null;
        this._curValueBuilder = null;
    }
    PlywoodValueBuilder.prototype._finalizeLastWithin = function () {
        if (!this._curValueBuilder)
            return;
        var lastDatum = this._data[this._data.length - 1];
        if (!lastDatum)
            throw new Error('unexpected within');
        lastDatum[this._curAttribute] = this._curValueBuilder.getValue();
        this._curAttribute = null;
        this._curValueBuilder = null;
    };
    PlywoodValueBuilder.prototype.processBit = function (bit) {
        if (typeof bit !== 'object')
            throw new Error("invalid bit: " + bit);
        switch (bit.type) {
            case 'value':
                this._value = bit.value;
                this._data = null;
                this._curAttribute = null;
                this._curValueBuilder = null;
                break;
            case 'init':
                this._finalizeLastWithin();
                this._attributes = bit.attributes;
                this._keys = bit.keys;
                this._data = [];
                break;
            case 'datum':
                this._finalizeLastWithin();
                if (!this._data)
                    this._data = [];
                this._data.push(bit.datum);
                break;
            case 'within':
                if (!this._curValueBuilder) {
                    this._curAttribute = bit.attribute;
                    this._curValueBuilder = new PlywoodValueBuilder();
                }
                this._curValueBuilder.processBit(bit.within);
                break;
            default:
                throw new Error("unexpected type: " + bit.type);
        }
    };
    PlywoodValueBuilder.prototype.getValue = function () {
        var _data = this._data;
        if (_data) {
            if (this._curValueBuilder) {
                var lastDatum = _data[_data.length - 1];
                if (!lastDatum)
                    throw new Error('unexpected within');
                lastDatum[this._curAttribute] = this._curValueBuilder.getValue();
            }
            return new Dataset({
                attributes: this._attributes,
                keys: this._keys,
                data: _data
            });
        }
        else {
            return this._value;
        }
    };
    return PlywoodValueBuilder;
}());
exports.PlywoodValueBuilder = PlywoodValueBuilder;
function fillExpressionExternalAlterationAsync(alteration, filler) {
    var tasks = [];
    fillExpressionExternalAlteration(alteration, function (external, terminal) {
        tasks.push(filler(external, terminal));
        return null;
    });
    return Promise.all(tasks).then(function (results) {
        var i = 0;
        fillExpressionExternalAlteration(alteration, function () {
            var res = results[i];
            i++;
            return res;
        });
        return alteration;
    });
}
function runtimeAbstract() {
    return new Error('must be implemented');
}
function getDataName(ex) {
    if (ex instanceof RefExpression) {
        return ex.name;
    }
    else if (ex instanceof ChainableExpression) {
        return getDataName(ex.operand);
    }
    else {
        return null;
    }
}
function getValue(param) {
    if (param instanceof LiteralExpression)
        return param.value;
    return param;
}
function getString(param) {
    if (typeof param === 'string')
        return param;
    if (param instanceof LiteralExpression && param.type === 'STRING') {
        return param.value;
    }
    if (param instanceof RefExpression && param.nest === 0) {
        return param.name;
    }
    throw new Error('could not extract a string out of ' + String(param));
}
function getNumber(param) {
    if (typeof param === 'number')
        return param;
    if (param instanceof LiteralExpression && param.type === 'NUMBER') {
        return param.value;
    }
    throw new Error('could not extract a number out of ' + String(param));
}
var ply = exports.ply = function(dataset) {
    if (!dataset) {
        dataset = new Dataset({
            keys: [],
            data: [{}]
        });
    }
    return r(dataset);
}
var $ = exports.$ = function(name, nest, type) {
    if (typeof name !== 'string')
        throw new TypeError('$() argument must be a string');
    if (typeof nest === 'string') {
        type = nest;
        nest = 0;
    }
    return new RefExpression({
        name: name,
        nest: nest != null ? nest : 0,
        type: type
    });
}
var i$ = exports.i$ = function(name, nest, type) {
    if (typeof name !== 'string')
        throw new TypeError('$() argument must be a string');
    if (typeof nest === 'string') {
        type = nest;
        nest = 0;
    }
    return new RefExpression({
        name: name,
        nest: nest != null ? nest : 0,
        type: type,
        ignoreCase: true
    });
}
var r = exports.r = function(value) {
    if (value instanceof External)
        throw new TypeError('r() can not accept externals');
    if (Array.isArray(value))
        value = Set.fromJS(value);
    return LiteralExpression.fromJS({ op: 'literal', value: value });
}
var toJS = exports.toJS = function(thing) {
    return (thing && typeof thing.toJS === 'function') ? thing.toJS() : thing;
}
function chainVia(op, expressions, zero) {
    var n = expressions.length;
    if (!n)
        return zero;
    var acc = expressions[0];
    if (!(acc instanceof Expression))
        acc = Expression.fromJSLoose(acc);
    for (var i = 1; i < n; i++)
        acc = acc[op](expressions[i]);
    return acc;
}
var Expression = (function () {
    function Expression(parameters, dummy) {
        if (dummy === void 0) { dummy = null; }
        this.op = parameters.op;
        if (dummy !== dummyObject) {
            throw new TypeError("can not call `new Expression` directly use Expression.fromJS instead");
        }
        if (parameters.simple)
            this.simple = true;
        if (parameters.options)
            this.options = parameters.options;
    }
    Expression.isExpression = function (candidate) {
        return candidate instanceof Expression;
    };
    Expression.expressionLookupFromJS = function (expressionJSs) {
        var expressions = Object.create(null);
        for (var name_1 in expressionJSs) {
            if (!hasOwnProp(expressionJSs, name_1))
                continue;
            expressions[name_1] = Expression.fromJSLoose(expressionJSs[name_1]);
        }
        return expressions;
    };
    Expression.expressionLookupToJS = function (expressions) {
        var expressionsJSs = {};
        for (var name_2 in expressions) {
            if (!hasOwnProp(expressions, name_2))
                continue;
            expressionsJSs[name_2] = expressions[name_2].toJS();
        }
        return expressionsJSs;
    };
    Expression.parse = function (str, timezone) {
        if (str[0] === '{' && str[str.length - 1] === '}') {
            return Expression.fromJS(JSON.parse(str));
        }
        var original = Expression.defaultParserTimezone;
        if (timezone)
            Expression.defaultParserTimezone = timezone;
        try {
            return Expression.expressionParser.parse(str);
        }
        catch (e) {
            throw new Error("Expression parse error: " + e.message + " on '" + str + "'");
        }
        finally {
            Expression.defaultParserTimezone = original;
        }
    };
    Expression.parseSQL = function (str, timezone) {
        var original = Expression.defaultParserTimezone;
        if (timezone)
            Expression.defaultParserTimezone = timezone;
        try {
            return Expression.plyqlParser.parse(str);
        }
        catch (e) {
            throw new Error("SQL parse error: " + e.message + " on '" + str + "'");
        }
        finally {
            Expression.defaultParserTimezone = original;
        }
    };
    Expression.fromJSLoose = function (param) {
        var expressionJS;
        switch (typeof param) {
            case 'undefined':
                throw new Error('must have an expression');
            case 'object':
                if (param === null) {
                    return Expression.NULL;
                }
                else if (param instanceof Expression) {
                    return param;
                }
                else if (isImmutableClass(param)) {
                    if (param.constructor.type) {
                        expressionJS = { op: 'literal', value: param };
                    }
                    else {
                        throw new Error("unknown object");
                    }
                }
                else if (param.op) {
                    expressionJS = param;
                }
                else if (param.toISOString) {
                    expressionJS = { op: 'literal', value: new Date(param) };
                }
                else if (Array.isArray(param)) {
                    expressionJS = { op: 'literal', value: Set.fromJS(param) };
                }
                else if (hasOwnProp(param, 'start') && hasOwnProp(param, 'end')) {
                    expressionJS = { op: 'literal', value: Range.fromJS(param) };
                }
                else {
                    throw new Error('unknown parameter');
                }
                break;
            case 'number':
            case 'boolean':
                expressionJS = { op: 'literal', value: param };
                break;
            case 'string':
                return Expression.parse(param);
            default:
                throw new Error("unrecognizable expression");
        }
        return Expression.fromJS(expressionJS);
    };
    Expression.jsNullSafetyUnary = function (inputJS, ifNotNull) {
        return "(_=" + inputJS + ",(_==null?null:" + ifNotNull('_') + "))";
    };
    Expression.jsNullSafetyBinary = function (lhs, rhs, combine, lhsCantBeNull, rhsCantBeNull) {
        if (lhsCantBeNull) {
            if (rhsCantBeNull) {
                return "(" + combine(lhs, rhs) + ")";
            }
            else {
                return "(_=" + rhs + ",(_==null)?null:(" + combine(lhs, '_') + "))";
            }
        }
        else {
            if (rhsCantBeNull) {
                return "(_=" + lhs + ",(_==null)?null:(" + combine('_', rhs) + "))";
            }
            else {
                return "(_=" + rhs + ",_2=" + lhs + ",(_==null||_2==null)?null:(" + combine('_', '_2') + ")";
            }
        }
    };
    Expression.parseTuning = function (tuning) {
        if (typeof tuning !== 'string')
            return {};
        var parts = tuning.split(',');
        var parsed = {};
        for (var _i = 0, parts_1 = parts; _i < parts_1.length; _i++) {
            var part = parts_1[_i];
            var subParts = part.split('=');
            if (subParts.length !== 2)
                throw new Error("can not parse tuning '" + tuning + "'");
            parsed[subParts[0]] = subParts[1];
        }
        return parsed;
    };
    Expression.safeString = function (str) {
        return /^[a-z]\w+$/i.test(str) ? str : JSON.stringify(str);
    };
    Expression.and = function (expressions) {
        return chainVia('and', expressions, Expression.TRUE);
    };
    Expression.or = function (expressions) {
        return chainVia('or', expressions, Expression.FALSE);
    };
    Expression.add = function (expressions) {
        return chainVia('add', expressions, Expression.ZERO);
    };
    Expression.subtract = function (expressions) {
        return chainVia('subtract', expressions, Expression.ZERO);
    };
    Expression.multiply = function (expressions) {
        return chainVia('multiply', expressions, Expression.ONE);
    };
    Expression.power = function (expressions) {
        return chainVia('power', expressions, Expression.ZERO);
    };
    Expression.concat = function (expressions) {
        return chainVia('concat', expressions, Expression.EMPTY_STRING);
    };
    Expression.register = function (ex) {
        var op = ex.op.replace(/^\w/, function (s) { return s.toLowerCase(); });
        Expression.classMap[op] = ex;
    };
    Expression.getConstructorFor = function (op) {
        var ClassFn = Expression.classMap[op];
        if (!ClassFn)
            throw new Error("unsupported expression op '" + op + "'");
        return ClassFn;
    };
    Expression.applyMixins = function (derivedCtor, baseCtors) {
        baseCtors.forEach(function (baseCtor) {
            Object.getOwnPropertyNames(baseCtor.prototype).forEach(function (name) {
                derivedCtor.prototype[name] = baseCtor.prototype[name];
            });
        });
    };
    Expression.jsToValue = function (js) {
        return {
            op: js.op,
            type: js.type,
            options: js.options
        };
    };
    Expression.fromJS = function (expressionJS) {
        if (!expressionJS)
            throw new Error('must have expressionJS');
        if (!hasOwnProp(expressionJS, "op")) {
            if (hasOwnProp(expressionJS, "action")) {
                expressionJS = shallowCopy(expressionJS);
                expressionJS.op = expressionJS.action;
                delete expressionJS.action;
                expressionJS.operand = { op: 'ref', name: '_' };
            }
            else {
                throw new Error("op must be defined");
            }
        }
        if (expressionJS.op === 'custom') {
            expressionJS = shallowCopy(expressionJS);
            expressionJS.op = 'customAggregate';
        }
        var op = expressionJS.op;
        if (typeof op !== "string") {
            throw new Error("op must be a string");
        }
        if (op === 'chain') {
            var actions = expressionJS.actions || [expressionJS.action];
            return Expression.fromJS(expressionJS.expression).performActions(actions.map(Expression.fromJS));
        }
        var ClassFn = Expression.getConstructorFor(op);
        return ClassFn.fromJS(expressionJS);
    };
    Expression.fromValue = function (parameters) {
        var op = parameters.op;
        var ClassFn = Expression.getConstructorFor(op);
        return new ClassFn(parameters);
    };
    Expression.prototype._ensureOp = function (op) {
        if (!this.op) {
            this.op = op;
            return;
        }
        if (this.op !== op) {
            throw new TypeError("incorrect expression op '" + this.op + "' (needs to be: '" + op + "')");
        }
    };
    Expression.prototype.valueOf = function () {
        var value = { op: this.op };
        if (this.simple)
            value.simple = true;
        if (this.options)
            value.options = this.options;
        return value;
    };
    Expression.prototype.toJS = function () {
        var js = { op: this.op };
        if (this.options)
            js.options = this.options;
        return js;
    };
    Expression.prototype.toJSON = function () {
        return this.toJS();
    };
    Expression.prototype.equals = function (other) {
        return other instanceof Expression &&
            this.op === other.op &&
            this.type === other.type &&
            generalLookupsEqual(this.options, other.options);
    };
    Expression.prototype.canHaveType = function (wantedType) {
        var type = this.type;
        if (!type || type === 'NULL')
            return true;
        if (wantedType === 'SET') {
            return Set.isSetType(type);
        }
        else {
            return type === wantedType;
        }
    };
    Expression.prototype.expressionCount = function () {
        return 1;
    };
    Expression.prototype.isOp = function (op) {
        return this.op === op;
    };
    Expression.prototype.markSimple = function () {
        if (this.simple)
            return this;
        var value = this.valueOf();
        value.simple = true;
        return Expression.fromValue(value);
    };
    Expression.prototype.containsOp = function (op) {
        return this.some(function (ex) { return ex.isOp(op) || null; });
    };
    Expression.prototype.hasExternal = function () {
        return this.some(function (ex) {
            if (ex instanceof ExternalExpression)
                return true;
            return null;
        });
    };
    Expression.prototype.getBaseExternals = function () {
        var externals = [];
        this.forEach(function (ex) {
            if (ex instanceof ExternalExpression)
                externals.push(ex.external.getBase());
        });
        return External.deduplicateExternals(externals);
    };
    Expression.prototype.getRawExternals = function () {
        var externals = [];
        this.forEach(function (ex) {
            if (ex instanceof ExternalExpression)
                externals.push(ex.external.getRaw());
        });
        return External.deduplicateExternals(externals);
    };
    Expression.prototype.getReadyExternals = function (limit) {
        if (limit === void 0) { limit = Infinity; }
        var indexToSkip = {};
        var externalsByIndex = {};
        this.every(function (ex, index) {
            if (limit <= 0)
                return null;
            if (ex instanceof ExternalExpression) {
                if (indexToSkip[index])
                    return null;
                if (!ex.external.suppress) {
                    limit--;
                    externalsByIndex[index] = {
                        external: ex.external,
                        terminal: true
                    };
                }
            }
            else if (ex instanceof ChainableExpression) {
                var h = ex._headExternal();
                if (h) {
                    if (h.allGood) {
                        limit--;
                        externalsByIndex[index + h.offset] = { external: h.external };
                        return true;
                    }
                    else {
                        indexToSkip[index + h.offset] = true;
                        return null;
                    }
                }
            }
            else if (ex instanceof LiteralExpression && ex.type === 'DATASET') {
                var datasetExternals = ex.value.getReadyExternals(limit);
                var size = sizeOfDatasetExternalAlterations(datasetExternals);
                if (size) {
                    limit -= size;
                    externalsByIndex[index] = datasetExternals;
                }
                return null;
            }
            return null;
        });
        return externalsByIndex;
    };
    Expression.prototype.applyReadyExternals = function (alterations) {
        return this.substitute(function (ex, index) {
            var alteration = alterations[index];
            if (!alteration)
                return null;
            if (Array.isArray(alteration)) {
                return r(ex.getLiteralValue().applyReadyExternals(alteration));
            }
            else {
                return r(alteration.result);
            }
        }).simplify();
    };
    Expression.prototype._headExternal = function () {
        var ex = this;
        var allGood = true;
        var offset = 0;
        while (ex instanceof ChainableExpression) {
            allGood = allGood && (ex.op === 'filter' ? ex.argumentsResolvedWithoutExternals() : ex.argumentsResolved());
            ex = ex.operand;
            offset++;
        }
        if (ex instanceof ExternalExpression) {
            return {
                allGood: allGood,
                external: ex.external,
                offset: offset
            };
        }
        else {
            return null;
        }
    };
    Expression.prototype.getHeadOperand = function () {
        return this;
    };
    Expression.prototype.getFreeReferences = function () {
        var freeReferences = [];
        this.forEach(function (ex, index, depth, nestDiff) {
            if (ex instanceof RefExpression && nestDiff <= ex.nest) {
                freeReferences.push(repeat('^', ex.nest - nestDiff) + ex.name);
            }
        });
        return deduplicateSort(freeReferences);
    };
    Expression.prototype.getFreeReferenceIndexes = function () {
        var freeReferenceIndexes = [];
        this.forEach(function (ex, index, depth, nestDiff) {
            if (ex instanceof RefExpression && nestDiff <= ex.nest) {
                freeReferenceIndexes.push(index);
            }
        });
        return freeReferenceIndexes;
    };
    Expression.prototype.incrementNesting = function (by) {
        if (by === void 0) { by = 1; }
        var freeReferenceIndexes = this.getFreeReferenceIndexes();
        if (freeReferenceIndexes.length === 0)
            return this;
        return this.substitute(function (ex, index) {
            if (ex instanceof RefExpression && freeReferenceIndexes.indexOf(index) !== -1) {
                return ex.incrementNesting(by);
            }
            return null;
        });
    };
    Expression.prototype.simplify = function () {
        return this;
    };
    Expression.prototype.every = function (iter, thisArg) {
        return this._everyHelper(iter, thisArg, { index: 0 }, 0, 0);
    };
    Expression.prototype._everyHelper = function (iter, thisArg, indexer, depth, nestDiff) {
        var pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
        if (pass != null) {
            return pass;
        }
        else {
            indexer.index++;
        }
        return true;
    };
    Expression.prototype.some = function (iter, thisArg) {
        var _this = this;
        return !this.every(function (ex, index, depth, nestDiff) {
            var v = iter.call(_this, ex, index, depth, nestDiff);
            return (v == null) ? null : !v;
        }, thisArg);
    };
    Expression.prototype.forEach = function (iter, thisArg) {
        var _this = this;
        this.every(function (ex, index, depth, nestDiff) {
            iter.call(_this, ex, index, depth, nestDiff);
            return null;
        }, thisArg);
    };
    Expression.prototype.substitute = function (substitutionFn, typeContext) {
        if (typeContext === void 0) { typeContext = null; }
        return this._substituteHelper(substitutionFn, { index: 0 }, 0, 0, typeContext).expression;
    };
    Expression.prototype._substituteHelper = function (substitutionFn, indexer, depth, nestDiff, typeContext) {
        var sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff, typeContext);
        if (sub) {
            indexer.index += this.expressionCount();
            return {
                expression: sub,
                typeContext: sub.updateTypeContextIfNeeded(typeContext)
            };
        }
        else {
            indexer.index++;
        }
        return {
            expression: this,
            typeContext: this.updateTypeContextIfNeeded(typeContext)
        };
    };
    Expression.prototype.fullyDefined = function () {
        return true;
    };
    Expression.prototype.getJSFn = function (datumVar) {
        if (datumVar === void 0) { datumVar = 'd[]'; }
        var type = this.type;
        var jsEx = this.getJS(datumVar);
        var body;
        if (type === 'NUMBER' || type === 'NUMBER_RANGE' || type === 'TIME') {
            body = "_=" + jsEx + ";return isNaN(_)?null:_";
        }
        else {
            body = "return " + jsEx + ";";
        }
        return "function(" + datumVar.replace('[]', '') + "){var _,_2;" + body + "}";
    };
    Expression.prototype.extractFromAnd = function (matchFn) {
        if (this.type !== 'BOOLEAN')
            return null;
        if (matchFn(this)) {
            return {
                extract: this,
                rest: Expression.TRUE
            };
        }
        else {
            return {
                extract: Expression.TRUE,
                rest: this
            };
        }
    };
    Expression.prototype.breakdownByDataset = function (tempNamePrefix) {
        if (tempNamePrefix === void 0) { tempNamePrefix = 'b'; }
        throw new Error('ToDo');
    };
    Expression.prototype.getLiteralValue = function () {
        return null;
    };
    Expression.prototype.upgradeToType = function (targetType) {
        return this;
    };
    Expression.prototype.performAction = function (action) {
        var _this = this;
        return action.substitute(function (ex) { return ex.equals(Expression._) ? _this : null; });
    };
    Expression.prototype.performActions = function (actions) {
        var ex = this;
        for (var _i = 0, actions_1 = actions; _i < actions_1.length; _i++) {
            var action = actions_1[_i];
            ex = ex.performAction(action);
        }
        return ex;
    };
    Expression.prototype.getOptions = function () {
        return this.options || {};
    };
    Expression.prototype.setOptions = function (options) {
        var value = this.valueOf();
        value.options = options;
        return Expression.fromValue(value);
    };
    Expression.prototype.setOption = function (optionKey, optionValue) {
        var newOptions = Object.assign({}, this.getOptions());
        newOptions[optionKey] = optionValue;
        return this.setOptions(newOptions);
    };
    Expression.prototype._mkChain = function (ExpressionClass, exs) {
        var cur = this;
        for (var _i = 0, exs_1 = exs; _i < exs_1.length; _i++) {
            var ex = exs_1[_i];
            cur = new ExpressionClass({
                operand: cur,
                expression: ex instanceof Expression ? ex : Expression.fromJSLoose(ex)
            });
        }
        return cur;
    };
    Expression.prototype.add = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(AddExpression, exs);
    };
    Expression.prototype.subtract = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(SubtractExpression, exs);
    };
    Expression.prototype.negate = function () {
        return Expression.ZERO.subtract(this);
    };
    Expression.prototype.multiply = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(MultiplyExpression, exs);
    };
    Expression.prototype.divide = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(DivideExpression, exs);
    };
    Expression.prototype.reciprocate = function () {
        return Expression.ONE.divide(this);
    };
    Expression.prototype.sqrt = function () {
        return this.power(0.5);
    };
    Expression.prototype.power = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(PowerExpression, exs);
    };
    Expression.prototype.log = function (ex) {
        if (ex === void 0) { ex = Math.E; }
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new LogExpression({ operand: this, expression: ex });
    };
    Expression.prototype.ln = function () {
        return new LogExpression({ operand: this, expression: r(Math.E) });
    };
    Expression.prototype.then = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new ThenExpression({ operand: this, expression: ex });
    };
    Expression.prototype.fallback = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new FallbackExpression({ operand: this, expression: ex });
    };
    Expression.prototype.is = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new IsExpression({ operand: this, expression: ex });
    };
    Expression.prototype.isnt = function (ex) {
        return this.is(ex).not();
    };
    Expression.prototype.lessThan = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new LessThanExpression({ operand: this, expression: ex });
    };
    Expression.prototype.lessThanOrEqual = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new LessThanOrEqualExpression({ operand: this, expression: ex });
    };
    Expression.prototype.greaterThan = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new GreaterThanExpression({ operand: this, expression: ex });
    };
    Expression.prototype.greaterThanOrEqual = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new GreaterThanOrEqualExpression({ operand: this, expression: ex });
    };
    Expression.prototype.contains = function (ex, compare) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        if (compare)
            compare = getString(compare);
        return new ContainsExpression({ operand: this, expression: ex, compare: compare });
    };
    Expression.prototype.match = function (re) {
        return new MatchExpression({ operand: this, regexp: getString(re) });
    };
    Expression.prototype.in = function (ex) {
        if (arguments.length === 2) {
            return this.overlap(ex, arguments[1]);
        }
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        if (Range.isRangeType(ex.type)) {
            return new OverlapExpression({ operand: this, expression: ex });
        }
        return new InExpression({ operand: this, expression: ex });
    };
    Expression.prototype.overlap = function (ex, snd) {
        if (arguments.length === 2) {
            ex = getValue(ex);
            snd = getValue(snd);
            if (typeof ex === 'string') {
                var parse = parseISODate(ex, Expression.defaultParserTimezone);
                if (parse)
                    ex = parse;
            }
            if (typeof snd === 'string') {
                var parse = parseISODate(snd, Expression.defaultParserTimezone);
                if (parse)
                    snd = parse;
            }
            if (typeof ex === 'number' && typeof snd === 'number') {
                ex = new NumberRange({ start: ex, end: snd });
            }
            else if (ex.toISOString && snd.toISOString) {
                ex = new TimeRange({ start: ex, end: snd });
            }
            else if (typeof ex === 'string' && typeof snd === 'string') {
                ex = new StringRange({ start: ex, end: snd });
            }
            else {
                throw new Error('uninterpretable IN parameters');
            }
        }
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new OverlapExpression({ operand: this, expression: ex });
    };
    Expression.prototype.not = function () {
        return new NotExpression({ operand: this });
    };
    Expression.prototype.and = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(AndExpression, exs);
    };
    Expression.prototype.or = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(OrExpression, exs);
    };
    Expression.prototype.substr = function (position, len) {
        return new SubstrExpression({ operand: this, position: getNumber(position), len: getNumber(len) });
    };
    Expression.prototype.extract = function (re) {
        return new ExtractExpression({ operand: this, regexp: getString(re) });
    };
    Expression.prototype.concat = function () {
        var exs = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            exs[_i] = arguments[_i];
        }
        return this._mkChain(ConcatExpression, exs);
    };
    Expression.prototype.lookup = function (lookupFn) {
        return new LookupExpression({ operand: this, lookupFn: getString(lookupFn) });
    };
    Expression.prototype.indexOf = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new IndexOfExpression({ operand: this, expression: ex });
    };
    Expression.prototype.transformCase = function (transformType) {
        return new TransformCaseExpression({ operand: this, transformType: getString(transformType) });
    };
    Expression.prototype.customTransform = function (custom, outputType) {
        if (!custom)
            throw new Error("Must provide an extraction function name for custom transform");
        outputType = outputType !== undefined ? getString(outputType) : null;
        return new CustomTransformExpression({ operand: this, custom: getString(custom), outputType: outputType });
    };
    Expression.prototype.numberBucket = function (size, offset) {
        if (offset === void 0) { offset = 0; }
        return new NumberBucketExpression({ operand: this, size: getNumber(size), offset: getNumber(offset) });
    };
    Expression.prototype.absolute = function () {
        return new AbsoluteExpression({ operand: this });
    };
    Expression.prototype.length = function () {
        return new LengthExpression({ operand: this });
    };
    Expression.prototype.timeBucket = function (duration, timezone) {
        if (!(duration instanceof Duration))
            duration = Duration.fromJS(getString(duration));
        if (timezone && !(timezone instanceof Timezone))
            timezone = Timezone.fromJS(getString(timezone));
        return new TimeBucketExpression({ operand: this, duration: duration, timezone: timezone });
    };
    Expression.prototype.timeFloor = function (duration, timezone) {
        if (!(duration instanceof Duration))
            duration = Duration.fromJS(getString(duration));
        if (timezone && !(timezone instanceof Timezone))
            timezone = Timezone.fromJS(getString(timezone));
        return new TimeFloorExpression({ operand: this, duration: duration, timezone: timezone });
    };
    Expression.prototype.timeShift = function (duration, step, timezone) {
        if (!(duration instanceof Duration))
            duration = Duration.fromJS(getString(duration));
        step = typeof step !== 'undefined' ? getNumber(step) : null;
        if (timezone && !(timezone instanceof Timezone))
            timezone = Timezone.fromJS(getString(timezone));
        return new TimeShiftExpression({ operand: this, duration: duration, step: step, timezone: timezone });
    };
    Expression.prototype.timeRange = function (duration, step, timezone) {
        if (!(duration instanceof Duration))
            duration = Duration.fromJS(getString(duration));
        step = typeof step !== 'undefined' ? getNumber(step) : null;
        if (timezone && !(timezone instanceof Timezone))
            timezone = Timezone.fromJS(getString(timezone));
        return new TimeRangeExpression({ operand: this, duration: duration, step: step, timezone: timezone });
    };
    Expression.prototype.timePart = function (part, timezone) {
        if (timezone && !(timezone instanceof Timezone))
            timezone = Timezone.fromJS(getString(timezone));
        return new TimePartExpression({ operand: this, part: getString(part), timezone: timezone });
    };
    Expression.prototype.cast = function (outputType) {
        return new CastExpression({ operand: this, outputType: getString(outputType) });
    };
    Expression.prototype.cardinality = function () {
        return new CardinalityExpression({ operand: this });
    };
    Expression.prototype.filter = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new FilterExpression({ operand: this, expression: ex });
    };
    Expression.prototype.split = function (splits, name, dataName) {
        if (arguments.length === 3 ||
            ((arguments.length === 2 || arguments.length === 1) && (typeof splits === 'string' || typeof splits.op === 'string'))) {
            name = arguments.length === 1 ? 'split' : getString(name);
            var realSplits = Object.create(null);
            realSplits[name] = splits;
            splits = realSplits;
        }
        else {
            dataName = name;
        }
        var parsedSplits = Object.create(null);
        for (var k in splits) {
            if (!hasOwnProp(splits, k))
                continue;
            var ex = splits[k];
            parsedSplits[k] = ex instanceof Expression ? ex : Expression.fromJSLoose(ex);
        }
        dataName = dataName ? getString(dataName) : getDataName(this);
        if (!dataName)
            throw new Error("could not guess data name in `split`, please provide one explicitly");
        return new SplitExpression({ operand: this, splits: parsedSplits, dataName: dataName });
    };
    Expression.prototype.apply = function (name, ex) {
        if (arguments.length < 2)
            throw new Error('invalid arguments to .apply, did you forget to specify a name?');
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new ApplyExpression({ operand: this, name: getString(name), expression: ex });
    };
    Expression.prototype.sort = function (ex, direction) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new SortExpression({ operand: this, expression: ex, direction: direction ? getString(direction) : null });
    };
    Expression.prototype.limit = function (value) {
        return new LimitExpression({ operand: this, value: getNumber(value) });
    };
    Expression.prototype.select = function () {
        var attributes = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            attributes[_i] = arguments[_i];
        }
        attributes = (attributes.length === 1 && Array.isArray(attributes[0])) ? attributes[0] : attributes.map(getString);
        return new SelectExpression({ operand: this, attributes: attributes });
    };
    Expression.prototype.count = function () {
        if (arguments.length)
            throw new Error('.count() should not have arguments, did you want to .filter().count() ?');
        return new CountExpression({ operand: this });
    };
    Expression.prototype.sum = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new SumExpression({ operand: this, expression: ex });
    };
    Expression.prototype.min = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new MinExpression({ operand: this, expression: ex });
    };
    Expression.prototype.max = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new MaxExpression({ operand: this, expression: ex });
    };
    Expression.prototype.average = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new AverageExpression({ operand: this, expression: ex });
    };
    Expression.prototype.countDistinct = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new CountDistinctExpression({ operand: this, expression: ex });
    };
    Expression.prototype.quantile = function (ex, value, tuning) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new QuantileExpression({ operand: this, expression: ex, value: getNumber(value), tuning: tuning ? getString(tuning) : null });
    };
    Expression.prototype.collect = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new CollectExpression({ operand: this, expression: ex });
    };
    Expression.prototype.customAggregate = function (custom) {
        return new CustomAggregateExpression({ operand: this, custom: getString(custom) });
    };
    Expression.prototype.join = function (ex) {
        if (!(ex instanceof Expression))
            ex = Expression.fromJSLoose(ex);
        return new JoinExpression({ operand: this, expression: ex });
    };
    Expression.prototype.needsEnvironment = function () {
        return false;
    };
    Expression.prototype.defineEnvironment = function (environment) {
        if (!environment.timezone)
            environment = { timezone: Timezone.UTC };
        if (typeof environment.timezone === 'string')
            environment = { timezone: Timezone.fromJS(environment.timezone) };
        return this.substitute(function (ex) {
            if (ex.needsEnvironment()) {
                return ex.defineEnvironment(environment);
            }
            return null;
        });
    };
    Expression.prototype.referenceCheck = function (context) {
        return this.changeInTypeContext(getFullTypeFromDatum(context));
    };
    Expression.prototype.definedInTypeContext = function (typeContext) {
        try {
            this.changeInTypeContext(typeContext);
        }
        catch (e) {
            return false;
        }
        return true;
    };
    Expression.prototype.referenceCheckInTypeContext = function (typeContext) {
        console.warn("referenceCheckInTypeContext is deprecated, use changeInTypeContext instead");
        return this.changeInTypeContext(typeContext);
    };
    Expression.prototype.changeInTypeContext = function (typeContext) {
        return this.substitute(function (ex, index, depth, nestDiff, typeContext) {
            if (ex instanceof RefExpression) {
                return ex.changeInTypeContext(typeContext);
            }
            return null;
        }, typeContext);
    };
    Expression.prototype.updateTypeContext = function (typeContext, extra) {
        return typeContext;
    };
    Expression.prototype.updateTypeContextIfNeeded = function (typeContext, extra) {
        return typeContext ? this.updateTypeContext(typeContext, extra) : null;
    };
    Expression.prototype.resolve = function (context, ifNotFound) {
        if (ifNotFound === void 0) { ifNotFound = 'throw'; }
        var expressions = Object.create(null);
        for (var k in context) {
            if (!hasOwnProp(context, k))
                continue;
            var value = context[k];
            if (value instanceof External) {
                expressions[k] = new ExternalExpression({ external: value });
            }
            else if (value instanceof Expression) {
                expressions[k] = value;
            }
            else {
                expressions[k] = new LiteralExpression({ value: value });
            }
        }
        return this.resolveWithExpressions(expressions, ifNotFound);
    };
    Expression.prototype.resolveWithExpressions = function (expressions, ifNotFound) {
        if (ifNotFound === void 0) { ifNotFound = 'throw'; }
        return this.substitute(function (ex, index, depth, nestDiff) {
            if (ex instanceof RefExpression) {
                var nest = ex.nest, ignoreCase = ex.ignoreCase, name_3 = ex.name;
                if (nestDiff === nest) {
                    var foundExpression = null;
                    var valueFound = false;
                    var property = ignoreCase ? RefExpression.findPropertyCI(expressions, name_3) : RefExpression.findProperty(expressions, name_3);
                    if (property != null) {
                        foundExpression = expressions[property];
                        valueFound = true;
                    }
                    if (foundExpression instanceof ExternalExpression) {
                        var mode = foundExpression.external.mode;
                        if (mode === 'split') {
                            return ex;
                        }
                        if (nest > 0 && mode !== 'raw') {
                            return ex;
                        }
                    }
                    if (valueFound) {
                        return foundExpression;
                    }
                    else if (ifNotFound === 'throw') {
                        throw new Error("could not resolve " + ex + " because is was not in the context");
                    }
                    else if (ifNotFound === 'null') {
                        return Expression.NULL;
                    }
                    else if (ifNotFound === 'leave') {
                        return ex;
                    }
                }
                else if (nestDiff < nest) {
                    throw new Error("went too deep during resolve on: " + ex);
                }
            }
            return null;
        });
    };
    Expression.prototype.resolved = function () {
        return this.every(function (ex, index, depth, nestDiff) {
            return (ex instanceof RefExpression) ? ex.nest <= nestDiff : null;
        });
    };
    Expression.prototype.resolvedWithoutExternals = function () {
        return this.every(function (ex, index, depth, nestDiff) {
            if (ex instanceof ExternalExpression)
                return false;
            return (ex instanceof RefExpression) ? ex.nest <= nestDiff : null;
        });
    };
    Expression.prototype.noRefs = function () {
        return this.every(function (ex) {
            if (ex instanceof RefExpression)
                return false;
            return null;
        });
    };
    Expression.prototype.isAggregate = function () {
        return false;
    };
    Expression.prototype.decomposeAverage = function (countEx) {
        return this.substitute(function (ex) {
            if (ex instanceof AverageExpression) {
                return ex.decomposeAverage(countEx);
            }
            return null;
        });
    };
    Expression.prototype.distribute = function () {
        return this.substitute(function (ex, index) {
            if (index === 0)
                return null;
            var distributedEx = ex.distribute();
            if (distributedEx === ex)
                return null;
            return distributedEx;
        }).simplify();
    };
    Expression.prototype.maxPossibleSplitValues = function () {
        return this.type === 'BOOLEAN' ? 3 : Infinity;
    };
    Expression.prototype._initialPrepare = function (context, environment) {
        return this.defineEnvironment(environment)
            .referenceCheck(context)
            .resolve(context)
            .simplify();
    };
    Expression.prototype.simulate = function (context, options) {
        if (context === void 0) { context = {}; }
        if (options === void 0) { options = {}; }
        failIfIntrospectNeededInDatum(context);
        var readyExpression = this._initialPrepare(context, options);
        if (readyExpression instanceof ExternalExpression) {
            readyExpression = readyExpression.unsuppress();
        }
        return readyExpression._computeResolvedSimulate(options, []);
    };
    Expression.prototype.simulateQueryPlan = function (context, options) {
        if (context === void 0) { context = {}; }
        if (options === void 0) { options = {}; }
        failIfIntrospectNeededInDatum(context);
        var readyExpression = this._initialPrepare(context, options);
        if (readyExpression instanceof ExternalExpression) {
            readyExpression = readyExpression.unsuppress();
        }
        var simulatedQueryGroups = [];
        readyExpression._computeResolvedSimulate(options, simulatedQueryGroups);
        return simulatedQueryGroups;
    };
    Expression.prototype._computeResolvedSimulate = function (options, simulatedQueryGroups) {
        var _a = options.maxComputeCycles, maxComputeCycles = _a === void 0 ? 5 : _a, _b = options.maxQueries, maxQueries = _b === void 0 ? 500 : _b, maxRows = options.maxRows, _c = options.concurrentQueryLimit, concurrentQueryLimit = _c === void 0 ? Infinity : _c;
        var ex = this;
        var readyExternals = ex.getReadyExternals(concurrentQueryLimit);
        var computeCycles = 0;
        var queries = 0;
        var _loop_1 = function () {
            var simulatedQueryGroup = [];
            fillExpressionExternalAlteration(readyExternals, function (external, terminal) {
                if (queries < maxQueries) {
                    queries++;
                    return external.simulateValue(terminal, simulatedQueryGroup);
                }
                else {
                    queries++;
                    return null;
                }
            });
            simulatedQueryGroups.push(simulatedQueryGroup);
            ex = ex.applyReadyExternals(readyExternals);
            var literalValue = ex.getLiteralValue();
            if (maxRows && literalValue instanceof Dataset) {
                ex = r(literalValue.depthFirstTrimTo(maxRows));
            }
            readyExternals = ex.getReadyExternals(concurrentQueryLimit);
            computeCycles++;
        };
        while (Object.keys(readyExternals).length > 0 && computeCycles < maxComputeCycles && queries < maxQueries) {
            _loop_1();
        }
        return ex.getLiteralValue();
    };
    Expression.prototype.compute = function (context, options, computeContext) {
        var _this = this;
        if (context === void 0) { context = {}; }
        if (options === void 0) { options = {}; }
        if (computeContext === void 0) { computeContext = {}; }
        return Promise.resolve(null)
            .then(function () {
            return introspectDatum(context);
        })
            .then(function (introspectedContext) {
            var readyExpression = _this._initialPrepare(introspectedContext, options);
            if (readyExpression instanceof ExternalExpression) {
                readyExpression = readyExpression.unsuppress();
            }
            return readyExpression._computeResolved(options, computeContext);
        });
    };
    Expression.prototype.computeStream = function (context, options, computeContext) {
        var _this = this;
        if (context === void 0) { context = {}; }
        if (options === void 0) { options = {}; }
        var pt = new PassThrough({ objectMode: true });
        var rawQueries = options.rawQueries;
        introspectDatum(context)
            .then(function (introspectedContext) {
            var readyExpression = _this._initialPrepare(introspectedContext, options);
            if (readyExpression instanceof ExternalExpression) {
                pipeWithError(readyExpression.external.queryValueStream(true, rawQueries, computeContext), pt);
                return;
            }
            readyExpression._computeResolved(options, computeContext)
                .then(function (v) {
                var i = iteratorFactory(v);
                var bit;
                while (bit = i()) {
                    pt.write(bit);
                }
                pt.end();
            });
        })
            .catch(function (e) {
            pt.emit('error', e);
        });
        return pt;
    };
    Expression.prototype._computeResolved = function (options, computeContext) {
        var _this = this;
        var rawQueries = options.rawQueries, _a = options.maxComputeCycles, maxComputeCycles = _a === void 0 ? 5 : _a, _b = options.maxQueries, maxQueries = _b === void 0 ? 500 : _b, maxRows = options.maxRows, _c = options.concurrentQueryLimit, concurrentQueryLimit = _c === void 0 ? Infinity : _c;
        var ex = this;
        var readyExternals = ex.getReadyExternals(concurrentQueryLimit);
        var computeCycles = 0;
        var queriesMade = 0;
        return promiseWhile(function () { return Object.keys(readyExternals).length > 0 && computeCycles < maxComputeCycles && queriesMade < maxQueries; }, function () { return tslib_1.__awaiter(_this, void 0, void 0, function () {
            var readyExternalsFilled, literalValue;
            return tslib_1.__generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4, fillExpressionExternalAlterationAsync(readyExternals, function (external, terminal) {
                            if (queriesMade < maxQueries) {
                                queriesMade++;
                                return external.queryValue(terminal, rawQueries, computeContext);
                            }
                            else {
                                queriesMade++;
                                return Promise.resolve(null);
                            }
                        })];
                    case 1:
                        readyExternalsFilled = _a.sent();
                        ex = ex.applyReadyExternals(readyExternalsFilled);
                        literalValue = ex.getLiteralValue();
                        if (maxRows && literalValue instanceof Dataset) {
                            ex = r(literalValue.depthFirstTrimTo(maxRows));
                        }
                        readyExternals = ex.getReadyExternals(concurrentQueryLimit);
                        computeCycles++;
                        return [2];
                }
            });
        }); })
            .then(function () {
            if (!ex.isOp('literal'))
                throw new Error("something went wrong, did not get literal: " + ex);
            return ex.getLiteralValue();
        });
    };
    Expression.defaultParserTimezone = Timezone.UTC;
    Expression.classMap = {};
    return Expression;
}());
exports.Expression = Expression;
var ChainableExpression = (function (_super) {
    tslib_1.__extends(ChainableExpression, _super);
    function ChainableExpression(value, dummy) {
        if (dummy === void 0) { dummy = null; }
        var _this = _super.call(this, value, dummy) || this;
        _this.operand = value.operand || Expression._;
        return _this;
    }
    ChainableExpression.jsToValue = function (js) {
        var value = Expression.jsToValue(js);
        value.operand = js.operand ? Expression.fromJS(js.operand) : Expression._;
        return value;
    };
    ChainableExpression.prototype._checkTypeAgainstTypes = function (name, type, neededTypes) {
        if (type && type !== 'NULL' && neededTypes.indexOf(type) === -1) {
            if (neededTypes.length === 1) {
                throw new Error(this.op + " must have " + name + " of type " + neededTypes[0] + " (is " + type + ")");
            }
            else {
                throw new Error(this.op + " must have " + name + " of type " + neededTypes.join(' or ') + " (is " + type + ")");
            }
        }
    };
    ChainableExpression.prototype._checkOperandTypes = function () {
        var neededTypes = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            neededTypes[_i] = arguments[_i];
        }
        this._checkTypeAgainstTypes('operand', Set.unwrapSetType(this.operand.type), neededTypes);
    };
    ChainableExpression.prototype._checkOperandTypesStrict = function () {
        var neededTypes = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            neededTypes[_i] = arguments[_i];
        }
        this._checkTypeAgainstTypes('operand', this.operand.type, neededTypes);
    };
    ChainableExpression.prototype._bumpOperandToTime = function () {
        if (this.operand.type === 'STRING') {
            this.operand = this.operand.upgradeToType('TIME');
        }
    };
    ChainableExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.operand = this.operand;
        return value;
    };
    ChainableExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        if (!this.operand.equals(Expression._)) {
            js.operand = this.operand.toJS();
        }
        return js;
    };
    ChainableExpression.prototype._toStringParameters = function (indent) {
        return [];
    };
    ChainableExpression.prototype.toString = function (indent) {
        return this.operand.toString(indent) + "." + this.op + "(" + this._toStringParameters(indent).join(',') + ")";
    };
    ChainableExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.operand.equals(other.operand);
    };
    ChainableExpression.prototype.changeOperand = function (operand) {
        if (this.operand === operand || this.operand.equals(operand))
            return this;
        var value = this.valueOf();
        value.operand = operand;
        delete value.simple;
        return Expression.fromValue(value);
    };
    ChainableExpression.prototype.swapWithOperand = function () {
        var operand = this.operand;
        if (operand instanceof ChainableExpression) {
            return operand.changeOperand(this.changeOperand(operand.operand));
        }
        else {
            throw new Error('operand must be chainable');
        }
    };
    ChainableExpression.prototype.getAction = function () {
        return this.changeOperand(Expression._);
    };
    ChainableExpression.prototype.getHeadOperand = function () {
        var iter = this.operand;
        while (iter instanceof ChainableExpression)
            iter = iter.operand;
        return iter;
    };
    ChainableExpression.prototype.getArgumentExpressions = function () {
        return [];
    };
    ChainableExpression.prototype.expressionCount = function () {
        var sum = _super.prototype.expressionCount.call(this) + this.operand.expressionCount();
        this.getArgumentExpressions().forEach(function (ex) { return sum += ex.expressionCount(); });
        return sum;
    };
    ChainableExpression.prototype.argumentsResolved = function () {
        return this.getArgumentExpressions().every(function (ex) { return ex.resolved(); });
    };
    ChainableExpression.prototype.argumentsResolvedWithoutExternals = function () {
        return this.getArgumentExpressions().every(function (ex) { return ex.resolvedWithoutExternals(); });
    };
    ChainableExpression.prototype.getFn = function () {
        var _this = this;
        return function (d) { return _this.calc(d); };
    };
    ChainableExpression.prototype._calcChainableHelper = function (operandValue) {
        throw runtimeAbstract();
    };
    ChainableExpression.prototype.fullyDefined = function () {
        return this.operand.isOp('literal');
    };
    ChainableExpression.prototype.calc = function (datum) {
        return this._calcChainableHelper(this.operand.calc(datum));
    };
    ChainableExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw runtimeAbstract();
    };
    ChainableExpression.prototype.getJS = function (datumVar) {
        return this._getJSChainableHelper(this.operand.getJS(datumVar));
    };
    ChainableExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        throw runtimeAbstract();
    };
    ChainableExpression.prototype.getSQL = function (dialect) {
        return this._getSQLChainableHelper(dialect, this.operand.getSQL(dialect));
    };
    ChainableExpression.prototype.pushIntoExternal = function () {
        var operand = this.operand;
        if (operand instanceof ExternalExpression) {
            return operand.addExpression(this.getAction());
        }
        return null;
    };
    ChainableExpression.prototype.specialSimplify = function () {
        return this;
    };
    ChainableExpression.prototype.simplify = function () {
        if (this.simple)
            return this;
        var simpler = this.changeOperand(this.operand.simplify());
        if (simpler.fullyDefined()) {
            return r(simpler.calc({}));
        }
        var specialSimpler = simpler.specialSimplify();
        if (specialSimpler === simpler) {
            simpler = specialSimpler.markSimple();
        }
        else {
            simpler = specialSimpler.simplify();
        }
        if (simpler instanceof ChainableExpression) {
            var pushedInExternal = simpler.pushIntoExternal();
            if (pushedInExternal)
                return pushedInExternal;
        }
        return simpler;
    };
    ChainableExpression.prototype.isNester = function () {
        return false;
    };
    ChainableExpression.prototype._everyHelper = function (iter, thisArg, indexer, depth, nestDiff) {
        var pass = iter.call(thisArg, this, indexer.index, depth, nestDiff);
        if (pass != null) {
            return pass;
        }
        else {
            indexer.index++;
        }
        depth++;
        var operand = this.operand;
        if (!operand._everyHelper(iter, thisArg, indexer, depth, nestDiff))
            return false;
        var nestDiffNext = nestDiff + Number(this.isNester());
        return this.getArgumentExpressions().every(function (ex) { return ex._everyHelper(iter, thisArg, indexer, depth, nestDiffNext); });
    };
    ChainableExpression.prototype._substituteHelper = function (substitutionFn, indexer, depth, nestDiff, typeContext) {
        var sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff, typeContext);
        if (sub) {
            indexer.index += this.expressionCount();
            return {
                expression: sub,
                typeContext: sub.updateTypeContextIfNeeded(typeContext)
            };
        }
        else {
            indexer.index++;
        }
        depth++;
        var operandSubs = this.operand._substituteHelper(substitutionFn, indexer, depth, nestDiff, typeContext);
        var updatedThis = this.changeOperand(operandSubs.expression);
        return {
            expression: updatedThis,
            typeContext: updatedThis.updateTypeContextIfNeeded(operandSubs.typeContext)
        };
    };
    return ChainableExpression;
}(Expression));
exports.ChainableExpression = ChainableExpression;
var ChainableUnaryExpression = (function (_super) {
    tslib_1.__extends(ChainableUnaryExpression, _super);
    function ChainableUnaryExpression(value, dummy) {
        if (dummy === void 0) { dummy = null; }
        var _this = _super.call(this, value, dummy) || this;
        if (!value.expression)
            throw new Error("must have an expression");
        _this.expression = value.expression;
        return _this;
    }
    ChainableUnaryExpression.jsToValue = function (js) {
        var value = ChainableExpression.jsToValue(js);
        value.expression = Expression.fromJS(js.expression);
        return value;
    };
    ChainableUnaryExpression.prototype._checkExpressionTypes = function () {
        var neededTypes = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            neededTypes[_i] = arguments[_i];
        }
        this._checkTypeAgainstTypes('expression', Set.unwrapSetType(this.expression.type), neededTypes);
    };
    ChainableUnaryExpression.prototype._checkExpressionTypesStrict = function () {
        var neededTypes = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            neededTypes[_i] = arguments[_i];
        }
        this._checkTypeAgainstTypes('expression', this.expression.type, neededTypes);
    };
    ChainableUnaryExpression.prototype._checkOperandExpressionTypesAlign = function () {
        var operandType = Set.unwrapSetType(this.operand.type);
        var expressionType = Set.unwrapSetType(this.expression.type);
        if (!operandType || operandType === 'NULL' || !expressionType || expressionType === 'NULL' || operandType === expressionType)
            return;
        throw new Error(this.op + " must have matching types (are " + this.operand.type + ", " + this.expression.type + ")");
    };
    ChainableUnaryExpression.prototype._bumpOperandExpressionToTime = function () {
        if (this.expression.type === 'TIME' && this.operand.type === 'STRING') {
            this.operand = this.operand.upgradeToType('TIME');
        }
        if (this.operand.type === 'TIME' && this.expression.type === 'STRING') {
            this.expression = this.expression.upgradeToType('TIME');
        }
    };
    ChainableUnaryExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.expression = this.expression;
        return value;
    };
    ChainableUnaryExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.expression = this.expression.toJS();
        return js;
    };
    ChainableUnaryExpression.prototype._toStringParameters = function (indent) {
        return [this.expression.toString(indent)];
    };
    ChainableUnaryExpression.prototype.toString = function (indent) {
        return this.operand.toString(indent) + "." + this.op + "(" + this._toStringParameters(indent).join(',') + ")";
    };
    ChainableUnaryExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.expression.equals(other.expression);
    };
    ChainableUnaryExpression.prototype.changeExpression = function (expression) {
        if (this.expression === expression || this.expression.equals(expression))
            return this;
        var value = this.valueOf();
        value.expression = expression;
        delete value.simple;
        return Expression.fromValue(value);
    };
    ChainableUnaryExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        throw runtimeAbstract();
    };
    ChainableUnaryExpression.prototype.fullyDefined = function () {
        return this.operand.isOp('literal') && this.expression.isOp('literal');
    };
    ChainableUnaryExpression.prototype.calc = function (datum) {
        return this._calcChainableUnaryHelper(this.operand.calc(datum), this.isNester() ? null : this.expression.calc(datum));
    };
    ChainableUnaryExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        throw runtimeAbstract();
    };
    ChainableUnaryExpression.prototype.getJS = function (datumVar) {
        return this._getJSChainableUnaryHelper(this.operand.getJS(datumVar), this.expression.getJS(datumVar));
    };
    ChainableUnaryExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        throw runtimeAbstract();
    };
    ChainableUnaryExpression.prototype.getSQL = function (dialect) {
        return this._getSQLChainableUnaryHelper(dialect, this.operand.getSQL(dialect), this.expression.getSQL(dialect));
    };
    ChainableUnaryExpression.prototype.getExpressionList = function () {
        var _a = this, op = _a.op, operand = _a.operand, expression = _a.expression;
        var expressionList = [expression];
        var iter = operand;
        while (iter.op === op) {
            expressionList.unshift(iter.expression);
            iter = iter.operand;
        }
        expressionList.unshift(iter);
        return expressionList;
    };
    ChainableUnaryExpression.prototype.isCommutative = function () {
        return false;
    };
    ChainableUnaryExpression.prototype.isAssociative = function () {
        return false;
    };
    ChainableUnaryExpression.prototype.associateLeft = function () {
        if (!this.isAssociative())
            return null;
        var _a = this, op = _a.op, operand = _a.operand, expression = _a.expression;
        if (op !== expression.op)
            return null;
        var MyClass = this.constructor;
        return new MyClass({
            operand: new MyClass({
                operand: operand,
                expression: expression.operand
            }),
            expression: expression.expression
        });
    };
    ChainableUnaryExpression.prototype.associateRightIfSimpler = function () {
        if (!this.isAssociative())
            return null;
        var _a = this, op = _a.op, operand = _a.operand, expression = _a.expression;
        if (op !== operand.op)
            return null;
        var MyClass = this.constructor;
        var simpleExpression = new MyClass({
            operand: operand.expression,
            expression: expression
        }).simplify();
        if (simpleExpression instanceof LiteralExpression) {
            return new MyClass({
                operand: operand.operand,
                expression: simpleExpression
            }).simplify();
        }
        else {
            return null;
        }
    };
    ChainableUnaryExpression.prototype.pushIntoExternal = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (operand instanceof ExternalExpression) {
            return operand.addExpression(this.getAction());
        }
        if (expression instanceof ExternalExpression) {
            return expression.prePush(this.changeExpression(Expression._));
        }
        return null;
    };
    ChainableUnaryExpression.prototype.simplify = function () {
        if (this.simple)
            return this;
        var simpleOperand = this.operand.simplify();
        var simpleExpression = this.expression.simplify();
        var simpler = this.changeOperand(simpleOperand).changeExpression(simpleExpression);
        if (simpler.fullyDefined())
            return r(simpler.calc({}));
        if (this.isCommutative() && simpleOperand instanceof LiteralExpression) {
            var MyClass = this.constructor;
            var myValue = this.valueOf();
            myValue.operand = simpleExpression;
            myValue.expression = simpleOperand;
            return new MyClass(myValue).simplify();
        }
        var assLeft = simpler.associateLeft();
        if (assLeft)
            return assLeft.simplify();
        if (simpler instanceof ChainableUnaryExpression) {
            var specialSimpler = simpler.specialSimplify();
            if (specialSimpler !== simpler) {
                return specialSimpler.simplify();
            }
            else {
                simpler = specialSimpler;
            }
            if (simpler instanceof ChainableUnaryExpression) {
                var assRight = simpler.associateRightIfSimpler();
                if (assRight)
                    return assRight;
            }
        }
        simpler = simpler.markSimple();
        if (simpler instanceof ChainableExpression) {
            var pushedInExternal = simpler.pushIntoExternal();
            if (pushedInExternal)
                return pushedInExternal;
        }
        return simpler;
    };
    ChainableUnaryExpression.prototype.getArgumentExpressions = function () {
        return [this.expression];
    };
    ChainableUnaryExpression.prototype._substituteHelper = function (substitutionFn, indexer, depth, nestDiff, typeContext) {
        var sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff);
        if (sub) {
            indexer.index += this.expressionCount();
            return {
                expression: sub,
                typeContext: sub.updateTypeContextIfNeeded(typeContext)
            };
        }
        else {
            indexer.index++;
        }
        depth++;
        var operandSubs = this.operand._substituteHelper(substitutionFn, indexer, depth, nestDiff, typeContext);
        var nestDiffNext = nestDiff + Number(this.isNester());
        var expressionSubs = this.expression._substituteHelper(substitutionFn, indexer, depth, nestDiffNext, this.isNester() ? operandSubs.typeContext : typeContext);
        var updatedThis = this.changeOperand(operandSubs.expression).changeExpression(expressionSubs.expression);
        return {
            expression: updatedThis,
            typeContext: updatedThis.updateTypeContextIfNeeded(operandSubs.typeContext, expressionSubs.typeContext)
        };
    };
    return ChainableUnaryExpression;
}(ChainableExpression));
exports.ChainableUnaryExpression = ChainableUnaryExpression;
var LiteralExpression = (function (_super) {
    tslib_1.__extends(LiteralExpression, _super);
    function LiteralExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        var value = parameters.value;
        _this.value = value;
        _this._ensureOp("literal");
        if (typeof _this.value === 'undefined') {
            throw new TypeError("must have a `value`");
        }
        _this.type = getValueType(value);
        _this.simple = true;
        return _this;
    }
    LiteralExpression.fromJS = function (parameters) {
        var value = {
            op: parameters.op,
            type: parameters.type
        };
        if (!hasOwnProp(parameters, 'value'))
            throw new Error('literal expression must have value');
        var v = parameters.value;
        if (isImmutableClass(v)) {
            value.value = v;
        }
        else {
            value.value = valueFromJS(v, parameters.type);
        }
        return new LiteralExpression(value);
    };
    LiteralExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.value = this.value;
        if (this.type)
            value.type = this.type;
        return value;
    };
    LiteralExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        if (this.value && this.value.toJS) {
            js.value = this.value.toJS();
            js.type = Set.isSetType(this.type) ? 'SET' : this.type;
        }
        else {
            js.value = this.value;
            if (this.type === 'TIME')
                js.type = 'TIME';
        }
        return js;
    };
    LiteralExpression.prototype.toString = function () {
        var value = this.value;
        if (value instanceof Dataset && value.basis()) {
            return 'ply()';
        }
        else if (this.type === 'STRING') {
            return JSON.stringify(value);
        }
        else {
            return String(value);
        }
    };
    LiteralExpression.prototype.getFn = function () {
        var value = this.value;
        return function () { return value; };
    };
    LiteralExpression.prototype.calc = function (datum) {
        return this.value;
    };
    LiteralExpression.prototype.getJS = function (datumVar) {
        return JSON.stringify(this.value);
    };
    LiteralExpression.prototype.getSQL = function (dialect) {
        var value = this.value;
        if (value === null)
            return dialect.nullConstant();
        switch (this.type) {
            case 'STRING':
                return dialect.escapeLiteral(value);
            case 'BOOLEAN':
                return dialect.booleanToSQL(value);
            case 'NUMBER':
                return dialect.numberToSQL(value);
            case 'NUMBER_RANGE':
                return "" + dialect.numberToSQL(value.start);
            case 'TIME':
                return dialect.timeToSQL(value);
            case 'TIME_RANGE':
                return "" + dialect.timeToSQL(value.start);
            case 'STRING_RANGE':
                return dialect.escapeLiteral(value.start);
            case 'SET/STRING':
            case 'SET/NUMBER':
            case 'SET/NUMBER_RANGE':
            case 'SET/TIME_RANGE':
                return '<DUMMY>';
            default:
                throw new Error("currently unsupported type: " + this.type);
        }
    };
    LiteralExpression.prototype.equals = function (other) {
        if (!_super.prototype.equals.call(this, other) || this.type !== other.type)
            return false;
        if (this.value && this.type !== 'DATASET') {
            if (this.value.equals) {
                return this.value.equals(other.value);
            }
            else if (this.value.toISOString && other.value.toISOString) {
                return this.value.valueOf() === other.value.valueOf();
            }
            else {
                return this.value === other.value;
            }
        }
        else {
            return this.value === other.value;
        }
    };
    LiteralExpression.prototype.updateTypeContext = function (typeContext) {
        var value = this.value;
        if (value instanceof Dataset) {
            var newTypeContext = value.getFullType();
            newTypeContext.parent = typeContext;
            return newTypeContext;
        }
        return typeContext;
    };
    LiteralExpression.prototype.getLiteralValue = function () {
        return this.value;
    };
    LiteralExpression.prototype.maxPossibleSplitValues = function () {
        var value = this.value;
        return value instanceof Set ? value.size() : 1;
    };
    LiteralExpression.prototype.upgradeToType = function (targetType) {
        var _a = this, type = _a.type, value = _a.value;
        if (type === targetType)
            return this;
        if (type === 'STRING' && targetType === 'TIME') {
            var parse = parseISODate(value, Expression.defaultParserTimezone);
            if (!parse)
                throw new Error("can not upgrade " + value + " to TIME");
            return r(parse);
        }
        else if (type === 'STRING_RANGE' && targetType === 'TIME_RANGE') {
            var parseStart = parseISODate(value.start, Expression.defaultParserTimezone);
            if (!parseStart)
                throw new Error("can not upgrade " + value.start + " to TIME");
            var parseEnd = parseISODate(value.end, Expression.defaultParserTimezone);
            if (!parseEnd)
                throw new Error("can not upgrade " + value.end + " to TIME");
            return r(TimeRange.fromJS({
                start: parseStart,
                end: parseEnd,
                bounds: '[]'
            }));
        }
        throw new Error("can not upgrade " + type + " to " + targetType);
    };
    LiteralExpression.op = "Literal";
    return LiteralExpression;
}(Expression));
exports.LiteralExpression = LiteralExpression;
Expression.NULL = new LiteralExpression({ value: null });
Expression.ZERO = new LiteralExpression({ value: 0 });
Expression.ONE = new LiteralExpression({ value: 1 });
Expression.FALSE = new LiteralExpression({ value: false });
Expression.TRUE = new LiteralExpression({ value: true });
Expression.EMPTY_STRING = new LiteralExpression({ value: '' });
Expression.EMPTY_SET = new LiteralExpression({ value: Set.fromJS([]) });
Expression.register(LiteralExpression);
var POSSIBLE_TYPES = exports.POSSIBLE_TYPES = {
    'NULL': 1,
    'BOOLEAN': 1,
    'NUMBER': 1,
    'TIME': 1,
    'STRING': 1,
    'NUMBER_RANGE': 1,
    'TIME_RANGE': 1,
    'SET': 1,
    'SET/NULL': 1,
    'SET/BOOLEAN': 1,
    'SET/NUMBER': 1,
    'SET/TIME': 1,
    'SET/STRING': 1,
    'SET/NUMBER_RANGE': 1,
    'SET/TIME_RANGE': 1,
    'DATASET': 1
};
var GENERATIONS_REGEXP = /^\^+/;
var TYPE_REGEXP = /:([A-Z\/_]+)$/;
var RefExpression = (function (_super) {
    tslib_1.__extends(RefExpression, _super);
    function RefExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("ref");
        var name = parameters.name;
        if (typeof name !== 'string' || name.length === 0) {
            throw new TypeError("must have a nonempty `name`");
        }
        _this.name = name;
        var nest = parameters.nest;
        if (typeof nest !== 'number') {
            throw new TypeError("must have nest");
        }
        if (nest < 0) {
            throw new Error("nest must be non-negative");
        }
        _this.nest = nest;
        var myType = parameters.type;
        if (myType) {
            if (!RefExpression.validType(myType)) {
                throw new TypeError("unsupported type '" + myType + "'");
            }
            _this.type = myType;
        }
        _this.simple = true;
        _this.ignoreCase = parameters.ignoreCase;
        return _this;
    }
    RefExpression.fromJS = function (parameters) {
        var value = Expression.jsToValue(parameters);
        value.nest = parameters.nest || 0;
        value.name = parameters.name;
        value.ignoreCase = parameters.ignoreCase;
        return new RefExpression(value);
    };
    RefExpression.parse = function (str) {
        var refValue = { op: 'ref' };
        var match;
        match = str.match(GENERATIONS_REGEXP);
        if (match) {
            var nest = match[0].length;
            refValue.nest = nest;
            str = str.substr(nest);
        }
        else {
            refValue.nest = 0;
        }
        match = str.match(TYPE_REGEXP);
        if (match) {
            refValue.type = match[1];
            str = str.substr(0, str.length - match[0].length);
        }
        if (str[0] === '{' && str[str.length - 1] === '}') {
            str = str.substr(1, str.length - 2);
        }
        refValue.name = str;
        return new RefExpression(refValue);
    };
    RefExpression.validType = function (typeName) {
        return hasOwnProp(POSSIBLE_TYPES, typeName);
    };
    RefExpression.toJavaScriptSafeName = function (variableName) {
        if (!RefExpression.SIMPLE_NAME_REGEXP.test(variableName)) {
            variableName = variableName.replace(/\W/g, function (c) { return "$" + c.charCodeAt(0); });
        }
        return '_' + variableName;
    };
    RefExpression.findProperty = function (obj, key) {
        return hasOwnProp(obj, key) ? key : null;
    };
    RefExpression.findPropertyCI = function (obj, key) {
        var lowerKey = key.toLowerCase();
        if (obj == null)
            return null;
        return SimpleArray.find(Object.keys(obj), function (v) { return v.toLowerCase() === lowerKey; });
    };
    RefExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.name = this.name;
        value.nest = this.nest;
        if (this.type)
            value.type = this.type;
        if (this.ignoreCase)
            value.ignoreCase = true;
        return value;
    };
    RefExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.name = this.name;
        if (this.nest)
            js.nest = this.nest;
        if (this.type)
            js.type = this.type;
        if (this.ignoreCase)
            js.ignoreCase = true;
        return js;
    };
    RefExpression.prototype.toString = function () {
        var _a = this, name = _a.name, nest = _a.nest, type = _a.type, ignoreCase = _a.ignoreCase;
        var str = name;
        if (!RefExpression.SIMPLE_NAME_REGEXP.test(name)) {
            str = '{' + str + '}';
        }
        if (nest) {
            str = repeat('^', nest) + str;
        }
        if (type) {
            str += ':' + type;
        }
        return (ignoreCase ? 'i$' : '$') + str;
    };
    RefExpression.prototype.changeName = function (name) {
        var value = this.valueOf();
        value.name = name;
        return new RefExpression(value);
    };
    RefExpression.prototype.getFn = function () {
        var _a = this, name = _a.name, nest = _a.nest, ignoreCase = _a.ignoreCase;
        if (nest)
            throw new Error('can not getFn on a nested function');
        return function (d) {
            var property = ignoreCase ? RefExpression.findPropertyCI(d, name) : name;
            return property != null ? d[property] : null;
        };
    };
    RefExpression.prototype.calc = function (datum) {
        var _a = this, name = _a.name, nest = _a.nest, ignoreCase = _a.ignoreCase;
        if (nest)
            throw new Error('can not calc on a nested expression');
        var property = ignoreCase ? RefExpression.findPropertyCI(datum, name) : name;
        return property != null ? datum[property] : null;
    };
    RefExpression.prototype.getJS = function (datumVar) {
        var _a = this, name = _a.name, nest = _a.nest, ignoreCase = _a.ignoreCase;
        if (nest)
            throw new Error("can not call getJS on unresolved expression");
        if (ignoreCase)
            throw new Error("can not express ignore case as js expression");
        var expr;
        if (datumVar) {
            expr = datumVar.replace('[]', "[" + JSON.stringify(name) + "]");
        }
        else {
            expr = RefExpression.toJavaScriptSafeName(name);
        }
        switch (this.type) {
            case 'NUMBER':
                return "parseFloat(" + expr + ")";
            default:
                return expr;
        }
    };
    RefExpression.prototype.getSQL = function (dialect, minimal) {
        if (minimal === void 0) { minimal = false; }
        if (this.nest)
            throw new Error("can not call getSQL on unresolved expression: " + this);
        return dialect.maybeNamespacedName(this.name);
    };
    RefExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.name === other.name &&
            this.nest === other.nest &&
            this.ignoreCase === other.ignoreCase;
    };
    RefExpression.prototype.changeInTypeContext = function (typeContext) {
        var _a = this, nest = _a.nest, ignoreCase = _a.ignoreCase, name = _a.name;
        var myTypeContext = typeContext;
        for (var i = nest; i > 0; i--) {
            myTypeContext = myTypeContext.parent;
            if (!myTypeContext)
                throw new Error("went too deep on " + this);
        }
        var myName = ignoreCase ? RefExpression.findPropertyCI(myTypeContext.datasetType, name) : name;
        if (myName == null)
            throw new Error("could not resolve " + this);
        var nestDiff = 0;
        while (myTypeContext && !hasOwnProp(myTypeContext.datasetType, myName)) {
            myTypeContext = myTypeContext.parent;
            nestDiff++;
        }
        if (!myTypeContext) {
            throw new Error("could not resolve " + this);
        }
        var myFullType = myTypeContext.datasetType[myName];
        var myType = myFullType.type;
        if (this.type && this.type !== myType) {
            throw new TypeError("type mismatch in " + this + " (has: " + this.type + " needs: " + myType + ")");
        }
        if (!this.type || nestDiff > 0 || ignoreCase) {
            return new RefExpression({
                name: myName,
                nest: nest + nestDiff,
                type: myType
            });
        }
        else {
            return this;
        }
    };
    RefExpression.prototype.updateTypeContext = function (typeContext) {
        if (this.type !== 'DATASET')
            return typeContext;
        var _a = this, nest = _a.nest, name = _a.name;
        var myTypeContext = typeContext;
        for (var i = nest; i > 0; i--) {
            myTypeContext = myTypeContext.parent;
            if (!myTypeContext)
                throw new Error('went too deep on ' + this.toString());
        }
        var myFullType = myTypeContext.datasetType[name];
        return {
            parent: typeContext,
            type: 'DATASET',
            datasetType: myFullType.datasetType
        };
    };
    RefExpression.prototype.incrementNesting = function (by) {
        if (by === void 0) { by = 1; }
        var value = this.valueOf();
        value.nest += by;
        return new RefExpression(value);
    };
    RefExpression.prototype.upgradeToType = function (targetType) {
        var type = this.type;
        if (targetType === 'TIME' && (!type || type === 'STRING')) {
            return this.changeType(targetType);
        }
        return this;
    };
    RefExpression.prototype.toCaseInsensitive = function () {
        var value = this.valueOf();
        value.ignoreCase = true;
        return new RefExpression(value);
    };
    RefExpression.prototype.changeType = function (newType) {
        var value = this.valueOf();
        value.type = newType;
        return new RefExpression(value);
    };
    RefExpression.SIMPLE_NAME_REGEXP = /^([a-z_]\w*)$/i;
    RefExpression.op = "Ref";
    return RefExpression;
}(Expression));
exports.RefExpression = RefExpression;
Expression._ = new RefExpression({ name: '_', nest: 0 });
Expression.register(RefExpression);
var ExternalExpression = (function (_super) {
    tslib_1.__extends(ExternalExpression, _super);
    function ExternalExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        var external = parameters.external;
        if (!external)
            throw new Error('must have an external');
        _this.external = external;
        _this._ensureOp('external');
        _this.type = external.mode === 'value' ? external.getValueType() : 'DATASET';
        _this.simple = true;
        return _this;
    }
    ExternalExpression.fromJS = function (parameters) {
        var value = {
            op: parameters.op
        };
        value.external = External.fromJS(parameters.external);
        return new ExternalExpression(value);
    };
    ExternalExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.external = this.external;
        return value;
    };
    ExternalExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.external = this.external.toJS();
        return js;
    };
    ExternalExpression.prototype.toString = function () {
        return "E:" + this.external;
    };
    ExternalExpression.prototype.getFn = function () {
        throw new Error('should not call getFn on External');
    };
    ExternalExpression.prototype.calc = function (datum) {
        throw new Error('should not call calc on External');
    };
    ExternalExpression.prototype.getJS = function (datumVar) {
        throw new Error('should not call getJS on External');
    };
    ExternalExpression.prototype.getSQL = function (dialect) {
        throw new Error('should not call getSQL on External');
    };
    ExternalExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.external.equals(other.external);
    };
    ExternalExpression.prototype.updateTypeContext = function (typeContext) {
        var external = this.external;
        if (external.mode !== 'value') {
            var newTypeContext = this.external.getFullType();
            newTypeContext.parent = typeContext;
            return newTypeContext;
        }
        return typeContext;
    };
    ExternalExpression.prototype.unsuppress = function () {
        var value = this.valueOf();
        value.external = this.external.show();
        return new ExternalExpression(value);
    };
    ExternalExpression.prototype.addExpression = function (expression) {
        var newExternal = this.external.addExpression(expression);
        if (!newExternal)
            return null;
        return new ExternalExpression({ external: newExternal });
    };
    ExternalExpression.prototype.prePush = function (expression) {
        var newExternal = this.external.prePush(expression);
        if (!newExternal)
            return null;
        return new ExternalExpression({ external: newExternal });
    };
    ExternalExpression.prototype.maxPossibleSplitValues = function () {
        return Infinity;
    };
    ExternalExpression.op = "external";
    return ExternalExpression;
}(Expression));
exports.ExternalExpression = ExternalExpression;
Expression.register(ExternalExpression);
var HasTimezone = (function () {
    function HasTimezone() {
    }
    HasTimezone.prototype.getTimezone = function () {
        return this.timezone || Timezone.UTC;
    };
    HasTimezone.prototype.changeTimezone = function (timezone) {
        if (timezone.equals(this.timezone))
            return this;
        var value = this.valueOf();
        value.timezone = timezone;
        return Expression.fromValue(value);
    };
    HasTimezone.prototype.needsEnvironment = function () {
        return !this.timezone;
    };
    HasTimezone.prototype.defineEnvironment = function (environment) {
        if (!environment.timezone)
            environment = { timezone: Timezone.UTC };
        if (typeof environment.timezone === 'string')
            environment = { timezone: Timezone.fromJS(environment.timezone) };
        if (this.timezone || !environment.timezone)
            return this;
        return this.changeTimezone(environment.timezone).substitute(function (ex) {
            if (ex.needsEnvironment()) {
                return ex.defineEnvironment(environment);
            }
            return null;
        });
    };
    return HasTimezone;
}());
exports.HasTimezone = HasTimezone;
var Aggregate = (function () {
    function Aggregate() {
    }
    Aggregate.prototype.isAggregate = function () {
        return true;
    };
    Aggregate.prototype.isNester = function () {
        return true;
    };
    Aggregate.prototype.fullyDefined = function () {
        var expression = this.expression;
        return this.operand.isOp('literal') && (expression ? expression.resolved() : true);
    };
    return Aggregate;
}());
exports.Aggregate = Aggregate;
var AbsoluteExpression = (function (_super) {
    tslib_1.__extends(AbsoluteExpression, _super);
    function AbsoluteExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("absolute");
        _this._checkOperandTypes('NUMBER');
        _this.type = _this.operand.type;
        return _this;
    }
    AbsoluteExpression.fromJS = function (parameters) {
        return new AbsoluteExpression(ChainableExpression.jsToValue(parameters));
    };
    AbsoluteExpression.prototype._calcChainableHelper = function (operandValue) {
        if (operandValue == null)
            return null;
        return Set.crossUnary(operandValue, function (a) { return Math.abs(a); });
    };
    AbsoluteExpression.prototype._getJSChainableHelper = function (operandJS) {
        return "Math.abs(" + operandJS + ")";
    };
    AbsoluteExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return "ABS(" + operandSQL + ")";
    };
    AbsoluteExpression.prototype.specialSimplify = function () {
        var operand = this.operand;
        if (operand instanceof AbsoluteExpression)
            return operand;
        return this;
    };
    AbsoluteExpression.op = "Absolute";
    return AbsoluteExpression;
}(ChainableExpression));
exports.AbsoluteExpression = AbsoluteExpression;
Expression.register(AbsoluteExpression);
var AddExpression = (function (_super) {
    tslib_1.__extends(AddExpression, _super);
    function AddExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("add");
        _this._checkOperandTypes('NUMBER');
        _this._checkExpressionTypes('NUMBER');
        _this.type = 'NUMBER';
        return _this;
    }
    AddExpression.fromJS = function (parameters) {
        return new AddExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    AddExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) { return a + b; });
    };
    AddExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "+" + expressionJS + ")";
    };
    AddExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + "+" + expressionSQL + ")";
    };
    AddExpression.prototype.isCommutative = function () {
        return true;
    };
    AddExpression.prototype.isAssociative = function () {
        return true;
    };
    AddExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.ZERO))
            return operand;
        return this;
    };
    AddExpression.op = "Add";
    return AddExpression;
}(ChainableUnaryExpression));
exports.AddExpression = AddExpression;
Expression.register(AddExpression);
var IS_OR_OVERLAP = {
    'is': true,
    'overlap': true
};
var AndExpression = (function (_super) {
    tslib_1.__extends(AndExpression, _super);
    function AndExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("and");
        _this._checkOperandTypes('BOOLEAN');
        _this._checkExpressionTypes('BOOLEAN');
        _this.type = 'BOOLEAN';
        return _this;
    }
    AndExpression.fromJS = function (parameters) {
        return new AndExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    AndExpression.merge = function (ex1, ex2) {
        if (ex1.equals(ex2))
            return ex1;
        if (!IS_OR_OVERLAP[ex1.op] || !IS_OR_OVERLAP[ex2.op])
            return null;
        var _a = ex1, lhs1 = _a.operand, rhs1 = _a.expression;
        var _b = ex2, lhs2 = _b.operand, rhs2 = _b.expression;
        if (!lhs1.equals(lhs2) || !Set.isAtomicType(lhs1.type) || !rhs1.isOp('literal') || !rhs2.isOp('literal'))
            return null;
        var intersect = Set.intersectCover(rhs1.getLiteralValue(), rhs2.getLiteralValue());
        if (intersect === null)
            return null;
        return lhs1.overlap(r(intersect)).simplify();
    };
    AndExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) { return a && b; });
    };
    AndExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "&&" + expressionJS + ")";
    };
    AndExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + " AND " + expressionSQL + ")";
    };
    AndExpression.prototype.isCommutative = function () {
        return true;
    };
    AndExpression.prototype.isAssociative = function () {
        return true;
    };
    AndExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.FALSE))
            return Expression.FALSE;
        if (expression.equals(Expression.TRUE))
            return operand;
        if (operand instanceof AndExpression) {
            var andExpressions = operand.getExpressionList();
            for (var i = 0; i < andExpressions.length; i++) {
                var andExpression = andExpressions[i];
                var mergedExpression = AndExpression.merge(andExpression, expression);
                if (mergedExpression) {
                    andExpressions[i] = mergedExpression;
                    return Expression.and(andExpressions).simplify();
                }
            }
        }
        else {
            var mergedExpression = AndExpression.merge(operand, expression);
            if (mergedExpression)
                return mergedExpression;
        }
        return this;
    };
    AndExpression.prototype.extractFromAnd = function (matchFn) {
        if (!this.simple)
            return this.simplify().extractFromAnd(matchFn);
        var andExpressions = this.getExpressionList();
        var includedExpressions = [];
        var excludedExpressions = [];
        for (var _i = 0, andExpressions_1 = andExpressions; _i < andExpressions_1.length; _i++) {
            var ex = andExpressions_1[_i];
            if (matchFn(ex)) {
                includedExpressions.push(ex);
            }
            else {
                excludedExpressions.push(ex);
            }
        }
        return {
            extract: Expression.and(includedExpressions),
            rest: Expression.and(excludedExpressions)
        };
    };
    AndExpression.op = "And";
    return AndExpression;
}(ChainableUnaryExpression));
exports.AndExpression = AndExpression;
Expression.register(AndExpression);
var ApplyExpression = (function (_super) {
    tslib_1.__extends(ApplyExpression, _super);
    function ApplyExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.name = parameters.name;
        _this._ensureOp("apply");
        _this._checkOperandTypes('DATASET');
        _this.type = 'DATASET';
        return _this;
    }
    ApplyExpression.fromJS = function (parameters) {
        var value = ChainableUnaryExpression.jsToValue(parameters);
        value.name = parameters.name;
        return new ApplyExpression(value);
    };
    ApplyExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.name = this.name;
        return value;
    };
    ApplyExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.name = this.name;
        return js;
    };
    ApplyExpression.prototype.updateTypeContext = function (typeContext, expressionTypeContext) {
        var exprType = this.expression.type;
        typeContext.datasetType[this.name] = exprType === 'DATASET' ? expressionTypeContext : { type: exprType };
        return typeContext;
    };
    ApplyExpression.prototype._toStringParameters = function (indent) {
        var name = this.name;
        if (!RefExpression.SIMPLE_NAME_REGEXP.test(name))
            name = JSON.stringify(name);
        return [name, this.expression.toString(indent)];
    };
    ApplyExpression.prototype.toString = function (indent) {
        if (indent == null)
            return _super.prototype.toString.call(this);
        var param;
        if (this.expression.type === 'DATASET') {
            param = '\n    ' + this._toStringParameters(indent + 2).join(',\n    ') + '\n  ';
        }
        else {
            param = this._toStringParameters(indent).join(',');
        }
        var actionStr = indentBy("  .apply(" + param + ")", indent);
        return this.operand.toString(indent) + "\n" + actionStr;
    };
    ApplyExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.name === other.name;
    };
    ApplyExpression.prototype.changeName = function (name) {
        var value = this.valueOf();
        value.name = name;
        return new ApplyExpression(value);
    };
    ApplyExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (!operandValue)
            return null;
        var _a = this, name = _a.name, expression = _a.expression;
        return operandValue.apply(name, expression);
    };
    ApplyExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return expressionSQL + " AS " + dialect.escapeName(this.name);
    };
    ApplyExpression.prototype.isNester = function () {
        return true;
    };
    ApplyExpression.prototype.fullyDefined = function () {
        return false;
    };
    ApplyExpression.prototype.specialSimplify = function () {
        var _a = this, name = _a.name, operand = _a.operand, expression = _a.expression;
        if (expression instanceof RefExpression && expression.name === name && expression.nest === 0) {
            return operand;
        }
        if (expression.isAggregate() &&
            operand instanceof ApplyExpression &&
            !operand.expression.isAggregate() &&
            expression.getFreeReferences().indexOf(operand.name) === -1) {
            return this.swapWithOperand();
        }
        var dataset = operand.getLiteralValue();
        if (dataset instanceof Dataset && expression.resolved()) {
            var freeReferences = expression.getFreeReferences();
            var datum_1 = dataset.data[0];
            if (datum_1 && freeReferences.some(function (freeReference) { return datum_1[freeReference] instanceof Expression; })) {
                return this;
            }
            dataset = dataset.applyFn(name, function (d) {
                var simp = expression.resolve(d, 'null').simplify();
                if (simp instanceof ExternalExpression)
                    return simp.external;
                if (simp instanceof LiteralExpression)
                    return simp.value;
                return simp;
            }, expression.type);
            return r(dataset);
        }
        return this;
    };
    ApplyExpression.op = "Apply";
    return ApplyExpression;
}(ChainableUnaryExpression));
exports.ApplyExpression = ApplyExpression;
Expression.register(ApplyExpression);
var AverageExpression = (function (_super) {
    tslib_1.__extends(AverageExpression, _super);
    function AverageExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("average");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('NUMBER');
        _this.type = 'NUMBER';
        return _this;
    }
    AverageExpression.fromJS = function (parameters) {
        return new AverageExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    AverageExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.average(this.expression) : null;
    };
    AverageExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "AVG(" + dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL) + ")";
    };
    AverageExpression.prototype.decomposeAverage = function (countEx) {
        var _a = this, operand = _a.operand, expression = _a.expression;
        return operand.sum(expression).divide(countEx ? operand.sum(countEx) : operand.count());
    };
    AverageExpression.op = "Average";
    return AverageExpression;
}(ChainableUnaryExpression));
exports.AverageExpression = AverageExpression;
Expression.applyMixins(AverageExpression, [Aggregate]);
Expression.register(AverageExpression);
var CardinalityExpression = (function (_super) {
    tslib_1.__extends(CardinalityExpression, _super);
    function CardinalityExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("cardinality");
        _this._checkOperandTypes('BOOLEAN', 'STRING', 'STRING_RANGE', 'NUMBER', 'NUMBER_RANGE', 'TIME', 'TIME_RANGE');
        _this.type = 'NUMBER';
        return _this;
    }
    CardinalityExpression.fromJS = function (parameters) {
        return new CardinalityExpression(ChainableExpression.jsToValue(parameters));
    };
    CardinalityExpression.prototype._calcChainableHelper = function (operandValue) {
        if (operandValue == null)
            return null;
        return operandValue instanceof Set ? operandValue.cardinality() : 1;
    };
    CardinalityExpression.prototype._getJSChainableHelper = function (operandJS) {
        return Expression.jsNullSafetyUnary(operandJS, function (input) { return input + ".length"; });
    };
    CardinalityExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return "cardinality(" + operandSQL + ")";
    };
    CardinalityExpression.op = "Cardinality";
    return CardinalityExpression;
}(ChainableExpression));
exports.CardinalityExpression = CardinalityExpression;
Expression.register(CardinalityExpression);
var CAST_TYPE_TO_FN = {
    TIME: {
        NUMBER: function (n) { return new Date(n); }
    },
    NUMBER: {
        TIME: function (n) { return Date.parse(n.toString()); },
        _: function (s) { return Number(s); }
    },
    STRING: {
        _: function (v) { return '' + v; }
    }
};
var CAST_TYPE_TO_JS = {
    TIME: {
        NUMBER: function (operandJS) { return "new Date(" + operandJS + ")"; }
    },
    NUMBER: {
        _: function (s) { return "(+(" + s + "))"; }
    },
    STRING: {
        _: function (operandJS) { return "(''+" + operandJS + ")"; }
    }
};
var CastExpression = (function (_super) {
    tslib_1.__extends(CastExpression, _super);
    function CastExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.outputType = parameters.outputType;
        _this._ensureOp("cast");
        if (typeof _this.outputType !== 'string') {
            throw new Error("`outputType` must be a string");
        }
        _this.type = _this.outputType;
        return _this;
    }
    CastExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.outputType = parameters.outputType || parameters.castType;
        return new CastExpression(value);
    };
    CastExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.outputType = this.outputType;
        return value;
    };
    CastExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.outputType = this.outputType;
        return js;
    };
    CastExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.outputType === other.outputType;
    };
    CastExpression.prototype._toStringParameters = function (indent) {
        return [this.outputType];
    };
    CastExpression.prototype._calcChainableHelper = function (operandValue) {
        var outputType = this.outputType;
        var inputType = this.operand.type;
        if (outputType === inputType)
            return operandValue;
        var caster = CAST_TYPE_TO_FN[outputType];
        if (!caster)
            throw new Error("unsupported cast type in calc '" + outputType + "'");
        var castFn = caster[inputType] || caster['_'];
        if (!castFn)
            throw new Error("unsupported cast from " + inputType + " to '" + outputType + "'");
        return operandValue ? castFn(operandValue) : null;
    };
    CastExpression.prototype._getJSChainableHelper = function (operandJS) {
        var outputType = this.outputType;
        var inputType = this.operand.type;
        if (outputType === inputType)
            return operandJS;
        var castJS = CAST_TYPE_TO_JS[outputType];
        if (!castJS)
            throw new Error("unsupported cast type in getJS '" + outputType + "'");
        var js = castJS[inputType] || castJS['_'];
        if (!js)
            throw new Error("unsupported combo in getJS of cast action: " + inputType + " to " + outputType);
        return js(operandJS);
    };
    CastExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.castExpression(this.operand.type, operandSQL, this.outputType);
    };
    CastExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, outputType = _a.outputType;
        if (operand.type === outputType)
            return operand;
        return this;
    };
    CastExpression.op = "Cast";
    return CastExpression;
}(ChainableExpression));
exports.CastExpression = CastExpression;
Expression.register(CastExpression);
var CollectExpression = (function (_super) {
    tslib_1.__extends(CollectExpression, _super);
    function CollectExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("collect");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('BOOLEAN', 'NUMBER', 'TIME', 'STRING', 'NUMBER_RANGE', 'TIME_RANGE', 'STRING_RANGE');
        _this.type = Set.wrapSetType(_this.expression.type);
        return _this;
    }
    CollectExpression.fromJS = function (parameters) {
        return new CollectExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    CollectExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.collect(this.expression) : null;
    };
    CollectExpression.op = "Collect";
    return CollectExpression;
}(ChainableUnaryExpression));
exports.CollectExpression = CollectExpression;
Expression.applyMixins(CollectExpression, [Aggregate]);
Expression.register(CollectExpression);
var ConcatExpression = (function (_super) {
    tslib_1.__extends(ConcatExpression, _super);
    function ConcatExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("concat");
        _this._checkOperandTypes('STRING');
        _this._checkExpressionTypes('STRING');
        _this.type = Set.isSetType(_this.operand.type) ? _this.operand.type : _this.expression.type;
        return _this;
    }
    ConcatExpression.fromJS = function (parameters) {
        return new ConcatExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    ConcatExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) { return '' + a + b; });
    };
    ConcatExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return Expression.jsNullSafetyBinary(operandJS, expressionJS, (function (a, b) { return a + "+" + b; }), operandJS[0] === '"', expressionJS[0] === '"');
    };
    ConcatExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return dialect.concatExpression(operandSQL, expressionSQL);
    };
    ConcatExpression.prototype.isAssociative = function () {
        return true;
    };
    ConcatExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (operand.equals(Expression.EMPTY_STRING))
            return expression;
        if (expression.equals(Expression.EMPTY_STRING))
            return operand;
        return this;
    };
    ConcatExpression.op = "Concat";
    return ConcatExpression;
}(ChainableUnaryExpression));
exports.ConcatExpression = ConcatExpression;
Expression.register(ConcatExpression);
var ContainsExpression = (function (_super) {
    tslib_1.__extends(ContainsExpression, _super);
    function ContainsExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._checkOperandTypes('STRING');
        _this._checkExpressionTypes('STRING');
        var compare = parameters.compare;
        if (!compare) {
            compare = ContainsExpression.NORMAL;
        }
        else if (compare !== ContainsExpression.NORMAL && compare !== ContainsExpression.IGNORE_CASE) {
            throw new Error("compare must be '" + ContainsExpression.NORMAL + "' or '" + ContainsExpression.IGNORE_CASE + "'");
        }
        _this.compare = compare;
        _this._ensureOp("contains");
        _this.type = 'BOOLEAN';
        return _this;
    }
    ContainsExpression.caseIndependent = function (str) {
        return str.toUpperCase() === str.toLowerCase();
    };
    ContainsExpression.fromJS = function (parameters) {
        var value = ChainableUnaryExpression.jsToValue(parameters);
        value.compare = parameters.compare;
        return new ContainsExpression(value);
    };
    ContainsExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.compare = this.compare;
        return value;
    };
    ContainsExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.compare = this.compare;
        return js;
    };
    ContainsExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.compare === other.compare;
    };
    ContainsExpression.prototype._toStringParameters = function (indent) {
        return [this.expression.toString(indent), this.compare];
    };
    ContainsExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        var fn;
        if (this.compare === ContainsExpression.NORMAL) {
            fn = function (a, b) { return String(a).indexOf(b) > -1; };
        }
        else {
            fn = function (a, b) { return String(a).toLowerCase().indexOf(String(b).toLowerCase()) > -1; };
        }
        return Set.crossBinaryBoolean(operandValue, expressionValue, fn);
    };
    ContainsExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        var combine;
        if (this.compare === ContainsExpression.NORMAL) {
            combine = function (lhs, rhs) { return "(''+" + lhs + ").indexOf(" + rhs + ")>-1"; };
        }
        else {
            combine = function (lhs, rhs) { return "(''+" + lhs + ").toLowerCase().indexOf((''+" + rhs + ").toLowerCase())>-1"; };
        }
        return Expression.jsNullSafetyBinary(operandJS, expressionJS, combine, operandJS[0] === '"', expressionJS[0] === '"');
    };
    ContainsExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        if (this.compare === ContainsExpression.IGNORE_CASE) {
            expressionSQL = "LOWER(" + expressionSQL + ")";
            operandSQL = "LOWER(" + operandSQL + ")";
        }
        return dialect.containsExpression(expressionSQL, operandSQL);
    };
    ContainsExpression.prototype.changeCompare = function (compare) {
        var value = this.valueOf();
        value.compare = compare;
        return new ContainsExpression(value);
    };
    ContainsExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression, compare = _a.compare;
        if (operand instanceof TransformCaseExpression && expression instanceof TransformCaseExpression) {
            var x = operand.operand, tt1 = operand.transformType;
            var y = expression.operand, tt2 = expression.transformType;
            if (tt1 === tt2) {
                return x.contains(y, ContainsExpression.IGNORE_CASE);
            }
        }
        if (compare === 'ignoreCase') {
            var expressionLiteral = expression.getLiteralValue();
            if (expressionLiteral != null &&
                ((typeof expressionLiteral === 'string' && ContainsExpression.caseIndependent(expressionLiteral)) ||
                    (expressionLiteral instanceof Set && expressionLiteral.elements.every(ContainsExpression.caseIndependent)))) {
                return this.changeCompare('normal');
            }
        }
        return this;
    };
    ContainsExpression.NORMAL = 'normal';
    ContainsExpression.IGNORE_CASE = 'ignoreCase';
    ContainsExpression.op = "Contains";
    return ContainsExpression;
}(ChainableUnaryExpression));
exports.ContainsExpression = ContainsExpression;
Expression.register(ContainsExpression);
var CountExpression = (function (_super) {
    tslib_1.__extends(CountExpression, _super);
    function CountExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("count");
        _this._checkOperandTypes('DATASET');
        _this.type = 'NUMBER';
        return _this;
    }
    CountExpression.fromJS = function (parameters) {
        return new CountExpression(ChainableExpression.jsToValue(parameters));
    };
    CountExpression.prototype.calc = function (datum) {
        var inV = this.operand.calc(datum);
        return inV ? inV.count() : 0;
    };
    CountExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return operandSQL.indexOf(' WHERE ') === -1 ? "COUNT(*)" : "SUM(" + dialect.aggregateFilterIfNeeded(operandSQL, '1', '0') + ")";
    };
    CountExpression.op = "Count";
    return CountExpression;
}(ChainableExpression));
exports.CountExpression = CountExpression;
Expression.applyMixins(CountExpression, [Aggregate]);
Expression.register(CountExpression);
var CountDistinctExpression = (function (_super) {
    tslib_1.__extends(CountDistinctExpression, _super);
    function CountDistinctExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("countDistinct");
        _this._checkOperandTypes('DATASET');
        _this.type = 'NUMBER';
        return _this;
    }
    CountDistinctExpression.fromJS = function (parameters) {
        return new CountDistinctExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    CountDistinctExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.countDistinct(this.expression) : null;
    };
    CountDistinctExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "COUNT(DISTINCT " + dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL) + ")";
    };
    CountDistinctExpression.op = "CountDistinct";
    return CountDistinctExpression;
}(ChainableUnaryExpression));
exports.CountDistinctExpression = CountDistinctExpression;
Expression.applyMixins(CountDistinctExpression, [Aggregate]);
Expression.register(CountDistinctExpression);
var CustomAggregateExpression = (function (_super) {
    tslib_1.__extends(CustomAggregateExpression, _super);
    function CustomAggregateExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.custom = parameters.custom;
        _this._ensureOp("customAggregate");
        _this._checkOperandTypes('DATASET');
        _this.type = 'NUMBER';
        return _this;
    }
    CustomAggregateExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.custom = parameters.custom;
        return new CustomAggregateExpression(value);
    };
    CustomAggregateExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.custom = this.custom;
        return value;
    };
    CustomAggregateExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.custom = this.custom;
        return js;
    };
    CustomAggregateExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.custom === other.custom;
    };
    CustomAggregateExpression.prototype._toStringParameters = function (indent) {
        return [this.custom];
    };
    CustomAggregateExpression.prototype._calcChainableHelper = function (operandValue) {
        throw new Error('can not compute on custom action');
    };
    CustomAggregateExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        throw new Error('custom action not implemented');
    };
    CustomAggregateExpression.op = "CustomAggregate";
    return CustomAggregateExpression;
}(ChainableExpression));
exports.CustomAggregateExpression = CustomAggregateExpression;
Expression.applyMixins(CustomAggregateExpression, [Aggregate]);
Expression.register(CustomAggregateExpression);
var CustomTransformExpression = (function (_super) {
    tslib_1.__extends(CustomTransformExpression, _super);
    function CustomTransformExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("customTransform");
        _this.custom = parameters.custom;
        if (parameters.outputType)
            _this.outputType = parameters.outputType;
        _this.type = _this.outputType || _this.operand.type;
        return _this;
    }
    CustomTransformExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.custom = parameters.custom;
        if (parameters.outputType)
            value.outputType = parameters.outputType;
        return new CustomTransformExpression(value);
    };
    CustomTransformExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.custom = this.custom;
        if (this.outputType)
            value.outputType = this.outputType;
        return value;
    };
    CustomTransformExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.custom = this.custom;
        if (this.outputType)
            js.outputType = this.outputType;
        return js;
    };
    CustomTransformExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.custom === other.custom &&
            this.outputType === other.outputType;
    };
    CustomTransformExpression.prototype._toStringParameters = function (indent) {
        var param = [this.custom];
        if (this.outputType)
            param.push(this.outputType);
        return param;
    };
    CustomTransformExpression.prototype._calcChainableHelper = function (operandValue) {
        throw new Error('can not calc on custom transform action');
    };
    CustomTransformExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        throw new Error("Custom transform not supported in SQL");
    };
    CustomTransformExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error("Custom transform can't yet be expressed as JS");
    };
    CustomTransformExpression.op = "CustomTransform";
    return CustomTransformExpression;
}(ChainableExpression));
exports.CustomTransformExpression = CustomTransformExpression;
Expression.register(CustomTransformExpression);
var DivideExpression = (function (_super) {
    tslib_1.__extends(DivideExpression, _super);
    function DivideExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("divide");
        _this._checkOperandTypes('NUMBER');
        _this._checkExpressionTypes('NUMBER');
        _this.type = 'NUMBER';
        return _this;
    }
    DivideExpression.fromJS = function (parameters) {
        return new DivideExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    DivideExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) { return b !== 0 ? a / b : null; });
    };
    DivideExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(_=" + expressionJS + ",(_===0||isNaN(_)?null:" + operandJS + "/" + expressionJS + "))";
    };
    DivideExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + "/" + expressionSQL + ")";
    };
    DivideExpression.prototype.specialSimplify = function () {
        if (this.expression.equals(Expression.ZERO))
            return Expression.NULL;
        if (this.expression.equals(Expression.ONE))
            return this.operand;
        return this;
    };
    DivideExpression.op = "Divide";
    return DivideExpression;
}(ChainableUnaryExpression));
exports.DivideExpression = DivideExpression;
Expression.register(DivideExpression);
var ExtractExpression = (function (_super) {
    tslib_1.__extends(ExtractExpression, _super);
    function ExtractExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.regexp = parameters.regexp;
        _this._ensureOp("extract");
        _this._checkOperandTypes('STRING');
        _this.type = _this.operand.type;
        return _this;
    }
    ExtractExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.regexp = parameters.regexp;
        return new ExtractExpression(value);
    };
    ExtractExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.regexp = this.regexp;
        return value;
    };
    ExtractExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.regexp = this.regexp;
        return js;
    };
    ExtractExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.regexp === other.regexp;
    };
    ExtractExpression.prototype._toStringParameters = function (indent) {
        return [this.regexp];
    };
    ExtractExpression.prototype._calcChainableHelper = function (operandValue) {
        if (!operandValue)
            return null;
        var re = new RegExp(this.regexp);
        return Set.crossUnary(operandValue, function (a) { return (String(a).match(re) || [])[1] || null; });
    };
    ExtractExpression.prototype._getJSChainableHelper = function (operandJS) {
        return "((''+" + operandJS + ").match(/" + this.regexp + "/) || [])[1] || null";
    };
    ExtractExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.extractExpression(operandSQL, this.regexp);
    };
    ExtractExpression.op = "Extract";
    return ExtractExpression;
}(ChainableExpression));
exports.ExtractExpression = ExtractExpression;
Expression.register(ExtractExpression);
var FallbackExpression = (function (_super) {
    tslib_1.__extends(FallbackExpression, _super);
    function FallbackExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("fallback");
        _this._checkOperandExpressionTypesAlign();
        _this.type = _this.operand.type || _this.expression.type;
        return _this;
    }
    FallbackExpression.fromJS = function (parameters) {
        return new FallbackExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    FallbackExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue !== null ? operandValue : expressionValue;
    };
    FallbackExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "((_=" + operandJS + "),(_!==null?_:" + expressionJS + "))";
    };
    FallbackExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return dialect.coalesceExpression(operandSQL, expressionSQL);
    };
    FallbackExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.NULL))
            return operand;
        if (operand.equals(Expression.NULL))
            return expression;
        if (operand.equals(expression))
            return operand;
        if (operand.getLiteralValue() != null)
            return operand;
        return this;
    };
    FallbackExpression.op = "Fallback";
    return FallbackExpression;
}(ChainableUnaryExpression));
exports.FallbackExpression = FallbackExpression;
Expression.register(FallbackExpression);
var FilterExpression = (function (_super) {
    tslib_1.__extends(FilterExpression, _super);
    function FilterExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("filter");
        _this._checkExpressionTypes('BOOLEAN');
        _this.type = 'DATASET';
        return _this;
    }
    FilterExpression.fromJS = function (parameters) {
        var value = ChainableUnaryExpression.jsToValue(parameters);
        return new FilterExpression(value);
    };
    FilterExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.filter(this.expression) : null;
    };
    FilterExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return operandSQL + " WHERE " + expressionSQL;
    };
    FilterExpression.prototype.isNester = function () {
        return true;
    };
    FilterExpression.prototype.fullyDefined = function () {
        return this.operand.isOp('literal') && this.expression.resolved();
    };
    FilterExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.TRUE))
            return operand;
        if (operand instanceof FilterExpression) {
            var x = operand.operand, a = operand.expression;
            return x.filter(a.and(expression));
        }
        if (operand instanceof ApplyExpression) {
            return expression.getFreeReferences().indexOf(operand.name) === -1 ? this.swapWithOperand() : this;
        }
        if (operand instanceof SplitExpression && operand.isLinear()) {
            var x = operand.operand, splits_1 = operand.splits, dataName = operand.dataName;
            var newFilter = expression.substitute(function (ex) {
                if (ex instanceof RefExpression && splits_1[ex.name])
                    return splits_1[ex.name];
                return null;
            });
            return x.filter(newFilter).split(splits_1, dataName);
        }
        if (operand instanceof SortExpression)
            return this.swapWithOperand();
        return this;
    };
    FilterExpression.op = "Filter";
    return FilterExpression;
}(ChainableUnaryExpression));
exports.FilterExpression = FilterExpression;
Expression.register(FilterExpression);
var GreaterThanExpression = (function (_super) {
    tslib_1.__extends(GreaterThanExpression, _super);
    function GreaterThanExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("greaterThan");
        _this._checkOperandTypes('NUMBER', 'TIME', 'STRING');
        _this._checkExpressionTypes('NUMBER', 'TIME', 'STRING');
        _this._bumpOperandExpressionToTime();
        _this._checkOperandExpressionTypesAlign();
        _this.type = 'BOOLEAN';
        return _this;
    }
    GreaterThanExpression.fromJS = function (parameters) {
        return new GreaterThanExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    GreaterThanExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinaryBoolean(operandValue, expressionValue, function (a, b) { return a > b; });
    };
    GreaterThanExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + ">" + expressionJS + ")";
    };
    GreaterThanExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + ">" + expressionSQL + ")";
    };
    GreaterThanExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression instanceof LiteralExpression) {
            return operand.overlap(r(Range.fromJS({ start: expression.value, end: null, bounds: '()' })));
        }
        if (operand instanceof LiteralExpression) {
            return expression.overlap(r(Range.fromJS({ start: null, end: operand.value, bounds: '()' })));
        }
        return this;
    };
    GreaterThanExpression.op = "GreaterThan";
    return GreaterThanExpression;
}(ChainableUnaryExpression));
exports.GreaterThanExpression = GreaterThanExpression;
Expression.register(GreaterThanExpression);
var GreaterThanOrEqualExpression = (function (_super) {
    tslib_1.__extends(GreaterThanOrEqualExpression, _super);
    function GreaterThanOrEqualExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("greaterThanOrEqual");
        _this._checkOperandTypes('NUMBER', 'TIME', 'STRING');
        _this._checkExpressionTypes('NUMBER', 'TIME', 'STRING');
        _this._bumpOperandExpressionToTime();
        _this._checkOperandExpressionTypesAlign();
        _this.type = 'BOOLEAN';
        return _this;
    }
    GreaterThanOrEqualExpression.fromJS = function (parameters) {
        return new GreaterThanOrEqualExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    GreaterThanOrEqualExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinaryBoolean(operandValue, expressionValue, function (a, b) { return a >= b; });
    };
    GreaterThanOrEqualExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + ">=" + expressionJS + ")";
    };
    GreaterThanOrEqualExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + ">=" + expressionSQL + ")";
    };
    GreaterThanOrEqualExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression instanceof LiteralExpression) {
            return operand.overlap(r(Range.fromJS({ start: expression.value, end: null, bounds: '[)' })));
        }
        if (operand instanceof LiteralExpression) {
            return expression.overlap(r(Range.fromJS({ start: null, end: operand.value, bounds: '(]' })));
        }
        return this;
    };
    GreaterThanOrEqualExpression.op = "GreaterThanOrEqual";
    return GreaterThanOrEqualExpression;
}(ChainableUnaryExpression));
exports.GreaterThanOrEqualExpression = GreaterThanOrEqualExpression;
Expression.register(GreaterThanOrEqualExpression);
var InExpression = (function (_super) {
    tslib_1.__extends(InExpression, _super);
    function InExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("in");
        var operandType = _this.operand.type;
        var expression = _this.expression;
        if (operandType) {
            if (!(operandType === 'NULL' ||
                expression.type === 'NULL' ||
                (!Set.isSetType(operandType) && expression.canHaveType('SET')))) {
                throw new TypeError("in expression " + _this + " has a bad type combination " + operandType + " IN " + (expression.type || '*'));
            }
        }
        else {
            if (!(expression.canHaveType('NUMBER_RANGE') || expression.canHaveType('STRING_RANGE') || expression.canHaveType('TIME_RANGE') || expression.canHaveType('SET'))) {
                throw new TypeError("in expression has invalid expression type " + expression.type);
            }
        }
        _this.type = 'BOOLEAN';
        return _this;
    }
    InExpression.fromJS = function (parameters) {
        var value = ChainableUnaryExpression.jsToValue(parameters);
        if (Range.isRangeType(value.expression.type)) {
            console.warn('InExpression should no longer be used for ranges use OverlapExpression instead');
            value.op = 'overlap';
            return new OverlapExpression(value);
        }
        return new InExpression(value);
    };
    InExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (!expressionValue)
            return null;
        return expressionValue.contains(operandValue);
    };
    InExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        throw new Error("can not convert " + this + " to JS function");
    };
    InExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        throw new Error("can not convert action to SQL " + this);
    };
    InExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (operand.type && !Set.isSetType(operand.type))
            return operand.is(expression);
        return this;
    };
    InExpression.op = "In";
    return InExpression;
}(ChainableUnaryExpression));
exports.InExpression = InExpression;
Expression.register(InExpression);
var IsExpression = (function (_super) {
    tslib_1.__extends(IsExpression, _super);
    function IsExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("is");
        _this._checkOperandExpressionTypesAlign();
        _this.type = 'BOOLEAN';
        return _this;
    }
    IsExpression.fromJS = function (parameters) {
        return new IsExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    IsExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return Set.crossBinaryBoolean(operandValue, expressionValue, function (a, b) { return a === b || Boolean(a && a.equals && a.equals(b)); });
    };
    IsExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        var expression = this.expression;
        if (expression instanceof LiteralExpression) {
            if (Set.isSetType(expression.type)) {
                var valueSet = expression.value;
                return JSON.stringify(valueSet.elements) + ".indexOf(" + operandJS + ")>-1";
            }
        }
        return "(" + operandJS + "===" + expressionJS + ")";
    };
    IsExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        var expressionSet = this.expression.getLiteralValue();
        if (expressionSet instanceof Set) {
            switch (this.expression.type) {
                case 'SET/STRING':
                case 'SET/NUMBER':
                    var nullCheck = null;
                    if (expressionSet.has(null)) {
                        nullCheck = "(" + operandSQL + " IS NULL)";
                        expressionSet = expressionSet.remove(null);
                    }
                    var inCheck = operandSQL + " IN (" + expressionSet.elements.map(function (v) { return typeof v === 'number' ? v : dialect.escapeLiteral(v); }).join(',') + ")";
                    return nullCheck ? "(" + nullCheck + " OR " + inCheck + ")" : inCheck;
                default:
                    return expressionSet.elements.map(function (e) { return dialect.isNotDistinctFromExpression(operandSQL, r(e).getSQL(dialect)); }).join(' OR ');
            }
        }
        else {
            return dialect.isNotDistinctFromExpression(operandSQL, expressionSQL);
        }
    };
    IsExpression.prototype.isCommutative = function () {
        return true;
    };
    IsExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (operand.equals(expression))
            return Expression.TRUE;
        var literalValue = expression.getLiteralValue();
        if (literalValue != null) {
            if (Set.isSet(literalValue) && literalValue.elements.length === 1) {
                return operand.is(r(literalValue.elements[0]));
            }
            if (operand instanceof IndexOfExpression && literalValue === -1) {
                var x = operand.operand, y = operand.expression;
                return x.contains(y).not();
            }
            if (operand instanceof TimeBucketExpression && literalValue instanceof TimeRange && operand.timezone) {
                var x = operand.operand, duration = operand.duration, timezone = operand.timezone;
                if (literalValue.start !== null && TimeRange.timeBucket(literalValue.start, duration, timezone).equals(literalValue)) {
                    return x.overlap(expression);
                }
                else {
                    return Expression.FALSE;
                }
            }
            if (operand instanceof NumberBucketExpression && literalValue instanceof NumberRange) {
                var x = operand.operand, size = operand.size, offset = operand.offset;
                if (literalValue.start !== null && NumberRange.numberBucket(literalValue.start, size, offset).equals(literalValue)) {
                    return x.overlap(expression);
                }
                else {
                    return Expression.FALSE;
                }
            }
            if (operand instanceof ThenExpression) {
                var x = operand.operand, y = operand.expression;
                if (y.isOp('literal')) {
                    return y.equals(expression) ? x.is(Expression.TRUE) : x.isnt(Expression.TRUE);
                }
            }
        }
        return this;
    };
    IsExpression.op = "Is";
    return IsExpression;
}(ChainableUnaryExpression));
exports.IsExpression = IsExpression;
Expression.register(IsExpression);
var JoinExpression = (function (_super) {
    tslib_1.__extends(JoinExpression, _super);
    function JoinExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("join");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('DATASET');
        _this.type = 'DATASET';
        return _this;
    }
    JoinExpression.fromJS = function (parameters) {
        return new JoinExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    JoinExpression.prototype.updateTypeContext = function (typeContext, expressionTypeContext) {
        var myDatasetType = typeContext.datasetType;
        var expressionDatasetType = expressionTypeContext.datasetType;
        for (var k in expressionDatasetType) {
            typeContext.datasetType[k] = expressionDatasetType[k];
            var ft = expressionDatasetType[k];
            if (hasOwnProp(myDatasetType, k)) {
                if (myDatasetType[k].type !== ft.type) {
                    throw new Error("incompatible types of joins on " + k + " between " + myDatasetType[k].type + " and " + ft.type);
                }
            }
            else {
                myDatasetType[k] = ft;
            }
        }
        return typeContext;
    };
    JoinExpression.prototype.pushIntoExternal = function () {
        return null;
    };
    JoinExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.join(expressionValue) : null;
    };
    JoinExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        throw new Error('not possible');
    };
    JoinExpression.op = "Join";
    return JoinExpression;
}(ChainableUnaryExpression));
exports.JoinExpression = JoinExpression;
Expression.register(JoinExpression);
var LengthExpression = (function (_super) {
    tslib_1.__extends(LengthExpression, _super);
    function LengthExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("length");
        _this._checkOperandTypes('STRING');
        _this.type = 'NUMBER';
        return _this;
    }
    LengthExpression.fromJS = function (parameters) {
        return new LengthExpression(ChainableExpression.jsToValue(parameters));
    };
    LengthExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue ? operandValue.length : null;
    };
    LengthExpression.prototype._getJSChainableHelper = function (operandJS) {
        return Expression.jsNullSafetyUnary(operandJS, function (input) { return input + ".length"; });
    };
    LengthExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.lengthExpression(operandSQL);
    };
    LengthExpression.op = "Length";
    return LengthExpression;
}(ChainableExpression));
exports.LengthExpression = LengthExpression;
Expression.register(LengthExpression);
var LessThanExpression = (function (_super) {
    tslib_1.__extends(LessThanExpression, _super);
    function LessThanExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("lessThan");
        _this._checkOperandTypes('NUMBER', 'TIME', 'STRING');
        _this._checkExpressionTypes('NUMBER', 'TIME', 'STRING');
        _this._bumpOperandExpressionToTime();
        _this._checkOperandExpressionTypesAlign();
        _this.type = 'BOOLEAN';
        return _this;
    }
    LessThanExpression.fromJS = function (parameters) {
        return new LessThanExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    LessThanExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinaryBoolean(operandValue, expressionValue, function (a, b) { return a < b; });
    };
    LessThanExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "<" + expressionJS + ")";
    };
    LessThanExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + "<" + expressionSQL + ")";
    };
    LessThanExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression instanceof LiteralExpression) {
            return operand.overlap(r(Range.fromJS({ start: null, end: expression.value, bounds: '()' })));
        }
        if (operand instanceof LiteralExpression) {
            return expression.overlap(r(Range.fromJS({ start: operand.value, end: null, bounds: '()' })));
        }
        return this;
    };
    LessThanExpression.op = "LessThan";
    return LessThanExpression;
}(ChainableUnaryExpression));
exports.LessThanExpression = LessThanExpression;
Expression.register(LessThanExpression);
var LessThanOrEqualExpression = (function (_super) {
    tslib_1.__extends(LessThanOrEqualExpression, _super);
    function LessThanOrEqualExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("lessThanOrEqual");
        _this._checkOperandTypes('NUMBER', 'TIME', 'STRING');
        _this._checkExpressionTypes('NUMBER', 'TIME', 'STRING');
        _this._bumpOperandExpressionToTime();
        _this._checkOperandExpressionTypesAlign();
        _this.type = 'BOOLEAN';
        return _this;
    }
    LessThanOrEqualExpression.fromJS = function (parameters) {
        return new LessThanOrEqualExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    LessThanOrEqualExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinaryBoolean(operandValue, expressionValue, function (a, b) { return a <= b; });
    };
    LessThanOrEqualExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "<=" + expressionJS + ")";
    };
    LessThanOrEqualExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + "<=" + expressionSQL + ")";
    };
    LessThanOrEqualExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression instanceof LiteralExpression) {
            return operand.overlap(r(Range.fromJS({ start: null, end: expression.value, bounds: '(]' })));
        }
        if (operand instanceof LiteralExpression) {
            return expression.overlap(r(Range.fromJS({ start: operand.value, end: null, bounds: '[)' })));
        }
        return this;
    };
    LessThanOrEqualExpression.op = "LessThanOrEqual";
    return LessThanOrEqualExpression;
}(ChainableUnaryExpression));
exports.LessThanOrEqualExpression = LessThanOrEqualExpression;
Expression.register(LessThanOrEqualExpression);
var IndexOfExpression = (function (_super) {
    tslib_1.__extends(IndexOfExpression, _super);
    function IndexOfExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("indexOf");
        _this._checkOperandTypes('STRING');
        _this._checkExpressionTypes('STRING');
        _this.type = 'NUMBER';
        return _this;
    }
    IndexOfExpression.fromJS = function (parameters) {
        return new IndexOfExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    IndexOfExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.indexOf(expressionValue) : null;
    };
    IndexOfExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return Expression.jsNullSafetyBinary(operandJS, expressionJS, (function (a, b) { return a + ".indexOf(" + b + ")"; }), operandJS[0] === '"', expressionJS[0] === '"');
    };
    IndexOfExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return dialect.indexOfExpression(operandSQL, expressionSQL);
    };
    IndexOfExpression.op = "IndexOf";
    return IndexOfExpression;
}(ChainableUnaryExpression));
exports.IndexOfExpression = IndexOfExpression;
Expression.register(IndexOfExpression);
var LogExpression = (function (_super) {
    tslib_1.__extends(LogExpression, _super);
    function LogExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("log");
        _this._checkOperandTypes('NUMBER');
        _this._checkExpressionTypes('NUMBER');
        _this.type = Set.isSetType(_this.operand.type) ? _this.operand.type : _this.expression.type;
        return _this;
    }
    LogExpression.fromJS = function (parameters) {
        return new LogExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    LogExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue == null || expressionValue == null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) {
            var log = Math.log(a) / Math.log(b);
            return isNaN(log) ? null : log;
        });
    };
    LogExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(Math.log(" + operandJS + ")/Math.log(" + expressionJS + "))";
    };
    LogExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        var myLiteral = this.expression.getLiteralValue();
        if (myLiteral === Math.E)
            return "LN(" + operandSQL + ")";
        return "LOG(" + expressionSQL + "," + operandSQL + ")";
    };
    LogExpression.prototype.specialSimplify = function () {
        var operand = this.operand;
        if (operand.equals(Expression.ONE))
            return Expression.ZERO;
        return this;
    };
    LogExpression.op = "Log";
    return LogExpression;
}(ChainableUnaryExpression));
exports.LogExpression = LogExpression;
Expression.register(LogExpression);
var LookupExpression = (function (_super) {
    tslib_1.__extends(LookupExpression, _super);
    function LookupExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("lookup");
        _this._checkOperandTypes('STRING');
        _this.lookupFn = parameters.lookupFn;
        _this.type = _this.operand.type;
        return _this;
    }
    LookupExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.lookupFn = parameters.lookupFn || parameters.lookup;
        return new LookupExpression(value);
    };
    LookupExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.lookupFn = this.lookupFn;
        return value;
    };
    LookupExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.lookupFn = this.lookupFn;
        return js;
    };
    LookupExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.lookupFn === other.lookupFn;
    };
    LookupExpression.prototype._toStringParameters = function (indent) {
        return [Expression.safeString(this.lookupFn)];
    };
    LookupExpression.prototype.fullyDefined = function () {
        return false;
    };
    LookupExpression.prototype._calcChainableHelper = function (operandValue) {
        throw new Error('can not express as JS');
    };
    LookupExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error('can not express as JS');
    };
    LookupExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        throw new Error('can not express as SQL');
    };
    LookupExpression.op = "Lookup";
    return LookupExpression;
}(ChainableExpression));
exports.LookupExpression = LookupExpression;
Expression.register(LookupExpression);
var LimitExpression = (function (_super) {
    tslib_1.__extends(LimitExpression, _super);
    function LimitExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("limit");
        _this._checkOperandTypes('DATASET');
        var value = parameters.value;
        if (value == null)
            value = Infinity;
        if (value < 0)
            throw new Error("limit value can not be negative (is " + value + ")");
        _this.value = value;
        _this.type = 'DATASET';
        return _this;
    }
    LimitExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.value = parameters.value || parameters.limit;
        return new LimitExpression(value);
    };
    LimitExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.value = this.value;
        return value;
    };
    LimitExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.value = this.value;
        return js;
    };
    LimitExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.value === other.value;
    };
    LimitExpression.prototype._toStringParameters = function (indent) {
        return [String(this.value)];
    };
    LimitExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue ? operandValue.limit(this.value) : null;
    };
    LimitExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return "LIMIT " + this.value;
    };
    LimitExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, value = _a.value;
        if (!isFinite(value))
            return operand;
        if (operand instanceof LimitExpression) {
            var x = operand.operand, a = operand.value;
            return x.limit(Math.min(a, value));
        }
        if (operand instanceof ApplyExpression) {
            return this.swapWithOperand();
        }
        return this;
    };
    LimitExpression.op = "Limit";
    return LimitExpression;
}(ChainableExpression));
exports.LimitExpression = LimitExpression;
Expression.register(LimitExpression);
var REGEXP_SPECIAL = "\\^$.|?*+()[{";
var MatchExpression = (function (_super) {
    tslib_1.__extends(MatchExpression, _super);
    function MatchExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("match");
        _this._checkOperandTypes('STRING');
        _this.regexp = parameters.regexp;
        _this.type = 'BOOLEAN';
        return _this;
    }
    MatchExpression.likeToRegExp = function (like, escapeChar) {
        if (escapeChar === void 0) { escapeChar = '\\'; }
        var regExp = ['^'];
        for (var i = 0; i < like.length; i++) {
            var char = like[i];
            if (char === escapeChar) {
                var nextChar = like[i + 1];
                if (!nextChar)
                    throw new Error("invalid LIKE string '" + like + "'");
                char = nextChar;
                i++;
            }
            else if (char === '%') {
                regExp.push('.*');
                continue;
            }
            else if (char === '_') {
                regExp.push('.');
                continue;
            }
            if (REGEXP_SPECIAL.indexOf(char) !== -1) {
                regExp.push('\\');
            }
            regExp.push(char);
        }
        regExp.push('$');
        return regExp.join('');
    };
    MatchExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.regexp = parameters.regexp;
        return new MatchExpression(value);
    };
    MatchExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.regexp = this.regexp;
        return value;
    };
    MatchExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.regexp = this.regexp;
        return js;
    };
    MatchExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.regexp === other.regexp;
    };
    MatchExpression.prototype._toStringParameters = function (indent) {
        return [this.regexp];
    };
    MatchExpression.prototype._calcChainableHelper = function (operandValue) {
        var re = new RegExp(this.regexp);
        if (operandValue == null)
            return null;
        return Set.crossUnaryBoolean(operandValue, function (a) { return re.test(a); });
    };
    MatchExpression.prototype._getJSChainableHelper = function (operandJS) {
        return "/" + this.regexp + "/.test(" + operandJS + ")";
    };
    MatchExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.regexpExpression(operandSQL, this.regexp);
    };
    MatchExpression.op = "Match";
    return MatchExpression;
}(ChainableExpression));
exports.MatchExpression = MatchExpression;
Expression.register(MatchExpression);
var MaxExpression = (function (_super) {
    tslib_1.__extends(MaxExpression, _super);
    function MaxExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("max");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('NUMBER', 'TIME');
        _this.type = Set.unwrapSetType(_this.expression.type);
        return _this;
    }
    MaxExpression.fromJS = function (parameters) {
        return new MaxExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    MaxExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.max(this.expression) : null;
    };
    MaxExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "MAX(" + dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL) + ")";
    };
    MaxExpression.op = "Max";
    return MaxExpression;
}(ChainableUnaryExpression));
exports.MaxExpression = MaxExpression;
Expression.applyMixins(MaxExpression, [Aggregate]);
Expression.register(MaxExpression);
var MinExpression = (function (_super) {
    tslib_1.__extends(MinExpression, _super);
    function MinExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("min");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('NUMBER', 'TIME');
        _this.type = Set.unwrapSetType(_this.expression.type);
        return _this;
    }
    MinExpression.fromJS = function (parameters) {
        return new MinExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    MinExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.min(this.expression) : null;
    };
    MinExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "MIN(" + dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL) + ")";
    };
    MinExpression.op = "Min";
    return MinExpression;
}(ChainableUnaryExpression));
exports.MinExpression = MinExpression;
Expression.applyMixins(MinExpression, [Aggregate]);
Expression.register(MinExpression);
var MultiplyExpression = (function (_super) {
    tslib_1.__extends(MultiplyExpression, _super);
    function MultiplyExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("multiply");
        _this._checkOperandTypes('NUMBER');
        _this._checkExpressionTypes('NUMBER');
        _this.type = 'NUMBER';
        return _this;
    }
    MultiplyExpression.fromJS = function (parameters) {
        return new MultiplyExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    MultiplyExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) { return a * b; });
    };
    MultiplyExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "*" + expressionJS + ")";
    };
    MultiplyExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + "*" + expressionSQL + ")";
    };
    MultiplyExpression.prototype.isCommutative = function () {
        return true;
    };
    MultiplyExpression.prototype.isAssociative = function () {
        return true;
    };
    MultiplyExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.ZERO))
            return Expression.ZERO;
        if (expression.equals(Expression.ONE))
            return operand;
        return this;
    };
    MultiplyExpression.op = "Multiply";
    return MultiplyExpression;
}(ChainableUnaryExpression));
exports.MultiplyExpression = MultiplyExpression;
Expression.register(MultiplyExpression);
var NotExpression = (function (_super) {
    tslib_1.__extends(NotExpression, _super);
    function NotExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("not");
        _this._checkOperandTypes('BOOLEAN');
        _this.type = 'BOOLEAN';
        return _this;
    }
    NotExpression.fromJS = function (parameters) {
        return new NotExpression(ChainableExpression.jsToValue(parameters));
    };
    NotExpression.prototype._calcChainableHelper = function (operandValue) {
        return !operandValue;
    };
    NotExpression.prototype._getJSChainableHelper = function (operandJS) {
        return "!(" + operandJS + ")";
    };
    NotExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return "NOT(" + operandSQL + ")";
    };
    NotExpression.prototype.specialSimplify = function () {
        var operand = this.operand;
        if (operand instanceof NotExpression)
            return operand.operand;
        return this;
    };
    NotExpression.op = "Not";
    return NotExpression;
}(ChainableExpression));
exports.NotExpression = NotExpression;
Expression.register(NotExpression);
var NumberBucketExpression = (function (_super) {
    tslib_1.__extends(NumberBucketExpression, _super);
    function NumberBucketExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.size = parameters.size;
        _this.offset = parameters.offset;
        _this._ensureOp("numberBucket");
        _this._checkOperandTypes('NUMBER');
        _this.type = 'NUMBER_RANGE';
        return _this;
    }
    NumberBucketExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.size = parameters.size;
        value.offset = hasOwnProp(parameters, 'offset') ? parameters.offset : 0;
        return new NumberBucketExpression(value);
    };
    NumberBucketExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.size = this.size;
        value.offset = this.offset;
        return value;
    };
    NumberBucketExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.size = this.size;
        if (this.offset)
            js.offset = this.offset;
        return js;
    };
    NumberBucketExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.size === other.size &&
            this.offset === other.offset;
    };
    NumberBucketExpression.prototype._toStringParameters = function (indent) {
        var params = [String(this.size)];
        if (this.offset)
            params.push(String(this.offset));
        return params;
    };
    NumberBucketExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue !== null ? NumberRange.numberBucket(operandValue, this.size, this.offset) : null;
    };
    NumberBucketExpression.prototype._getJSChainableHelper = function (operandJS) {
        var _this = this;
        return Expression.jsNullSafetyUnary(operandJS, function (n) { return continuousFloorExpression(n, "Math.floor", _this.size, _this.offset); });
    };
    NumberBucketExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return continuousFloorExpression(operandSQL, "FLOOR", this.size, this.offset);
    };
    NumberBucketExpression.op = "NumberBucket";
    return NumberBucketExpression;
}(ChainableExpression));
exports.NumberBucketExpression = NumberBucketExpression;
Expression.register(NumberBucketExpression);
var IS_OR_OVERLAP = {
    'is': true,
    'overlap': true
};
var OrExpression = (function (_super) {
    tslib_1.__extends(OrExpression, _super);
    function OrExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("or");
        _this._checkOperandTypes('BOOLEAN');
        _this._checkExpressionTypes('BOOLEAN');
        _this.type = 'BOOLEAN';
        return _this;
    }
    OrExpression.fromJS = function (parameters) {
        return new OrExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    OrExpression.merge = function (ex1, ex2) {
        if (ex1.equals(ex2))
            return ex1;
        if (!IS_OR_OVERLAP[ex1.op] || !IS_OR_OVERLAP[ex2.op])
            return null;
        var _a = ex1, lhs1 = _a.operand, rhs1 = _a.expression;
        var _b = ex2, lhs2 = _b.operand, rhs2 = _b.expression;
        if (!lhs1.equals(lhs2) || !rhs1.isOp('literal') || !rhs2.isOp('literal'))
            return null;
        var union = Set.unionCover(rhs1.getLiteralValue(), rhs2.getLiteralValue());
        if (union === null)
            return null;
        return lhs1.overlap(r(union)).simplify();
    };
    OrExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue || expressionValue;
    };
    OrExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "||" + expressionJS + ")";
    };
    OrExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + " OR " + expressionSQL + ")";
    };
    OrExpression.prototype.isCommutative = function () {
        return true;
    };
    OrExpression.prototype.isAssociative = function () {
        return true;
    };
    OrExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.TRUE))
            return Expression.TRUE;
        if (expression.equals(Expression.FALSE))
            return operand;
        if (operand instanceof OrExpression) {
            var orExpressions = operand.getExpressionList();
            for (var i = 0; i < orExpressions.length; i++) {
                var orExpression = orExpressions[i];
                var mergedExpression = OrExpression.merge(orExpression, expression);
                if (mergedExpression) {
                    orExpressions[i] = mergedExpression;
                    return Expression.or(orExpressions).simplify();
                }
            }
        }
        else {
            var mergedExpression = OrExpression.merge(operand, expression);
            if (mergedExpression)
                return mergedExpression;
        }
        return this;
    };
    OrExpression.op = "Or";
    return OrExpression;
}(ChainableUnaryExpression));
exports.OrExpression = OrExpression;
Expression.register(OrExpression);
var OverlapExpression = (function (_super) {
    tslib_1.__extends(OverlapExpression, _super);
    function OverlapExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("overlap");
        var operandType = Range.unwrapRangeType(Set.unwrapSetType(_this.operand.type));
        var expressionType = Range.unwrapRangeType(Set.unwrapSetType(_this.expression.type));
        if (!(!operandType || operandType === 'NULL' || !expressionType || expressionType === 'NULL' || operandType === expressionType)) {
            throw new Error(_this.op + " must have matching types (are " + _this.operand.type + ", " + _this.expression.type + ")");
        }
        _this.type = 'BOOLEAN';
        return _this;
    }
    OverlapExpression.fromJS = function (parameters) {
        return new OverlapExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    OverlapExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return Set.crossBinaryBoolean(operandValue, expressionValue, function (a, b) {
            if (a instanceof Range) {
                return b instanceof Range ? a.intersects(b) : a.containsValue(b);
            }
            else {
                return b instanceof Range ? b.containsValue(a) : a === b;
            }
        });
    };
    OverlapExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        var expression = this.expression;
        if (expression instanceof LiteralExpression) {
            if (Range.isRangeType(expression.type)) {
                var range = expression.value;
                var r0 = range.start;
                var r1 = range.end;
                var bounds = range.bounds;
                var cmpStrings = [];
                if (r0 != null) {
                    cmpStrings.push("" + JSON.stringify(r0) + (bounds[0] === '(' ? '<' : '<=') + "_");
                }
                if (r1 != null) {
                    cmpStrings.push("_" + (bounds[1] === ')' ? '<' : '<=') + JSON.stringify(r1));
                }
                return "((_=" + operandJS + ")," + cmpStrings.join('&&') + ")";
            }
            else {
                throw new Error("can not convert " + this + " to JS function, unsupported type " + expression.type);
            }
        }
        throw new Error("can not convert " + this + " to JS function");
    };
    OverlapExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        var expression = this.expression;
        var expressionType = expression.type;
        switch (expressionType) {
            case 'NUMBER_RANGE':
            case 'TIME_RANGE':
                if (expression instanceof LiteralExpression) {
                    var range = expression.value;
                    return dialect.inExpression(operandSQL, dialect.numberOrTimeToSQL(range.start), dialect.numberOrTimeToSQL(range.end), range.bounds);
                }
                throw new Error("can not convert action to SQL " + this);
            case 'STRING_RANGE':
                if (expression instanceof LiteralExpression) {
                    var stringRange = expression.value;
                    return dialect.inExpression(operandSQL, dialect.escapeLiteral(stringRange.start), dialect.escapeLiteral(stringRange.end), stringRange.bounds);
                }
                throw new Error("can not convert action to SQL " + this);
            case 'SET/NUMBER_RANGE':
            case 'SET/TIME_RANGE':
                if (expression instanceof LiteralExpression) {
                    var setOfRange = expression.value;
                    return setOfRange.elements.map(function (range) {
                        return dialect.inExpression(operandSQL, dialect.numberOrTimeToSQL(range.start), dialect.numberOrTimeToSQL(range.end), range.bounds);
                    }).join(' OR ');
                }
                throw new Error("can not convert action to SQL " + this);
            default:
                throw new Error("can not convert action to SQL " + this);
        }
    };
    OverlapExpression.prototype.isCommutative = function () {
        return true;
    };
    OverlapExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        var literalValue = expression.getLiteralValue();
        if (literalValue instanceof Set) {
            if (literalValue.empty())
                return Expression.FALSE;
            var simpleSet = literalValue.simplifyCover();
            if (simpleSet !== literalValue) {
                return operand.overlap(r(simpleSet));
            }
        }
        if (!Range.isRangeType(operand.type) && !Range.isRangeType(expression.type))
            return operand.is(expression);
        if (operand instanceof IndexOfExpression && literalValue instanceof NumberRange) {
            var x = operand.operand, y = operand.expression;
            var start = literalValue.start, end = literalValue.end, bounds = literalValue.bounds;
            if ((start < 0 && end === null) || (start === 0 && end === null && bounds[0] === '[')) {
                return x.contains(y);
            }
        }
        return this;
    };
    OverlapExpression.op = "Overlap";
    return OverlapExpression;
}(ChainableUnaryExpression));
exports.OverlapExpression = OverlapExpression;
Expression.register(OverlapExpression);
var PowerExpression = (function (_super) {
    tslib_1.__extends(PowerExpression, _super);
    function PowerExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("power");
        _this._checkOperandTypes('NUMBER');
        _this._checkExpressionTypes('NUMBER');
        _this.type = Set.isSetType(_this.operand.type) ? _this.operand.type : _this.expression.type;
        return _this;
    }
    PowerExpression.fromJS = function (parameters) {
        return new PowerExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    PowerExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue == null || expressionValue == null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) {
            var pow = Math.pow(a, b);
            return isNaN(pow) ? null : pow;
        });
    };
    PowerExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "Math.pow(" + operandJS + "," + expressionJS + ")";
    };
    PowerExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "POWER(" + operandSQL + "," + expressionSQL + ")";
    };
    PowerExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.ZERO))
            return Expression.ONE;
        if (expression.equals(Expression.ONE))
            return operand;
        return this;
    };
    PowerExpression.op = "Power";
    return PowerExpression;
}(ChainableUnaryExpression));
exports.PowerExpression = PowerExpression;
Expression.register(PowerExpression);
var QuantileExpression = (function (_super) {
    tslib_1.__extends(QuantileExpression, _super);
    function QuantileExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("quantile");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('NUMBER');
        _this.value = parameters.value;
        _this.tuning = parameters.tuning;
        _this.type = 'NUMBER';
        return _this;
    }
    QuantileExpression.fromJS = function (parameters) {
        var value = ChainableUnaryExpression.jsToValue(parameters);
        value.value = parameters.value || parameters.quantile;
        value.tuning = parameters.tuning;
        return new QuantileExpression(value);
    };
    QuantileExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.value = this.value;
        value.tuning = this.tuning;
        return value;
    };
    QuantileExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.value = this.value;
        if (this.tuning)
            js.tuning = this.tuning;
        return js;
    };
    QuantileExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.value === other.value &&
            this.tuning === other.tuning;
    };
    QuantileExpression.prototype._toStringParameters = function (indent) {
        var params = [this.expression.toString(indent), String(this.value)];
        if (this.tuning)
            params.push(Expression.safeString(this.tuning));
        return params;
    };
    QuantileExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.quantile(this.expression, this.value) : null;
    };
    QuantileExpression.op = "Quantile";
    return QuantileExpression;
}(ChainableUnaryExpression));
exports.QuantileExpression = QuantileExpression;
Expression.applyMixins(QuantileExpression, [Aggregate]);
Expression.register(QuantileExpression);
var SelectExpression = (function (_super) {
    tslib_1.__extends(SelectExpression, _super);
    function SelectExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("select");
        _this._checkOperandTypes('DATASET');
        _this.attributes = parameters.attributes;
        _this.type = 'DATASET';
        return _this;
    }
    SelectExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.attributes = parameters.attributes;
        return new SelectExpression(value);
    };
    SelectExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.attributes = this.attributes;
        return value;
    };
    SelectExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.attributes = this.attributes;
        return js;
    };
    SelectExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            String(this.attributes) === String(other.attributes);
    };
    SelectExpression.prototype._toStringParameters = function (indent) {
        return this.attributes;
    };
    SelectExpression.prototype.updateTypeContext = function (typeContext) {
        var attributes = this.attributes;
        var datasetType = typeContext.datasetType, parent = typeContext.parent;
        var newDatasetType = Object.create(null);
        for (var _i = 0, attributes_1 = attributes; _i < attributes_1.length; _i++) {
            var attr = attributes_1[_i];
            var attrType = datasetType[attr];
            if (!attrType)
                throw new Error("unknown attribute '" + attr + "' in select");
            newDatasetType[attr] = attrType;
        }
        return {
            type: 'DATASET',
            datasetType: newDatasetType,
            parent: parent
        };
    };
    SelectExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue ? operandValue.select(this.attributes) : null;
    };
    SelectExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        throw new Error('can not be expressed as SQL directly');
    };
    SelectExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, attributes = _a.attributes;
        if (operand instanceof SelectExpression) {
            var x = operand.operand, attr = operand.attributes;
            return x.select(attr.filter(function (a) { return attributes.indexOf(a) !== -1; }));
        }
        else if (operand instanceof ApplyExpression) {
            var x = operand.operand, name_1 = operand.name;
            if (attributes.indexOf(name_1) === -1) {
                return this.changeOperand(x);
            }
        }
        return this;
    };
    SelectExpression.op = "Select";
    return SelectExpression;
}(ChainableExpression));
exports.SelectExpression = SelectExpression;
Expression.register(SelectExpression);
var SortExpression = (function (_super) {
    tslib_1.__extends(SortExpression, _super);
    function SortExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("sort");
        _this._checkOperandTypes('DATASET');
        if (!_this.expression.isOp('ref')) {
            throw new Error("must be a reference expression: " + _this.expression);
        }
        var direction = parameters.direction || SortExpression.DEFAULT_DIRECTION;
        if (direction !== SortExpression.DESCENDING && direction !== SortExpression.ASCENDING) {
            throw new Error("direction must be '" + SortExpression.DESCENDING + "' or '" + SortExpression.ASCENDING + "'");
        }
        _this.direction = direction;
        _this.type = 'DATASET';
        return _this;
    }
    SortExpression.fromJS = function (parameters) {
        var value = ChainableUnaryExpression.jsToValue(parameters);
        value.direction = parameters.direction;
        return new SortExpression(value);
    };
    SortExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.direction = this.direction;
        return value;
    };
    SortExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.direction = this.direction;
        return js;
    };
    SortExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.direction === other.direction;
    };
    SortExpression.prototype._toStringParameters = function (indent) {
        return [this.expression.toString(indent), this.direction];
    };
    SortExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.sort(this.expression, this.direction) : null;
    };
    SortExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        var dir = this.direction === SortExpression.DESCENDING ? 'DESC' : 'ASC';
        return "ORDER BY " + expressionSQL + " " + dir;
    };
    SortExpression.prototype.refName = function () {
        var expression = this.expression;
        return (expression instanceof RefExpression) ? expression.name : null;
    };
    SortExpression.prototype.isNester = function () {
        return true;
    };
    SortExpression.prototype.fullyDefined = function () {
        return this.operand.isOp('literal') && this.expression.resolved();
    };
    SortExpression.prototype.changeDirection = function (direction) {
        if (this.direction === direction)
            return this;
        var value = this.valueOf();
        value.direction = direction;
        return new SortExpression(value);
    };
    SortExpression.prototype.toggleDirection = function () {
        return this.changeDirection(this.direction === SortExpression.ASCENDING ? SortExpression.DESCENDING : SortExpression.ASCENDING);
    };
    SortExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (operand instanceof SortExpression && operand.expression.equals(expression))
            return this.changeOperand(operand.operand);
        return this;
    };
    SortExpression.DESCENDING = 'descending';
    SortExpression.ASCENDING = 'ascending';
    SortExpression.DEFAULT_DIRECTION = 'ascending';
    SortExpression.op = "Sort";
    return SortExpression;
}(ChainableUnaryExpression));
exports.SortExpression = SortExpression;
Expression.register(SortExpression);
var SplitExpression = (function (_super) {
    tslib_1.__extends(SplitExpression, _super);
    function SplitExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("split");
        _this._checkOperandTypes('DATASET');
        var splits = parameters.splits;
        if (!splits)
            throw new Error('must have splits');
        _this.splits = splits;
        _this.keys = Object.keys(splits).sort();
        if (!_this.keys.length)
            throw new Error('must have at least one split');
        _this.dataName = parameters.dataName;
        _this.type = 'DATASET';
        return _this;
    }
    SplitExpression.fromJS = function (parameters) {
        var _a;
        var value = ChainableExpression.jsToValue(parameters);
        var splits;
        if (parameters.expression && parameters.name) {
            splits = (_a = {}, _a[parameters.name] = parameters.expression, _a);
        }
        else {
            splits = parameters.splits;
        }
        value.splits = Expression.expressionLookupFromJS(splits);
        value.dataName = parameters.dataName;
        return new SplitExpression(value);
    };
    SplitExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.splits = this.splits;
        value.dataName = this.dataName;
        return value;
    };
    SplitExpression.prototype.toJS = function () {
        var splits = this.splits;
        var js = _super.prototype.toJS.call(this);
        if (this.isMultiSplit()) {
            js.splits = Expression.expressionLookupToJS(splits);
        }
        else {
            for (var name_1 in splits) {
                js.name = name_1;
                js.expression = splits[name_1].toJS();
            }
        }
        js.dataName = this.dataName;
        return js;
    };
    SplitExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            immutableLookupsEqual(this.splits, other.splits) &&
            this.dataName === other.dataName;
    };
    SplitExpression.prototype.changeSplits = function (splits) {
        if (immutableLookupsEqual(this.splits, splits))
            return this;
        var value = this.valueOf();
        value.splits = splits;
        return new SplitExpression(value);
    };
    SplitExpression.prototype.numSplits = function () {
        return this.keys.length;
    };
    SplitExpression.prototype.isMultiSplit = function () {
        return this.numSplits() > 1;
    };
    SplitExpression.prototype._toStringParameters = function (indent) {
        if (this.isMultiSplit()) {
            var splits = this.splits;
            var splitStrings = [];
            for (var name_2 in splits) {
                splitStrings.push(name_2 + ": " + splits[name_2]);
            }
            return [splitStrings.join(', '), this.dataName];
        }
        else {
            return [this.firstSplitExpression().toString(), this.firstSplitName(), this.dataName];
        }
    };
    SplitExpression.prototype.updateTypeContext = function (typeContext) {
        var newDatasetType = {};
        this.mapSplits(function (name, expression) {
            newDatasetType[name] = {
                type: Set.unwrapSetType(expression.type)
            };
        });
        newDatasetType[this.dataName] = typeContext;
        return {
            parent: typeContext.parent,
            type: 'DATASET',
            datasetType: newDatasetType
        };
    };
    SplitExpression.prototype.firstSplitName = function () {
        return this.keys[0];
    };
    SplitExpression.prototype.firstSplitExpression = function () {
        return this.splits[this.firstSplitName()];
    };
    SplitExpression.prototype.getArgumentExpressions = function () {
        return this.mapSplits(function (name, ex) { return ex; });
    };
    SplitExpression.prototype.mapSplits = function (fn) {
        var _a = this, splits = _a.splits, keys = _a.keys;
        var res = [];
        for (var _i = 0, keys_1 = keys; _i < keys_1.length; _i++) {
            var k = keys_1[_i];
            var v = fn(k, splits[k]);
            if (typeof v !== 'undefined')
                res.push(v);
        }
        return res;
    };
    SplitExpression.prototype.mapSplitExpressions = function (fn) {
        var _a = this, splits = _a.splits, keys = _a.keys;
        var ret = Object.create(null);
        for (var _i = 0, keys_2 = keys; _i < keys_2.length; _i++) {
            var key = keys_2[_i];
            ret[key] = fn(splits[key], key);
        }
        return ret;
    };
    SplitExpression.prototype.addSplits = function (splits) {
        var newSplits = this.mapSplitExpressions(function (ex) { return ex; });
        for (var k in splits) {
            newSplits[k] = splits[k];
        }
        return this.changeSplits(newSplits);
    };
    SplitExpression.prototype.calc = function (datum) {
        var _a = this, operand = _a.operand, splits = _a.splits, dataName = _a.dataName;
        var operandValue = operand.calc(datum);
        return operandValue ? operandValue.split(splits, dataName) : null;
    };
    SplitExpression.prototype.getSQL = function (dialect) {
        var groupBys = this.mapSplits(function (name, expression) { return expression.getSQL(dialect); });
        return "GROUP BY " + groupBys.join(', ');
    };
    SplitExpression.prototype.getSelectSQL = function (dialect) {
        return this.mapSplits(function (name, expression) { return expression.getSQL(dialect) + " AS " + dialect.escapeName(name); });
    };
    SplitExpression.prototype.getGroupBySQL = function (dialect) {
        return this.mapSplits(function (name, expression) { return expression.getSQL(dialect); });
    };
    SplitExpression.prototype.getShortGroupBySQL = function () {
        return Object.keys(this.splits).map(function (d, i) { return String(i + 1); });
    };
    SplitExpression.prototype.fullyDefined = function () {
        return this.operand.isOp('literal') && this.mapSplits(function (name, expression) { return expression.resolved(); }).every(Boolean);
    };
    SplitExpression.prototype.simplify = function () {
        if (this.simple)
            return this;
        var simpleOperand = this.operand.simplify();
        var simpleSplits = this.mapSplitExpressions(function (ex) { return ex.simplify(); });
        var simpler = this.changeOperand(simpleOperand).changeSplits(simpleSplits);
        if (simpler.fullyDefined())
            return r(this.calc({}));
        if (simpler instanceof ChainableExpression) {
            var pushedInExternal = simpler.pushIntoExternal();
            if (pushedInExternal)
                return pushedInExternal;
        }
        return simpler.markSimple();
    };
    SplitExpression.prototype._substituteHelper = function (substitutionFn, indexer, depth, nestDiff, typeContext) {
        var sub = substitutionFn.call(this, this, indexer.index, depth, nestDiff);
        if (sub) {
            indexer.index += this.expressionCount();
            return {
                expression: sub,
                typeContext: sub.updateTypeContextIfNeeded(typeContext)
            };
        }
        else {
            indexer.index++;
        }
        depth++;
        var operandSubs = this.operand._substituteHelper(substitutionFn, indexer, depth, nestDiff, typeContext);
        var nestDiffNext = nestDiff + 1;
        var splitsSubs = this.mapSplitExpressions(function (ex) {
            return ex._substituteHelper(substitutionFn, indexer, depth, nestDiffNext, operandSubs.typeContext).expression;
        });
        var updatedThis = this.changeOperand(operandSubs.expression).changeSplits(splitsSubs);
        return {
            expression: updatedThis,
            typeContext: updatedThis.updateTypeContextIfNeeded(operandSubs.typeContext)
        };
    };
    SplitExpression.prototype.transformExpressions = function (fn) {
        return this.changeSplits(this.mapSplitExpressions(fn));
    };
    SplitExpression.prototype.filterFromDatum = function (datum) {
        return Expression.and(this.mapSplits(function (name, expression) {
            if (Set.isSetType(expression.type)) {
                return r(datum[name]).overlap(expression);
            }
            else {
                return expression.is(r(datum[name]));
            }
        })).simplify();
    };
    SplitExpression.prototype.hasKey = function (key) {
        return hasOwnProp(this.splits, key);
    };
    SplitExpression.prototype.isLinear = function () {
        var _a = this, splits = _a.splits, keys = _a.keys;
        for (var _i = 0, keys_3 = keys; _i < keys_3.length; _i++) {
            var k = keys_3[_i];
            var split = splits[k];
            if (Set.isSetType(split.type))
                return false;
        }
        return true;
    };
    SplitExpression.prototype.maxBucketNumber = function () {
        var _a = this, splits = _a.splits, keys = _a.keys;
        var num = 1;
        for (var _i = 0, keys_4 = keys; _i < keys_4.length; _i++) {
            var key = keys_4[_i];
            num *= splits[key].maxPossibleSplitValues();
        }
        return num;
    };
    SplitExpression.op = "Split";
    return SplitExpression;
}(ChainableExpression));
exports.SplitExpression = SplitExpression;
Expression.applyMixins(SplitExpression, [Aggregate]);
Expression.register(SplitExpression);
var SubstrExpression = (function (_super) {
    tslib_1.__extends(SubstrExpression, _super);
    function SubstrExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.position = parameters.position;
        _this.len = parameters.len;
        _this._ensureOp("substr");
        _this._checkOperandTypes('STRING');
        _this.type = _this.operand.type;
        return _this;
    }
    SubstrExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.position = parameters.position;
        value.len = parameters.len || parameters.length;
        return new SubstrExpression(value);
    };
    SubstrExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.position = this.position;
        value.len = this.len;
        return value;
    };
    SubstrExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.position = this.position;
        js.len = this.len;
        return js;
    };
    SubstrExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.position === other.position &&
            this.len === other.len;
    };
    SubstrExpression.prototype._toStringParameters = function (indent) {
        return [String(this.position), String(this.len)];
    };
    SubstrExpression.prototype._calcChainableHelper = function (operandValue) {
        if (operandValue === null)
            return null;
        var _a = this, position = _a.position, len = _a.len;
        return Set.crossUnary(operandValue, function (a) { return a.substr(position, len); });
    };
    SubstrExpression.prototype._getJSChainableHelper = function (operandJS) {
        var _a = this, position = _a.position, len = _a.len;
        return "((_=" + operandJS + "),_==null?null:(''+_).substr(" + position + "," + len + "))";
    };
    SubstrExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.substrExpression(operandSQL, this.position, this.len);
    };
    SubstrExpression.prototype.specialSimplify = function () {
        var len = this.len;
        if (len === 0)
            return Expression.EMPTY_STRING;
        return this;
    };
    SubstrExpression.op = "Substr";
    return SubstrExpression;
}(ChainableExpression));
exports.SubstrExpression = SubstrExpression;
Expression.register(SubstrExpression);
var SubtractExpression = (function (_super) {
    tslib_1.__extends(SubtractExpression, _super);
    function SubtractExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("subtract");
        _this._checkOperandTypes('NUMBER');
        _this._checkExpressionTypes('NUMBER');
        _this.type = 'NUMBER';
        return _this;
    }
    SubtractExpression.fromJS = function (parameters) {
        return new SubtractExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    SubtractExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) { return a - b; });
    };
    SubtractExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "-" + expressionJS + ")";
    };
    SubtractExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + "-" + expressionSQL + ")";
    };
    SubtractExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.ZERO))
            return operand;
        return this;
    };
    SubtractExpression.op = "Subtract";
    return SubtractExpression;
}(ChainableUnaryExpression));
exports.SubtractExpression = SubtractExpression;
Expression.register(SubtractExpression);
var SumExpression = (function (_super) {
    tslib_1.__extends(SumExpression, _super);
    function SumExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("sum");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('NUMBER');
        _this.type = 'NUMBER';
        return _this;
    }
    SumExpression.fromJS = function (parameters) {
        return new SumExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    SumExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.sum(this.expression) : null;
    };
    SumExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "SUM(" + dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL, '0') + ")";
    };
    SumExpression.prototype.distribute = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression instanceof LiteralExpression) {
            var value = expression.value;
            return operand.count().multiply(value).simplify();
        }
        if (expression instanceof AddExpression) {
            var lhs = expression.operand, rhs = expression.expression;
            return operand.sum(lhs).distribute().add(operand.sum(rhs).distribute()).simplify();
        }
        if (expression instanceof SubtractExpression) {
            var lhs = expression.operand, rhs = expression.expression;
            return operand.sum(lhs).distribute().subtract(operand.sum(rhs).distribute()).simplify();
        }
        if (expression instanceof MultiplyExpression) {
            var lhs = expression.operand, rhs = expression.expression;
            if (rhs instanceof LiteralExpression) {
                return operand.sum(lhs).distribute().multiply(rhs).simplify();
            }
        }
        return this;
    };
    SumExpression.op = "Sum";
    return SumExpression;
}(ChainableUnaryExpression));
exports.SumExpression = SumExpression;
Expression.applyMixins(SumExpression, [Aggregate]);
Expression.register(SumExpression);
var ThenExpression = (function (_super) {
    tslib_1.__extends(ThenExpression, _super);
    function ThenExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("then");
        _this._checkOperandTypes('BOOLEAN');
        _this.type = _this.expression.type;
        return _this;
    }
    ThenExpression.fromJS = function (parameters) {
        return new ThenExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    ThenExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? expressionValue : null;
    };
    ThenExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "((_=" + operandJS + "),(_?" + expressionJS + ":null))";
    };
    ThenExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return dialect.ifThenElseExpression(operandSQL, expressionSQL);
    };
    ThenExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.NULL))
            return operand;
        if (operand.equals(Expression.NULL))
            return Expression.NULL;
        if (operand.equals(Expression.FALSE))
            return Expression.NULL;
        if (operand.equals(Expression.TRUE))
            return expression;
        return this;
    };
    ThenExpression.op = "Then";
    return ThenExpression;
}(ChainableUnaryExpression));
exports.ThenExpression = ThenExpression;
Expression.register(ThenExpression);
var TimeBucketExpression = (function (_super) {
    tslib_1.__extends(TimeBucketExpression, _super);
    function TimeBucketExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        var duration = parameters.duration;
        _this.duration = duration;
        _this.timezone = parameters.timezone;
        _this._ensureOp("timeBucket");
        _this._checkOperandTypes('TIME');
        if (!(duration instanceof Duration)) {
            throw new Error("`duration` must be a Duration");
        }
        if (!duration.isFloorable()) {
            throw new Error("duration '" + duration.toString() + "' is not floorable");
        }
        _this.type = 'TIME_RANGE';
        return _this;
    }
    TimeBucketExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.duration = Duration.fromJS(parameters.duration);
        if (parameters.timezone)
            value.timezone = Timezone.fromJS(parameters.timezone);
        return new TimeBucketExpression(value);
    };
    TimeBucketExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.duration = this.duration;
        if (this.timezone)
            value.timezone = this.timezone;
        return value;
    };
    TimeBucketExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.duration = this.duration.toJS();
        if (this.timezone)
            js.timezone = this.timezone.toJS();
        return js;
    };
    TimeBucketExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.duration.equals(other.duration) &&
            immutableEqual(this.timezone, other.timezone);
    };
    TimeBucketExpression.prototype._toStringParameters = function (indent) {
        var ret = [this.duration.toString()];
        if (this.timezone)
            ret.push(Expression.safeString(this.timezone.toString()));
        return ret;
    };
    TimeBucketExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue ? TimeRange.timeBucket(operandValue, this.duration, this.getTimezone()) : null;
    };
    TimeBucketExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error("implement me");
    };
    TimeBucketExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.timeBucketExpression(operandSQL, this.duration, this.getTimezone());
    };
    TimeBucketExpression.op = "TimeBucket";
    return TimeBucketExpression;
}(ChainableExpression));
exports.TimeBucketExpression = TimeBucketExpression;
Expression.applyMixins(TimeBucketExpression, [HasTimezone]);
Expression.register(TimeBucketExpression);
var TimeFloorExpression = (function (_super) {
    tslib_1.__extends(TimeFloorExpression, _super);
    function TimeFloorExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        var duration = parameters.duration;
        _this.duration = duration;
        _this.timezone = parameters.timezone;
        _this._ensureOp("timeFloor");
        _this._bumpOperandToTime();
        _this._checkOperandTypes('TIME');
        if (!(duration instanceof Duration)) {
            throw new Error("`duration` must be a Duration");
        }
        if (!duration.isFloorable()) {
            throw new Error("duration '" + duration.toString() + "' is not floorable");
        }
        _this.type = 'TIME';
        return _this;
    }
    TimeFloorExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.duration = Duration.fromJS(parameters.duration);
        if (parameters.timezone)
            value.timezone = Timezone.fromJS(parameters.timezone);
        return new TimeFloorExpression(value);
    };
    TimeFloorExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.duration = this.duration;
        if (this.timezone)
            value.timezone = this.timezone;
        return value;
    };
    TimeFloorExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.duration = this.duration.toJS();
        if (this.timezone)
            js.timezone = this.timezone.toJS();
        return js;
    };
    TimeFloorExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.duration.equals(other.duration) &&
            immutableEqual(this.timezone, other.timezone);
    };
    TimeFloorExpression.prototype._toStringParameters = function (indent) {
        var ret = [this.duration.toString()];
        if (this.timezone)
            ret.push(Expression.safeString(this.timezone.toString()));
        return ret;
    };
    TimeFloorExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue ? this.duration.floor(operandValue, this.getTimezone()) : null;
    };
    TimeFloorExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error("implement me");
    };
    TimeFloorExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.timeFloorExpression(operandSQL, this.duration, this.getTimezone());
    };
    TimeFloorExpression.prototype.alignsWith = function (ex) {
        var _a = this, timezone = _a.timezone, duration = _a.duration;
        if (!timezone)
            return false;
        if (ex instanceof TimeFloorExpression || ex instanceof TimeBucketExpression) {
            return timezone.equals(ex.timezone) && ex.duration.dividesBy(duration);
        }
        if (ex instanceof OverlapExpression) {
            var literal = ex.expression.getLiteralValue();
            if (literal instanceof TimeRange) {
                return literal.isAligned(duration, timezone);
            }
            else if (literal instanceof Set) {
                if (literal.setType !== 'TIME_RANGE')
                    return false;
                return literal.elements.every(function (e) {
                    return e.isAligned(duration, timezone);
                });
            }
        }
        return false;
    };
    TimeFloorExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, duration = _a.duration, timezone = _a.timezone;
        if (operand instanceof TimeFloorExpression) {
            var d = operand.duration, tz = operand.timezone;
            if (duration.equals(d) && immutableEqual(timezone, tz))
                return operand;
        }
        return this;
    };
    TimeFloorExpression.op = "TimeFloor";
    return TimeFloorExpression;
}(ChainableExpression));
exports.TimeFloorExpression = TimeFloorExpression;
Expression.applyMixins(TimeFloorExpression, [HasTimezone]);
Expression.register(TimeFloorExpression);
var TimePartExpression = (function (_super) {
    tslib_1.__extends(TimePartExpression, _super);
    function TimePartExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.part = parameters.part;
        _this.timezone = parameters.timezone;
        _this._ensureOp("timePart");
        _this._checkOperandTypes('TIME');
        if (typeof _this.part !== 'string') {
            throw new Error("`part` must be a string");
        }
        _this.type = 'NUMBER';
        return _this;
    }
    TimePartExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.part = parameters.part;
        if (parameters.timezone)
            value.timezone = Timezone.fromJS(parameters.timezone);
        return new TimePartExpression(value);
    };
    TimePartExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.part = this.part;
        if (this.timezone)
            value.timezone = this.timezone;
        return value;
    };
    TimePartExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.part = this.part;
        if (this.timezone)
            js.timezone = this.timezone.toJS();
        return js;
    };
    TimePartExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.part === other.part &&
            immutableEqual(this.timezone, other.timezone);
    };
    TimePartExpression.prototype._toStringParameters = function (indent) {
        var ret = [this.part];
        if (this.timezone)
            ret.push(Expression.safeString(this.timezone.toString()));
        return ret;
    };
    TimePartExpression.prototype._calcChainableHelper = function (operandValue) {
        var part = this.part;
        var parter = TimePartExpression.PART_TO_FUNCTION[part];
        if (!parter)
            throw new Error("unsupported part '" + part + "'");
        if (!operandValue)
            return null;
        return parter(moment.tz(operandValue, this.getTimezone().toString()));
    };
    TimePartExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error("implement me");
    };
    TimePartExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.timePartExpression(operandSQL, this.part, this.getTimezone());
    };
    TimePartExpression.prototype.maxPossibleSplitValues = function () {
        var maxValue = TimePartExpression.PART_TO_MAX_VALUES[this.part];
        if (!maxValue)
            return Infinity;
        return maxValue + 1;
    };
    TimePartExpression.op = "TimePart";
    TimePartExpression.PART_TO_FUNCTION = {
        SECOND_OF_MINUTE: function (d) { return d.seconds(); },
        SECOND_OF_HOUR: function (d) { return d.minutes() * 60 + d.seconds(); },
        SECOND_OF_DAY: function (d) { return (d.hours() * 60 + d.minutes()) * 60 + d.seconds(); },
        SECOND_OF_WEEK: function (d) { return ((d.day() * 24) + d.hours() * 60 + d.minutes()) * 60 + d.seconds(); },
        SECOND_OF_MONTH: function (d) { return (((d.date() - 1) * 24) + d.hours() * 60 + d.minutes()) * 60 + d.seconds(); },
        SECOND_OF_YEAR: function (d) { return (((d.dayOfYear() - 1) * 24) + d.hours() * 60 + d.minutes()) * 60 + d.seconds(); },
        MINUTE_OF_HOUR: function (d) { return d.minutes(); },
        MINUTE_OF_DAY: function (d) { return d.hours() * 60 + d.minutes(); },
        MINUTE_OF_WEEK: function (d) { return (d.day() * 24) + d.hours() * 60 + d.minutes(); },
        MINUTE_OF_MONTH: function (d) { return ((d.date() - 1) * 24) + d.hours() * 60 + d.minutes(); },
        MINUTE_OF_YEAR: function (d) { return ((d.dayOfYear() - 1) * 24) + d.hours() * 60 + d.minutes(); },
        HOUR_OF_DAY: function (d) { return d.hours(); },
        HOUR_OF_WEEK: function (d) { return d.day() * 24 + d.hours(); },
        HOUR_OF_MONTH: function (d) { return (d.date() - 1) * 24 + d.hours(); },
        HOUR_OF_YEAR: function (d) { return (d.dayOfYear() - 1) * 24 + d.hours(); },
        DAY_OF_WEEK: function (d) { return d.day() || 7; },
        DAY_OF_MONTH: function (d) { return d.date(); },
        DAY_OF_YEAR: function (d) { return d.dayOfYear(); },
        MONTH_OF_YEAR: function (d) { return d.month(); },
        YEAR: function (d) { return d.year(); },
        QUARTER: function (d) { return d.quarter(); }
    };
    TimePartExpression.PART_TO_MAX_VALUES = {
        SECOND_OF_MINUTE: 61,
        SECOND_OF_HOUR: 3601,
        SECOND_OF_DAY: 93601,
        MINUTE_OF_HOUR: 60,
        MINUTE_OF_DAY: 26 * 60,
        HOUR_OF_DAY: 26,
        DAY_OF_WEEK: 7,
        DAY_OF_MONTH: 31,
        DAY_OF_YEAR: 366,
        WEEK_OF_MONTH: 5,
        WEEK_OF_YEAR: 53,
        MONTH_OF_YEAR: 12
    };
    return TimePartExpression;
}(ChainableExpression));
exports.TimePartExpression = TimePartExpression;
Expression.applyMixins(TimePartExpression, [HasTimezone]);
Expression.register(TimePartExpression);
var TimeRangeExpression = (function (_super) {
    tslib_1.__extends(TimeRangeExpression, _super);
    function TimeRangeExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.duration = parameters.duration;
        _this.step = parameters.step || TimeRangeExpression.DEFAULT_STEP;
        _this.timezone = parameters.timezone;
        _this._ensureOp("timeRange");
        _this._checkOperandTypes('TIME');
        if (!(_this.duration instanceof Duration)) {
            throw new Error("`duration` must be a Duration");
        }
        _this.type = 'TIME_RANGE';
        return _this;
    }
    TimeRangeExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.duration = Duration.fromJS(parameters.duration);
        value.step = parameters.step;
        if (parameters.timezone)
            value.timezone = Timezone.fromJS(parameters.timezone);
        return new TimeRangeExpression(value);
    };
    TimeRangeExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.duration = this.duration;
        value.step = this.step;
        if (this.timezone)
            value.timezone = this.timezone;
        return value;
    };
    TimeRangeExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.duration = this.duration.toJS();
        js.step = this.step;
        if (this.timezone)
            js.timezone = this.timezone.toJS();
        return js;
    };
    TimeRangeExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.duration.equals(other.duration) &&
            this.step === other.step &&
            immutableEqual(this.timezone, other.timezone);
    };
    TimeRangeExpression.prototype._toStringParameters = function (indent) {
        var ret = [this.duration.toString(), this.step.toString()];
        if (this.timezone)
            ret.push(Expression.safeString(this.timezone.toString()));
        return ret;
    };
    TimeRangeExpression.prototype.getQualifiedDurationDescription = function (capitalize) {
        var step = Math.abs(this.step);
        var durationDescription = this.duration.getDescription(capitalize);
        return step !== 1 ? pluralIfNeeded(step, durationDescription) : durationDescription;
    };
    TimeRangeExpression.prototype._calcChainableHelper = function (operandValue) {
        var duration = this.duration;
        var step = this.step;
        var timezone = this.getTimezone();
        if (operandValue === null)
            return null;
        var other = duration.shift(operandValue, timezone, step);
        if (step > 0) {
            return new TimeRange({ start: operandValue, end: other });
        }
        else {
            return new TimeRange({ start: other, end: operandValue });
        }
    };
    TimeRangeExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error("implement me");
    };
    TimeRangeExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        throw new Error("implement me");
    };
    TimeRangeExpression.DEFAULT_STEP = 1;
    TimeRangeExpression.op = "TimeRange";
    return TimeRangeExpression;
}(ChainableExpression));
exports.TimeRangeExpression = TimeRangeExpression;
Expression.applyMixins(TimeRangeExpression, [HasTimezone]);
Expression.register(TimeRangeExpression);
var TimeShiftExpression = (function (_super) {
    tslib_1.__extends(TimeShiftExpression, _super);
    function TimeShiftExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.duration = parameters.duration;
        _this.step = parameters.step != null ? parameters.step : TimeShiftExpression.DEFAULT_STEP;
        _this.timezone = parameters.timezone;
        _this._ensureOp("timeShift");
        _this._checkOperandTypes('TIME');
        if (!(_this.duration instanceof Duration)) {
            throw new Error("`duration` must be a Duration");
        }
        _this.type = 'TIME';
        return _this;
    }
    TimeShiftExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.duration = Duration.fromJS(parameters.duration);
        value.step = parameters.step;
        if (parameters.timezone)
            value.timezone = Timezone.fromJS(parameters.timezone);
        return new TimeShiftExpression(value);
    };
    TimeShiftExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.duration = this.duration;
        value.step = this.step;
        if (this.timezone)
            value.timezone = this.timezone;
        return value;
    };
    TimeShiftExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.duration = this.duration.toJS();
        js.step = this.step;
        if (this.timezone)
            js.timezone = this.timezone.toJS();
        return js;
    };
    TimeShiftExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.duration.equals(other.duration) &&
            this.step === other.step &&
            immutableEqual(this.timezone, other.timezone);
    };
    TimeShiftExpression.prototype._toStringParameters = function (indent) {
        var ret = [this.duration.toString(), this.step.toString()];
        if (this.timezone)
            ret.push(Expression.safeString(this.timezone.toString()));
        return ret;
    };
    TimeShiftExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue ? this.duration.shift(operandValue, this.getTimezone(), this.step) : null;
    };
    TimeShiftExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error("implement me");
    };
    TimeShiftExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.timeShiftExpression(operandSQL, this.duration, this.getTimezone());
    };
    TimeShiftExpression.prototype.changeStep = function (step) {
        if (this.step === step)
            return this;
        var value = this.valueOf();
        value.step = step;
        return new TimeShiftExpression(value);
    };
    TimeShiftExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, duration = _a.duration, step = _a.step, timezone = _a.timezone;
        if (step === 0)
            return operand;
        if (operand instanceof TimeShiftExpression) {
            var x = operand.operand, d = operand.duration, s = operand.step, tz = operand.timezone;
            if (duration.equals(d) && immutableEqual(timezone, tz)) {
                return x.timeShift(d, step + s, tz);
            }
        }
        return this;
    };
    TimeShiftExpression.DEFAULT_STEP = 1;
    TimeShiftExpression.op = "TimeShift";
    return TimeShiftExpression;
}(ChainableExpression));
exports.TimeShiftExpression = TimeShiftExpression;
Expression.applyMixins(TimeShiftExpression, [HasTimezone]);
Expression.register(TimeShiftExpression);
var TransformCaseExpression = (function (_super) {
    tslib_1.__extends(TransformCaseExpression, _super);
    function TransformCaseExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        var transformType = parameters.transformType;
        if (transformType !== TransformCaseExpression.UPPER_CASE && transformType !== TransformCaseExpression.LOWER_CASE) {
            throw new Error("Must supply transform type of '" + TransformCaseExpression.UPPER_CASE + "' or '" + TransformCaseExpression.LOWER_CASE + "'");
        }
        _this.transformType = transformType;
        _this._ensureOp("transformCase");
        _this._checkOperandTypes('STRING');
        _this.type = 'STRING';
        return _this;
    }
    TransformCaseExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.transformType = parameters.transformType;
        return new TransformCaseExpression(value);
    };
    TransformCaseExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.transformType = this.transformType;
        return value;
    };
    TransformCaseExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.transformType = this.transformType;
        return js;
    };
    TransformCaseExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.transformType === other.transformType;
    };
    TransformCaseExpression.prototype._calcChainableHelper = function (operandValue) {
        var transformType = this.transformType;
        return transformType === TransformCaseExpression.UPPER_CASE ? String(operandValue).toLocaleUpperCase() : String(operandValue).toLocaleLowerCase();
    };
    TransformCaseExpression.prototype._getJSChainableHelper = function (operandJS) {
        var transformType = this.transformType;
        return Expression.jsNullSafetyUnary(operandJS, function (input) {
            return transformType === TransformCaseExpression.UPPER_CASE ? "String(" + input + ").toLocaleUpperCase()" : "String(" + input + ").toLocaleLowerCase()";
        });
    };
    TransformCaseExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        var transformType = this.transformType;
        return transformType === TransformCaseExpression.UPPER_CASE ? "UPPER(" + operandSQL + ")" : "LOWER(" + operandSQL + ")";
    };
    TransformCaseExpression.prototype.specialSimplify = function () {
        var operand = this.operand;
        if (operand instanceof TransformCaseExpression)
            return this.changeOperand(operand.operand);
        return this;
    };
    TransformCaseExpression.UPPER_CASE = 'upperCase';
    TransformCaseExpression.LOWER_CASE = 'lowerCase';
    TransformCaseExpression.op = "TransformCase";
    return TransformCaseExpression;
}(ChainableExpression));
exports.TransformCaseExpression = TransformCaseExpression;
Expression.register(TransformCaseExpression);
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
exports.TotalContainer = TotalContainer;
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
            case 'NUMBER': return External.numberInflaterFactory(label);
            case 'TIME': return External.timeInflaterFactory(label);
            default: return null;
        }
    };
    External.booleanInflaterFactory = function (label) {
        return function (d) {
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
    External.timeInflaterFactory = function (label) {
        return function (d) {
            var v = d[label];
            if ('' + v === "null") {
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
exports.External = External;
var basicExecutorFactory = exports.basicExecutorFactory = function(parameters) {
    var datasets = parameters.datasets;
    return function (ex, opt, computeContext) {
        if (opt === void 0) { opt = {}; }
        if (computeContext === void 0) { computeContext = {}; }
        return ex.compute(datasets, opt, computeContext);
    };
}
Expression.expressionParser = require("./expressionParser")(exports, Chronoshift);



