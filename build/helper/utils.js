import * as tslib_1 from "tslib";
import * as hasOwnProp from 'has-own-prop';
export function repeat(str, times) {
    return new Array(times + 1).join(str);
}
export function indentBy(str, indent) {
    var spaces = repeat(' ', indent);
    return str.split('\n').map(function (x) { return spaces + x; }).join('\n');
}
export function dictEqual(dictA, dictB) {
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
export function shallowCopy(thing) {
    var newThing = {};
    for (var k in thing) {
        if (hasOwnProp(thing, k))
            newThing[k] = thing[k];
    }
    return newThing;
}
export function deduplicateSort(a) {
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
export function mapLookup(thing, fn) {
    var newThing = Object.create(null);
    for (var k in thing) {
        if (hasOwnProp(thing, k))
            newThing[k] = fn(thing[k]);
    }
    return newThing;
}
export function emptyLookup(lookup) {
    for (var k in lookup) {
        if (hasOwnProp(lookup, k))
            return false;
    }
    return true;
}
export function nonEmptyLookup(lookup) {
    return !emptyLookup(lookup);
}
export function clip(x) {
    var rx = Math.round(x);
    return Math.abs(x - rx) < 1e-5 ? rx : x;
}
export function safeAdd(num, delta) {
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
export function safeRange(num, delta) {
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
export function continuousFloorExpression(variable, floorFn, size, offset) {
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
export { ExtendableError };
export function pluralIfNeeded(n, thing) {
    return n + " " + thing + (n === 1 ? '' : 's');
}
export function pipeWithError(src, dest) {
    src.pipe(dest);
    src.on('error', function (e) { return dest.emit('error', e); });
    return dest;
}
