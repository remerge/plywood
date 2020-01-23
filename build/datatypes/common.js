import { isDate } from 'chronoshift';
import * as hasOwnProp from 'has-own-prop';
import { Expression } from '../expressions/baseExpression';
import { External } from '../external/baseExternal';
import { Dataset } from './dataset';
import { NumberRange } from './numberRange';
import { Set } from './set';
import { StringRange } from './stringRange';
import { TimeRange } from './timeRange';
export function getValueType(value) {
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
export function getFullType(value) {
    var myType = getValueType(value);
    return myType === 'DATASET' ? value.getFullType() : { type: myType };
}
export function getFullTypeFromDatum(datum) {
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
export function valueFromJS(v, typeOverride) {
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
export function valueToJS(v) {
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
export function datumHasExternal(datum) {
    for (var name_1 in datum) {
        var value = datum[name_1];
        if (value instanceof External)
            return true;
        if (value instanceof Dataset && value.hasExternal())
            return true;
    }
    return false;
}
export function introspectDatum(datum) {
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
export function failIfIntrospectNeededInDatum(datum) {
    Object.keys(datum)
        .forEach(function (name) {
        var v = datum[name];
        if (v instanceof External && v.needsIntrospect()) {
            throw new Error('Can not have un-introspected external');
        }
    });
}
