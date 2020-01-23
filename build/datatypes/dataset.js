import { isDate, Timezone } from 'chronoshift';
import * as hasOwnProp from 'has-own-prop';
import { generalEqual, NamedArray, SimpleArray } from 'immutable-class';
import { Expression, ExternalExpression, LiteralExpression } from '../expressions/index';
import { External, TotalContainer } from '../external/baseExternal';
import { AttributeInfo } from './attributeInfo';
import { datumHasExternal, valueFromJS, valueToJS } from './common';
import { NumberRange } from './numberRange';
import { Set } from './set';
import { StringRange } from './stringRange';
import { TimeRange } from './timeRange';
export function fillExpressionExternalAlteration(alteration, filler) {
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
export function sizeOfExpressionExternalAlteration(alteration) {
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
export function fillDatasetExternalAlterations(alterations, filler) {
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
export function sizeOfDatasetExternalAlterations(alterations) {
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
export { Dataset };
check = Dataset;
