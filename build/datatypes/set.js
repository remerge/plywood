import { Timezone } from 'chronoshift';
import * as hasOwnProp from 'has-own-prop';
import { generalEqual } from 'immutable-class';
import { getValueType, valueFromJS, valueToJS } from './common';
import { NumberRange } from './numberRange';
import { Range } from './range';
import { StringRange } from './stringRange';
import { TimeRange } from './timeRange';
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
export { Set };
check = Set;
Set.EMPTY = Set.fromJS([]);
