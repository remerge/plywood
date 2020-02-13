import * as hasOwnProp from 'has-own-prop';
import { immutableEqual, NamedArray } from 'immutable-class';
import { Expression, RefExpression } from '../expressions';
import { Range } from './range';
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
export { AttributeInfo };
check = AttributeInfo;
