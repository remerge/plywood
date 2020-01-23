import * as tslib_1 from "tslib";
import { parseISODate } from 'chronoshift';
import * as hasOwnProp from 'has-own-prop';
import { isImmutableClass } from 'immutable-class';
import { getValueType, valueFromJS } from '../datatypes/common';
import { Dataset, Set, TimeRange } from '../datatypes/index';
import { Expression, r } from './baseExpression';
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
export { LiteralExpression };
Expression.NULL = new LiteralExpression({ value: null });
Expression.ZERO = new LiteralExpression({ value: 0 });
Expression.ONE = new LiteralExpression({ value: 1 });
Expression.FALSE = new LiteralExpression({ value: false });
Expression.TRUE = new LiteralExpression({ value: true });
Expression.EMPTY_STRING = new LiteralExpression({ value: '' });
Expression.EMPTY_SET = new LiteralExpression({ value: Set.fromJS([]) });
Expression.register(LiteralExpression);
