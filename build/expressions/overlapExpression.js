import * as tslib_1 from "tslib";
import { NumberRange, Range, Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression, r } from './baseExpression';
import { IndexOfExpression } from './indexOfExpression';
import { LiteralExpression } from './literalExpression';
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
export { OverlapExpression };
Expression.register(OverlapExpression);
