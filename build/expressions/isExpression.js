import * as tslib_1 from "tslib";
import { NumberRange, Set, TimeRange } from '../datatypes/index';
import { ChainableUnaryExpression, Expression, r } from './baseExpression';
import { IndexOfExpression } from './indexOfExpression';
import { LiteralExpression } from './literalExpression';
import { NumberBucketExpression } from './numberBucketExpression';
import { ThenExpression } from './thenExpression';
import { TimeBucketExpression } from './timeBucketExpression';
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
export { IsExpression };
Expression.register(IsExpression);
