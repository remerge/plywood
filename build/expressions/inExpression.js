import * as tslib_1 from "tslib";
import { Range, Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression } from './baseExpression';
import { OverlapExpression } from './overlapExpression';
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
export { InExpression };
Expression.register(InExpression);
