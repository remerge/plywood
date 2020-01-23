import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { Range } from '../datatypes/range';
import { ChainableUnaryExpression, Expression, r } from './baseExpression';
import { LiteralExpression } from './literalExpression';
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
export { GreaterThanOrEqualExpression };
Expression.register(GreaterThanOrEqualExpression);
