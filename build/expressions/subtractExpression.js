import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression } from './baseExpression';
var SubtractExpression = (function (_super) {
    tslib_1.__extends(SubtractExpression, _super);
    function SubtractExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("subtract");
        _this._checkOperandTypes('NUMBER');
        _this._checkExpressionTypes('NUMBER');
        _this.type = 'NUMBER';
        return _this;
    }
    SubtractExpression.fromJS = function (parameters) {
        return new SubtractExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    SubtractExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) { return a - b; });
    };
    SubtractExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "-" + expressionJS + ")";
    };
    SubtractExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + "-" + expressionSQL + ")";
    };
    SubtractExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.ZERO))
            return operand;
        return this;
    };
    SubtractExpression.op = "Subtract";
    return SubtractExpression;
}(ChainableUnaryExpression));
export { SubtractExpression };
Expression.register(SubtractExpression);
