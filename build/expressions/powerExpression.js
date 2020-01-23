import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression } from './baseExpression';
var PowerExpression = (function (_super) {
    tslib_1.__extends(PowerExpression, _super);
    function PowerExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("power");
        _this._checkOperandTypes('NUMBER');
        _this._checkExpressionTypes('NUMBER');
        _this.type = Set.isSetType(_this.operand.type) ? _this.operand.type : _this.expression.type;
        return _this;
    }
    PowerExpression.fromJS = function (parameters) {
        return new PowerExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    PowerExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue == null || expressionValue == null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) {
            var pow = Math.pow(a, b);
            return isNaN(pow) ? null : pow;
        });
    };
    PowerExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "Math.pow(" + operandJS + "," + expressionJS + ")";
    };
    PowerExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "POWER(" + operandSQL + "," + expressionSQL + ")";
    };
    PowerExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.ZERO))
            return Expression.ONE;
        if (expression.equals(Expression.ONE))
            return operand;
        return this;
    };
    PowerExpression.op = "Power";
    return PowerExpression;
}(ChainableUnaryExpression));
export { PowerExpression };
Expression.register(PowerExpression);
