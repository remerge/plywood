import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression } from './baseExpression';
var MultiplyExpression = (function (_super) {
    tslib_1.__extends(MultiplyExpression, _super);
    function MultiplyExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("multiply");
        _this._checkOperandTypes('NUMBER');
        _this._checkExpressionTypes('NUMBER');
        _this.type = 'NUMBER';
        return _this;
    }
    MultiplyExpression.fromJS = function (parameters) {
        return new MultiplyExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    MultiplyExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) { return a * b; });
    };
    MultiplyExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "*" + expressionJS + ")";
    };
    MultiplyExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + "*" + expressionSQL + ")";
    };
    MultiplyExpression.prototype.isCommutative = function () {
        return true;
    };
    MultiplyExpression.prototype.isAssociative = function () {
        return true;
    };
    MultiplyExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.ZERO))
            return Expression.ZERO;
        if (expression.equals(Expression.ONE))
            return operand;
        return this;
    };
    MultiplyExpression.op = "Multiply";
    return MultiplyExpression;
}(ChainableUnaryExpression));
export { MultiplyExpression };
Expression.register(MultiplyExpression);
