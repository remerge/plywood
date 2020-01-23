import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression } from './baseExpression';
var AddExpression = (function (_super) {
    tslib_1.__extends(AddExpression, _super);
    function AddExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("add");
        _this._checkOperandTypes('NUMBER');
        _this._checkExpressionTypes('NUMBER');
        _this.type = 'NUMBER';
        return _this;
    }
    AddExpression.fromJS = function (parameters) {
        return new AddExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    AddExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) { return a + b; });
    };
    AddExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "+" + expressionJS + ")";
    };
    AddExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + "+" + expressionSQL + ")";
    };
    AddExpression.prototype.isCommutative = function () {
        return true;
    };
    AddExpression.prototype.isAssociative = function () {
        return true;
    };
    AddExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.ZERO))
            return operand;
        return this;
    };
    AddExpression.op = "Add";
    return AddExpression;
}(ChainableUnaryExpression));
export { AddExpression };
Expression.register(AddExpression);
