import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression } from './baseExpression';
var DivideExpression = (function (_super) {
    tslib_1.__extends(DivideExpression, _super);
    function DivideExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("divide");
        _this._checkOperandTypes('NUMBER');
        _this._checkExpressionTypes('NUMBER');
        _this.type = 'NUMBER';
        return _this;
    }
    DivideExpression.fromJS = function (parameters) {
        return new DivideExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    DivideExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) { return b !== 0 ? a / b : null; });
    };
    DivideExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(_=" + expressionJS + ",(_===0||isNaN(_)?null:" + operandJS + "/" + expressionJS + "))";
    };
    DivideExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + "/" + expressionSQL + ")";
    };
    DivideExpression.prototype.specialSimplify = function () {
        if (this.expression.equals(Expression.ZERO))
            return Expression.NULL;
        if (this.expression.equals(Expression.ONE))
            return this.operand;
        return this;
    };
    DivideExpression.op = "Divide";
    return DivideExpression;
}(ChainableUnaryExpression));
export { DivideExpression };
Expression.register(DivideExpression);
