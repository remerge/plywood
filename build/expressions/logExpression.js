import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression } from './baseExpression';
var LogExpression = (function (_super) {
    tslib_1.__extends(LogExpression, _super);
    function LogExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("log");
        _this._checkOperandTypes('NUMBER');
        _this._checkExpressionTypes('NUMBER');
        _this.type = Set.isSetType(_this.operand.type) ? _this.operand.type : _this.expression.type;
        return _this;
    }
    LogExpression.fromJS = function (parameters) {
        return new LogExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    LogExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue == null || expressionValue == null)
            return null;
        return Set.crossBinary(operandValue, expressionValue, function (a, b) {
            var log = Math.log(a) / Math.log(b);
            return isNaN(log) ? null : log;
        });
    };
    LogExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(Math.log(" + operandJS + ")/Math.log(" + expressionJS + "))";
    };
    LogExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        var myLiteral = this.expression.getLiteralValue();
        if (myLiteral === Math.E)
            return "LN(" + operandSQL + ")";
        return "LOG(" + expressionSQL + "," + operandSQL + ")";
    };
    LogExpression.prototype.specialSimplify = function () {
        var operand = this.operand;
        if (operand.equals(Expression.ONE))
            return Expression.ZERO;
        return this;
    };
    LogExpression.op = "Log";
    return LogExpression;
}(ChainableUnaryExpression));
export { LogExpression };
Expression.register(LogExpression);
