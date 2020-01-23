import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableExpression, Expression } from './baseExpression';
var AbsoluteExpression = (function (_super) {
    tslib_1.__extends(AbsoluteExpression, _super);
    function AbsoluteExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("absolute");
        _this._checkOperandTypes('NUMBER');
        _this.type = _this.operand.type;
        return _this;
    }
    AbsoluteExpression.fromJS = function (parameters) {
        return new AbsoluteExpression(ChainableExpression.jsToValue(parameters));
    };
    AbsoluteExpression.prototype._calcChainableHelper = function (operandValue) {
        if (operandValue == null)
            return null;
        return Set.crossUnary(operandValue, function (a) { return Math.abs(a); });
    };
    AbsoluteExpression.prototype._getJSChainableHelper = function (operandJS) {
        return "Math.abs(" + operandJS + ")";
    };
    AbsoluteExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return "ABS(" + operandSQL + ")";
    };
    AbsoluteExpression.prototype.specialSimplify = function () {
        var operand = this.operand;
        if (operand instanceof AbsoluteExpression)
            return operand;
        return this;
    };
    AbsoluteExpression.op = "Absolute";
    return AbsoluteExpression;
}(ChainableExpression));
export { AbsoluteExpression };
Expression.register(AbsoluteExpression);
