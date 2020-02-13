import * as tslib_1 from "tslib";
import { ChainableExpression, Expression } from './baseExpression';
var NotExpression = (function (_super) {
    tslib_1.__extends(NotExpression, _super);
    function NotExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("not");
        _this._checkOperandTypes('BOOLEAN');
        _this.type = 'BOOLEAN';
        return _this;
    }
    NotExpression.fromJS = function (parameters) {
        return new NotExpression(ChainableExpression.jsToValue(parameters));
    };
    NotExpression.prototype._calcChainableHelper = function (operandValue) {
        return !operandValue;
    };
    NotExpression.prototype._getJSChainableHelper = function (operandJS) {
        return "!(" + operandJS + ")";
    };
    NotExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return "NOT(" + operandSQL + ")";
    };
    NotExpression.prototype.specialSimplify = function () {
        var operand = this.operand;
        if (operand instanceof NotExpression)
            return operand.operand;
        return this;
    };
    NotExpression.op = "Not";
    return NotExpression;
}(ChainableExpression));
export { NotExpression };
Expression.register(NotExpression);
