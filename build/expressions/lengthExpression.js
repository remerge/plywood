import * as tslib_1 from "tslib";
import { ChainableExpression, Expression } from './baseExpression';
var LengthExpression = (function (_super) {
    tslib_1.__extends(LengthExpression, _super);
    function LengthExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("length");
        _this._checkOperandTypes('STRING');
        _this.type = 'NUMBER';
        return _this;
    }
    LengthExpression.fromJS = function (parameters) {
        return new LengthExpression(ChainableExpression.jsToValue(parameters));
    };
    LengthExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue ? operandValue.length : null;
    };
    LengthExpression.prototype._getJSChainableHelper = function (operandJS) {
        return Expression.jsNullSafetyUnary(operandJS, function (input) { return input + ".length"; });
    };
    LengthExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.lengthExpression(operandSQL);
    };
    LengthExpression.op = "Length";
    return LengthExpression;
}(ChainableExpression));
export { LengthExpression };
Expression.register(LengthExpression);
