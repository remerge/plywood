import * as tslib_1 from "tslib";
import { ApplyExpression } from './applyExpression';
import { ChainableExpression, Expression } from './baseExpression';
var LimitExpression = (function (_super) {
    tslib_1.__extends(LimitExpression, _super);
    function LimitExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("limit");
        _this._checkOperandTypes('DATASET');
        var value = parameters.value;
        if (value == null)
            value = Infinity;
        if (value < 0)
            throw new Error("limit value can not be negative (is " + value + ")");
        _this.value = value;
        _this.type = 'DATASET';
        return _this;
    }
    LimitExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.value = parameters.value || parameters.limit;
        return new LimitExpression(value);
    };
    LimitExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.value = this.value;
        return value;
    };
    LimitExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.value = this.value;
        return js;
    };
    LimitExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.value === other.value;
    };
    LimitExpression.prototype._toStringParameters = function (indent) {
        return [String(this.value)];
    };
    LimitExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue ? operandValue.limit(this.value) : null;
    };
    LimitExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return "LIMIT " + this.value;
    };
    LimitExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, value = _a.value;
        if (!isFinite(value))
            return operand;
        if (operand instanceof LimitExpression) {
            var x = operand.operand, a = operand.value;
            return x.limit(Math.min(a, value));
        }
        if (operand instanceof ApplyExpression) {
            return this.swapWithOperand();
        }
        return this;
    };
    LimitExpression.op = "Limit";
    return LimitExpression;
}(ChainableExpression));
export { LimitExpression };
Expression.register(LimitExpression);
