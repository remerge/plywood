import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableExpression, Expression } from './baseExpression';
var SubstrExpression = (function (_super) {
    tslib_1.__extends(SubstrExpression, _super);
    function SubstrExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.position = parameters.position;
        _this.len = parameters.len;
        _this._ensureOp("substr");
        _this._checkOperandTypes('STRING');
        _this.type = _this.operand.type;
        return _this;
    }
    SubstrExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.position = parameters.position;
        value.len = parameters.len || parameters.length;
        return new SubstrExpression(value);
    };
    SubstrExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.position = this.position;
        value.len = this.len;
        return value;
    };
    SubstrExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.position = this.position;
        js.len = this.len;
        return js;
    };
    SubstrExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.position === other.position &&
            this.len === other.len;
    };
    SubstrExpression.prototype._toStringParameters = function (indent) {
        return [String(this.position), String(this.len)];
    };
    SubstrExpression.prototype._calcChainableHelper = function (operandValue) {
        if (operandValue === null)
            return null;
        var _a = this, position = _a.position, len = _a.len;
        return Set.crossUnary(operandValue, function (a) { return a.substr(position, len); });
    };
    SubstrExpression.prototype._getJSChainableHelper = function (operandJS) {
        var _a = this, position = _a.position, len = _a.len;
        return "((_=" + operandJS + "),_==null?null:(''+_).substr(" + position + "," + len + "))";
    };
    SubstrExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.substrExpression(operandSQL, this.position, this.len);
    };
    SubstrExpression.prototype.specialSimplify = function () {
        var len = this.len;
        if (len === 0)
            return Expression.EMPTY_STRING;
        return this;
    };
    SubstrExpression.op = "Substr";
    return SubstrExpression;
}(ChainableExpression));
export { SubstrExpression };
Expression.register(SubstrExpression);
