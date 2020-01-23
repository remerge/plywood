import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableExpression, Expression } from './baseExpression';
var ExtractExpression = (function (_super) {
    tslib_1.__extends(ExtractExpression, _super);
    function ExtractExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.regexp = parameters.regexp;
        _this._ensureOp("extract");
        _this._checkOperandTypes('STRING');
        _this.type = _this.operand.type;
        return _this;
    }
    ExtractExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.regexp = parameters.regexp;
        return new ExtractExpression(value);
    };
    ExtractExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.regexp = this.regexp;
        return value;
    };
    ExtractExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.regexp = this.regexp;
        return js;
    };
    ExtractExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.regexp === other.regexp;
    };
    ExtractExpression.prototype._toStringParameters = function (indent) {
        return [this.regexp];
    };
    ExtractExpression.prototype._calcChainableHelper = function (operandValue) {
        if (!operandValue)
            return null;
        var re = new RegExp(this.regexp);
        return Set.crossUnary(operandValue, function (a) { return (String(a).match(re) || [])[1] || null; });
    };
    ExtractExpression.prototype._getJSChainableHelper = function (operandJS) {
        return "((''+" + operandJS + ").match(/" + this.regexp + "/) || [])[1] || null";
    };
    ExtractExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.extractExpression(operandSQL, this.regexp);
    };
    ExtractExpression.op = "Extract";
    return ExtractExpression;
}(ChainableExpression));
export { ExtractExpression };
Expression.register(ExtractExpression);
