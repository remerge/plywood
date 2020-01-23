import * as tslib_1 from "tslib";
import { ChainableExpression, Expression } from './baseExpression';
var LookupExpression = (function (_super) {
    tslib_1.__extends(LookupExpression, _super);
    function LookupExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("lookup");
        _this._checkOperandTypes('STRING');
        _this.lookupFn = parameters.lookupFn;
        _this.type = _this.operand.type;
        return _this;
    }
    LookupExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.lookupFn = parameters.lookupFn || parameters.lookup;
        return new LookupExpression(value);
    };
    LookupExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.lookupFn = this.lookupFn;
        return value;
    };
    LookupExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.lookupFn = this.lookupFn;
        return js;
    };
    LookupExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.lookupFn === other.lookupFn;
    };
    LookupExpression.prototype._toStringParameters = function (indent) {
        return [Expression.safeString(this.lookupFn)];
    };
    LookupExpression.prototype.fullyDefined = function () {
        return false;
    };
    LookupExpression.prototype._calcChainableHelper = function (operandValue) {
        throw new Error('can not express as JS');
    };
    LookupExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error('can not express as JS');
    };
    LookupExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        throw new Error('can not express as SQL');
    };
    LookupExpression.op = "Lookup";
    return LookupExpression;
}(ChainableExpression));
export { LookupExpression };
Expression.register(LookupExpression);
