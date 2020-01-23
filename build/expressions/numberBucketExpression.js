import * as tslib_1 from "tslib";
import * as hasOwnProp from 'has-own-prop';
import { NumberRange } from '../datatypes/numberRange';
import { continuousFloorExpression } from '../helper/utils';
import { ChainableExpression, Expression } from './baseExpression';
var NumberBucketExpression = (function (_super) {
    tslib_1.__extends(NumberBucketExpression, _super);
    function NumberBucketExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.size = parameters.size;
        _this.offset = parameters.offset;
        _this._ensureOp("numberBucket");
        _this._checkOperandTypes('NUMBER');
        _this.type = 'NUMBER_RANGE';
        return _this;
    }
    NumberBucketExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.size = parameters.size;
        value.offset = hasOwnProp(parameters, 'offset') ? parameters.offset : 0;
        return new NumberBucketExpression(value);
    };
    NumberBucketExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.size = this.size;
        value.offset = this.offset;
        return value;
    };
    NumberBucketExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.size = this.size;
        if (this.offset)
            js.offset = this.offset;
        return js;
    };
    NumberBucketExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.size === other.size &&
            this.offset === other.offset;
    };
    NumberBucketExpression.prototype._toStringParameters = function (indent) {
        var params = [String(this.size)];
        if (this.offset)
            params.push(String(this.offset));
        return params;
    };
    NumberBucketExpression.prototype._calcChainableHelper = function (operandValue) {
        return operandValue !== null ? NumberRange.numberBucket(operandValue, this.size, this.offset) : null;
    };
    NumberBucketExpression.prototype._getJSChainableHelper = function (operandJS) {
        var _this = this;
        return Expression.jsNullSafetyUnary(operandJS, function (n) { return continuousFloorExpression(n, "Math.floor", _this.size, _this.offset); });
    };
    NumberBucketExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return continuousFloorExpression(operandSQL, "FLOOR", this.size, this.offset);
    };
    NumberBucketExpression.op = "NumberBucket";
    return NumberBucketExpression;
}(ChainableExpression));
export { NumberBucketExpression };
Expression.register(NumberBucketExpression);
