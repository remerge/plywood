import * as tslib_1 from "tslib";
import { ChainableExpression, Expression } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
var CustomAggregateExpression = (function (_super) {
    tslib_1.__extends(CustomAggregateExpression, _super);
    function CustomAggregateExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.custom = parameters.custom;
        _this._ensureOp("customAggregate");
        _this._checkOperandTypes('DATASET');
        _this.type = 'NUMBER';
        return _this;
    }
    CustomAggregateExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.custom = parameters.custom;
        return new CustomAggregateExpression(value);
    };
    CustomAggregateExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.custom = this.custom;
        return value;
    };
    CustomAggregateExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.custom = this.custom;
        return js;
    };
    CustomAggregateExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.custom === other.custom;
    };
    CustomAggregateExpression.prototype._toStringParameters = function (indent) {
        return [this.custom];
    };
    CustomAggregateExpression.prototype._calcChainableHelper = function (operandValue) {
        throw new Error('can not compute on custom action');
    };
    CustomAggregateExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        throw new Error('custom action not implemented');
    };
    CustomAggregateExpression.op = "CustomAggregate";
    return CustomAggregateExpression;
}(ChainableExpression));
export { CustomAggregateExpression };
Expression.applyMixins(CustomAggregateExpression, [Aggregate]);
Expression.register(CustomAggregateExpression);
