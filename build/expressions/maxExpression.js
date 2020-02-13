import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
var MaxExpression = (function (_super) {
    tslib_1.__extends(MaxExpression, _super);
    function MaxExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("max");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('NUMBER', 'TIME');
        _this.type = Set.unwrapSetType(_this.expression.type);
        return _this;
    }
    MaxExpression.fromJS = function (parameters) {
        return new MaxExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    MaxExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.max(this.expression) : null;
    };
    MaxExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "MAX(" + dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL) + ")";
    };
    MaxExpression.op = "Max";
    return MaxExpression;
}(ChainableUnaryExpression));
export { MaxExpression };
Expression.applyMixins(MaxExpression, [Aggregate]);
Expression.register(MaxExpression);
