import * as tslib_1 from "tslib";
import { ChainableUnaryExpression, Expression } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
var CountDistinctExpression = (function (_super) {
    tslib_1.__extends(CountDistinctExpression, _super);
    function CountDistinctExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("countDistinct");
        _this._checkOperandTypes('DATASET');
        _this.type = 'NUMBER';
        return _this;
    }
    CountDistinctExpression.fromJS = function (parameters) {
        return new CountDistinctExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    CountDistinctExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.countDistinct(this.expression) : null;
    };
    CountDistinctExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "COUNT(DISTINCT " + dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL) + ")";
    };
    CountDistinctExpression.op = "CountDistinct";
    return CountDistinctExpression;
}(ChainableUnaryExpression));
export { CountDistinctExpression };
Expression.applyMixins(CountDistinctExpression, [Aggregate]);
Expression.register(CountDistinctExpression);
