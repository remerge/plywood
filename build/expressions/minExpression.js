import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
var MinExpression = (function (_super) {
    tslib_1.__extends(MinExpression, _super);
    function MinExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("min");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('NUMBER', 'TIME');
        _this.type = Set.unwrapSetType(_this.expression.type);
        return _this;
    }
    MinExpression.fromJS = function (parameters) {
        return new MinExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    MinExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.min(this.expression) : null;
    };
    MinExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "MIN(" + dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL) + ")";
    };
    MinExpression.op = "Min";
    return MinExpression;
}(ChainableUnaryExpression));
export { MinExpression };
Expression.applyMixins(MinExpression, [Aggregate]);
Expression.register(MinExpression);
