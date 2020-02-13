import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableUnaryExpression, Expression } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
var CollectExpression = (function (_super) {
    tslib_1.__extends(CollectExpression, _super);
    function CollectExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("collect");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('BOOLEAN', 'NUMBER', 'TIME', 'STRING', 'NUMBER_RANGE', 'TIME_RANGE', 'STRING_RANGE');
        _this.type = Set.wrapSetType(_this.expression.type);
        return _this;
    }
    CollectExpression.fromJS = function (parameters) {
        return new CollectExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    CollectExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.collect(this.expression) : null;
    };
    CollectExpression.op = "Collect";
    return CollectExpression;
}(ChainableUnaryExpression));
export { CollectExpression };
Expression.applyMixins(CollectExpression, [Aggregate]);
Expression.register(CollectExpression);
