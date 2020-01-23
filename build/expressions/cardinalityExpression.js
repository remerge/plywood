import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { ChainableExpression, Expression } from './baseExpression';
var CardinalityExpression = (function (_super) {
    tslib_1.__extends(CardinalityExpression, _super);
    function CardinalityExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("cardinality");
        _this._checkOperandTypes('BOOLEAN', 'STRING', 'STRING_RANGE', 'NUMBER', 'NUMBER_RANGE', 'TIME', 'TIME_RANGE');
        _this.type = 'NUMBER';
        return _this;
    }
    CardinalityExpression.fromJS = function (parameters) {
        return new CardinalityExpression(ChainableExpression.jsToValue(parameters));
    };
    CardinalityExpression.prototype._calcChainableHelper = function (operandValue) {
        if (operandValue == null)
            return null;
        return operandValue instanceof Set ? operandValue.cardinality() : 1;
    };
    CardinalityExpression.prototype._getJSChainableHelper = function (operandJS) {
        return Expression.jsNullSafetyUnary(operandJS, function (input) { return input + ".length"; });
    };
    CardinalityExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return "cardinality(" + operandSQL + ")";
    };
    CardinalityExpression.op = "Cardinality";
    return CardinalityExpression;
}(ChainableExpression));
export { CardinalityExpression };
Expression.register(CardinalityExpression);
