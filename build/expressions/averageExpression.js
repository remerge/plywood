import * as tslib_1 from "tslib";
import { ChainableUnaryExpression, Expression } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
var AverageExpression = (function (_super) {
    tslib_1.__extends(AverageExpression, _super);
    function AverageExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("average");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('NUMBER');
        _this.type = 'NUMBER';
        return _this;
    }
    AverageExpression.fromJS = function (parameters) {
        return new AverageExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    AverageExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.average(this.expression) : null;
    };
    AverageExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "AVG(" + dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL) + ")";
    };
    AverageExpression.prototype.decomposeAverage = function (countEx) {
        var _a = this, operand = _a.operand, expression = _a.expression;
        return operand.sum(expression).divide(countEx ? operand.sum(countEx) : operand.count());
    };
    AverageExpression.op = "Average";
    return AverageExpression;
}(ChainableUnaryExpression));
export { AverageExpression };
Expression.applyMixins(AverageExpression, [Aggregate]);
Expression.register(AverageExpression);
