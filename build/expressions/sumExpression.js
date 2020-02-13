import * as tslib_1 from "tslib";
import { AddExpression } from './addExpression';
import { ChainableUnaryExpression, Expression } from './baseExpression';
import { LiteralExpression } from './literalExpression';
import { Aggregate } from './mixins/aggregate';
import { MultiplyExpression } from './multiplyExpression';
import { SubtractExpression } from './subtractExpression';
var SumExpression = (function (_super) {
    tslib_1.__extends(SumExpression, _super);
    function SumExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("sum");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('NUMBER');
        _this.type = 'NUMBER';
        return _this;
    }
    SumExpression.fromJS = function (parameters) {
        return new SumExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    SumExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.sum(this.expression) : null;
    };
    SumExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "SUM(" + dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL, '0') + ")";
    };
    SumExpression.prototype.distribute = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression instanceof LiteralExpression) {
            var value = expression.value;
            return operand.count().multiply(value).simplify();
        }
        if (expression instanceof AddExpression) {
            var lhs = expression.operand, rhs = expression.expression;
            return operand.sum(lhs).distribute().add(operand.sum(rhs).distribute()).simplify();
        }
        if (expression instanceof SubtractExpression) {
            var lhs = expression.operand, rhs = expression.expression;
            return operand.sum(lhs).distribute().subtract(operand.sum(rhs).distribute()).simplify();
        }
        if (expression instanceof MultiplyExpression) {
            var lhs = expression.operand, rhs = expression.expression;
            if (rhs instanceof LiteralExpression) {
                return operand.sum(lhs).distribute().multiply(rhs).simplify();
            }
        }
        return this;
    };
    SumExpression.op = "Sum";
    return SumExpression;
}(ChainableUnaryExpression));
export { SumExpression };
Expression.applyMixins(SumExpression, [Aggregate]);
Expression.register(SumExpression);
