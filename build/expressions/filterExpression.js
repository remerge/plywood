import * as tslib_1 from "tslib";
import { ApplyExpression } from './applyExpression';
import { ChainableUnaryExpression, Expression } from './baseExpression';
import { RefExpression } from './refExpression';
import { SortExpression } from './sortExpression';
import { SplitExpression } from './splitExpression';
var FilterExpression = (function (_super) {
    tslib_1.__extends(FilterExpression, _super);
    function FilterExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("filter");
        _this._checkExpressionTypes('BOOLEAN');
        _this.type = 'DATASET';
        return _this;
    }
    FilterExpression.fromJS = function (parameters) {
        var value = ChainableUnaryExpression.jsToValue(parameters);
        return new FilterExpression(value);
    };
    FilterExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.filter(this.expression) : null;
    };
    FilterExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return operandSQL + " WHERE " + expressionSQL;
    };
    FilterExpression.prototype.isNester = function () {
        return true;
    };
    FilterExpression.prototype.fullyDefined = function () {
        return this.operand.isOp('literal') && this.expression.resolved();
    };
    FilterExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.TRUE))
            return operand;
        if (operand instanceof FilterExpression) {
            var x = operand.operand, a = operand.expression;
            return x.filter(a.and(expression));
        }
        if (operand instanceof ApplyExpression) {
            return expression.getFreeReferences().indexOf(operand.name) === -1 ? this.swapWithOperand() : this;
        }
        if (operand instanceof SplitExpression && operand.isLinear()) {
            var x = operand.operand, splits_1 = operand.splits, dataName = operand.dataName;
            var newFilter = expression.substitute(function (ex) {
                if (ex instanceof RefExpression && splits_1[ex.name])
                    return splits_1[ex.name];
                return null;
            });
            return x.filter(newFilter).split(splits_1, dataName);
        }
        if (operand instanceof SortExpression)
            return this.swapWithOperand();
        return this;
    };
    FilterExpression.op = "Filter";
    return FilterExpression;
}(ChainableUnaryExpression));
export { FilterExpression };
Expression.register(FilterExpression);
