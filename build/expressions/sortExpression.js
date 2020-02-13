import * as tslib_1 from "tslib";
import { ChainableUnaryExpression, Expression } from './baseExpression';
import { RefExpression } from './refExpression';
var SortExpression = (function (_super) {
    tslib_1.__extends(SortExpression, _super);
    function SortExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("sort");
        _this._checkOperandTypes('DATASET');
        if (!_this.expression.isOp('ref')) {
            throw new Error("must be a reference expression: " + _this.expression);
        }
        var direction = parameters.direction || SortExpression.DEFAULT_DIRECTION;
        if (direction !== SortExpression.DESCENDING && direction !== SortExpression.ASCENDING) {
            throw new Error("direction must be '" + SortExpression.DESCENDING + "' or '" + SortExpression.ASCENDING + "'");
        }
        _this.direction = direction;
        _this.type = 'DATASET';
        return _this;
    }
    SortExpression.fromJS = function (parameters) {
        var value = ChainableUnaryExpression.jsToValue(parameters);
        value.direction = parameters.direction;
        return new SortExpression(value);
    };
    SortExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.direction = this.direction;
        return value;
    };
    SortExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.direction = this.direction;
        return js;
    };
    SortExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.direction === other.direction;
    };
    SortExpression.prototype._toStringParameters = function (indent) {
        return [this.expression.toString(indent), this.direction];
    };
    SortExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.sort(this.expression, this.direction) : null;
    };
    SortExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        var dir = this.direction === SortExpression.DESCENDING ? 'DESC' : 'ASC';
        return "ORDER BY " + expressionSQL + " " + dir;
    };
    SortExpression.prototype.refName = function () {
        var expression = this.expression;
        return (expression instanceof RefExpression) ? expression.name : null;
    };
    SortExpression.prototype.isNester = function () {
        return true;
    };
    SortExpression.prototype.fullyDefined = function () {
        return this.operand.isOp('literal') && this.expression.resolved();
    };
    SortExpression.prototype.changeDirection = function (direction) {
        if (this.direction === direction)
            return this;
        var value = this.valueOf();
        value.direction = direction;
        return new SortExpression(value);
    };
    SortExpression.prototype.toggleDirection = function () {
        return this.changeDirection(this.direction === SortExpression.ASCENDING ? SortExpression.DESCENDING : SortExpression.ASCENDING);
    };
    SortExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (operand instanceof SortExpression && operand.expression.equals(expression))
            return this.changeOperand(operand.operand);
        return this;
    };
    SortExpression.DESCENDING = 'descending';
    SortExpression.ASCENDING = 'ascending';
    SortExpression.DEFAULT_DIRECTION = 'ascending';
    SortExpression.op = "Sort";
    return SortExpression;
}(ChainableUnaryExpression));
export { SortExpression };
Expression.register(SortExpression);
