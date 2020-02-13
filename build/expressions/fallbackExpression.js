import * as tslib_1 from "tslib";
import { ChainableUnaryExpression, Expression } from './baseExpression';
var FallbackExpression = (function (_super) {
    tslib_1.__extends(FallbackExpression, _super);
    function FallbackExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("fallback");
        _this._checkOperandExpressionTypesAlign();
        _this.type = _this.operand.type || _this.expression.type;
        return _this;
    }
    FallbackExpression.fromJS = function (parameters) {
        return new FallbackExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    FallbackExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue !== null ? operandValue : expressionValue;
    };
    FallbackExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "((_=" + operandJS + "),(_!==null?_:" + expressionJS + "))";
    };
    FallbackExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return dialect.coalesceExpression(operandSQL, expressionSQL);
    };
    FallbackExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression.equals(Expression.NULL))
            return operand;
        if (operand.equals(Expression.NULL))
            return expression;
        if (operand.equals(expression))
            return operand;
        if (operand.getLiteralValue() != null)
            return operand;
        return this;
    };
    FallbackExpression.op = "Fallback";
    return FallbackExpression;
}(ChainableUnaryExpression));
export { FallbackExpression };
Expression.register(FallbackExpression);
