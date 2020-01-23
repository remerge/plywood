import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { Range } from '../datatypes/range';
import { ChainableUnaryExpression, Expression, r } from './baseExpression';
import { LiteralExpression } from './literalExpression';
var LessThanOrEqualExpression = (function (_super) {
    tslib_1.__extends(LessThanOrEqualExpression, _super);
    function LessThanOrEqualExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("lessThanOrEqual");
        _this._checkOperandTypes('NUMBER', 'TIME', 'STRING');
        _this._checkExpressionTypes('NUMBER', 'TIME', 'STRING');
        _this._bumpOperandExpressionToTime();
        _this._checkOperandExpressionTypesAlign();
        _this.type = 'BOOLEAN';
        return _this;
    }
    LessThanOrEqualExpression.fromJS = function (parameters) {
        return new LessThanOrEqualExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    LessThanOrEqualExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinaryBoolean(operandValue, expressionValue, function (a, b) { return a <= b; });
    };
    LessThanOrEqualExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "<=" + expressionJS + ")";
    };
    LessThanOrEqualExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + "<=" + expressionSQL + ")";
    };
    LessThanOrEqualExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression instanceof LiteralExpression) {
            return operand.overlap(r(Range.fromJS({ start: null, end: expression.value, bounds: '(]' })));
        }
        if (operand instanceof LiteralExpression) {
            return expression.overlap(r(Range.fromJS({ start: operand.value, end: null, bounds: '[)' })));
        }
        return this;
    };
    LessThanOrEqualExpression.op = "LessThanOrEqual";
    return LessThanOrEqualExpression;
}(ChainableUnaryExpression));
export { LessThanOrEqualExpression };
Expression.register(LessThanOrEqualExpression);
