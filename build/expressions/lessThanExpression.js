import * as tslib_1 from "tslib";
import { Set } from '../datatypes/index';
import { Range } from '../datatypes/range';
import { ChainableUnaryExpression, Expression, r } from './baseExpression';
import { LiteralExpression } from './literalExpression';
var LessThanExpression = (function (_super) {
    tslib_1.__extends(LessThanExpression, _super);
    function LessThanExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("lessThan");
        _this._checkOperandTypes('NUMBER', 'TIME', 'STRING');
        _this._checkExpressionTypes('NUMBER', 'TIME', 'STRING');
        _this._bumpOperandExpressionToTime();
        _this._checkOperandExpressionTypesAlign();
        _this.type = 'BOOLEAN';
        return _this;
    }
    LessThanExpression.fromJS = function (parameters) {
        return new LessThanExpression(ChainableUnaryExpression.jsToValue(parameters));
    };
    LessThanExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        if (operandValue === null || expressionValue === null)
            return null;
        return Set.crossBinaryBoolean(operandValue, expressionValue, function (a, b) { return a < b; });
    };
    LessThanExpression.prototype._getJSChainableUnaryHelper = function (operandJS, expressionJS) {
        return "(" + operandJS + "<" + expressionJS + ")";
    };
    LessThanExpression.prototype._getSQLChainableUnaryHelper = function (dialect, operandSQL, expressionSQL) {
        return "(" + operandSQL + "<" + expressionSQL + ")";
    };
    LessThanExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, expression = _a.expression;
        if (expression instanceof LiteralExpression) {
            return operand.overlap(r(Range.fromJS({ start: null, end: expression.value, bounds: '()' })));
        }
        if (operand instanceof LiteralExpression) {
            return expression.overlap(r(Range.fromJS({ start: operand.value, end: null, bounds: '()' })));
        }
        return this;
    };
    LessThanExpression.op = "LessThan";
    return LessThanExpression;
}(ChainableUnaryExpression));
export { LessThanExpression };
Expression.register(LessThanExpression);
