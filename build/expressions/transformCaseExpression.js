import * as tslib_1 from "tslib";
import { ChainableExpression, Expression } from './baseExpression';
var TransformCaseExpression = (function (_super) {
    tslib_1.__extends(TransformCaseExpression, _super);
    function TransformCaseExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        var transformType = parameters.transformType;
        if (transformType !== TransformCaseExpression.UPPER_CASE && transformType !== TransformCaseExpression.LOWER_CASE) {
            throw new Error("Must supply transform type of '" + TransformCaseExpression.UPPER_CASE + "' or '" + TransformCaseExpression.LOWER_CASE + "'");
        }
        _this.transformType = transformType;
        _this._ensureOp("transformCase");
        _this._checkOperandTypes('STRING');
        _this.type = 'STRING';
        return _this;
    }
    TransformCaseExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.transformType = parameters.transformType;
        return new TransformCaseExpression(value);
    };
    TransformCaseExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.transformType = this.transformType;
        return value;
    };
    TransformCaseExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.transformType = this.transformType;
        return js;
    };
    TransformCaseExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.transformType === other.transformType;
    };
    TransformCaseExpression.prototype._calcChainableHelper = function (operandValue) {
        var transformType = this.transformType;
        return transformType === TransformCaseExpression.UPPER_CASE ? String(operandValue).toLocaleUpperCase() : String(operandValue).toLocaleLowerCase();
    };
    TransformCaseExpression.prototype._getJSChainableHelper = function (operandJS) {
        var transformType = this.transformType;
        return Expression.jsNullSafetyUnary(operandJS, function (input) {
            return transformType === TransformCaseExpression.UPPER_CASE ? "String(" + input + ").toLocaleUpperCase()" : "String(" + input + ").toLocaleLowerCase()";
        });
    };
    TransformCaseExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        var transformType = this.transformType;
        return transformType === TransformCaseExpression.UPPER_CASE ? "UPPER(" + operandSQL + ")" : "LOWER(" + operandSQL + ")";
    };
    TransformCaseExpression.prototype.specialSimplify = function () {
        var operand = this.operand;
        if (operand instanceof TransformCaseExpression)
            return this.changeOperand(operand.operand);
        return this;
    };
    TransformCaseExpression.UPPER_CASE = 'upperCase';
    TransformCaseExpression.LOWER_CASE = 'lowerCase';
    TransformCaseExpression.op = "TransformCase";
    return TransformCaseExpression;
}(ChainableExpression));
export { TransformCaseExpression };
Expression.register(TransformCaseExpression);
