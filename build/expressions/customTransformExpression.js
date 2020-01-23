import * as tslib_1 from "tslib";
import { ChainableExpression, Expression } from './baseExpression';
var CustomTransformExpression = (function (_super) {
    tslib_1.__extends(CustomTransformExpression, _super);
    function CustomTransformExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("customTransform");
        _this.custom = parameters.custom;
        if (parameters.outputType)
            _this.outputType = parameters.outputType;
        _this.type = _this.outputType || _this.operand.type;
        return _this;
    }
    CustomTransformExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.custom = parameters.custom;
        if (parameters.outputType)
            value.outputType = parameters.outputType;
        return new CustomTransformExpression(value);
    };
    CustomTransformExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.custom = this.custom;
        if (this.outputType)
            value.outputType = this.outputType;
        return value;
    };
    CustomTransformExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.custom = this.custom;
        if (this.outputType)
            js.outputType = this.outputType;
        return js;
    };
    CustomTransformExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.custom === other.custom &&
            this.outputType === other.outputType;
    };
    CustomTransformExpression.prototype._toStringParameters = function (indent) {
        var param = [this.custom];
        if (this.outputType)
            param.push(this.outputType);
        return param;
    };
    CustomTransformExpression.prototype._calcChainableHelper = function (operandValue) {
        throw new Error('can not calc on custom transform action');
    };
    CustomTransformExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        throw new Error("Custom transform not supported in SQL");
    };
    CustomTransformExpression.prototype._getJSChainableHelper = function (operandJS) {
        throw new Error("Custom transform can't yet be expressed as JS");
    };
    CustomTransformExpression.op = "CustomTransform";
    return CustomTransformExpression;
}(ChainableExpression));
export { CustomTransformExpression };
Expression.register(CustomTransformExpression);
