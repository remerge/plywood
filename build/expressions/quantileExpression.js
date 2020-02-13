import * as tslib_1 from "tslib";
import { ChainableUnaryExpression, Expression } from './baseExpression';
import { Aggregate } from './mixins/aggregate';
var QuantileExpression = (function (_super) {
    tslib_1.__extends(QuantileExpression, _super);
    function QuantileExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("quantile");
        _this._checkOperandTypes('DATASET');
        _this._checkExpressionTypes('NUMBER');
        _this.value = parameters.value;
        _this.tuning = parameters.tuning;
        _this.type = 'NUMBER';
        return _this;
    }
    QuantileExpression.fromJS = function (parameters) {
        var value = ChainableUnaryExpression.jsToValue(parameters);
        value.value = parameters.value || parameters.quantile;
        value.tuning = parameters.tuning;
        return new QuantileExpression(value);
    };
    QuantileExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.value = this.value;
        value.tuning = this.tuning;
        return value;
    };
    QuantileExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.value = this.value;
        if (this.tuning)
            js.tuning = this.tuning;
        return js;
    };
    QuantileExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.value === other.value &&
            this.tuning === other.tuning;
    };
    QuantileExpression.prototype._toStringParameters = function (indent) {
        var params = [this.expression.toString(indent), String(this.value)];
        if (this.tuning)
            params.push(Expression.safeString(this.tuning));
        return params;
    };
    QuantileExpression.prototype._calcChainableUnaryHelper = function (operandValue, expressionValue) {
        return operandValue ? operandValue.quantile(this.expression, this.value) : null;
    };
    QuantileExpression.op = "Quantile";
    return QuantileExpression;
}(ChainableUnaryExpression));
export { QuantileExpression };
Expression.applyMixins(QuantileExpression, [Aggregate]);
Expression.register(QuantileExpression);
