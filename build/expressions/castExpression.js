import * as tslib_1 from "tslib";
import { ChainableExpression, Expression } from './baseExpression';
var CAST_TYPE_TO_FN = {
    TIME: {
        NUMBER: function (n) { return new Date(n); }
    },
    NUMBER: {
        TIME: function (n) { return Date.parse(n.toString()); },
        _: function (s) { return Number(s); }
    },
    STRING: {
        _: function (v) { return '' + v; }
    }
};
var CAST_TYPE_TO_JS = {
    TIME: {
        NUMBER: function (operandJS) { return "new Date(" + operandJS + ")"; }
    },
    NUMBER: {
        _: function (s) { return "(+(" + s + "))"; }
    },
    STRING: {
        _: function (operandJS) { return "(''+" + operandJS + ")"; }
    }
};
var CastExpression = (function (_super) {
    tslib_1.__extends(CastExpression, _super);
    function CastExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.outputType = parameters.outputType;
        _this._ensureOp("cast");
        if (typeof _this.outputType !== 'string') {
            throw new Error("`outputType` must be a string");
        }
        _this.type = _this.outputType;
        return _this;
    }
    CastExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.outputType = parameters.outputType || parameters.castType;
        return new CastExpression(value);
    };
    CastExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.outputType = this.outputType;
        return value;
    };
    CastExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.outputType = this.outputType;
        return js;
    };
    CastExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.outputType === other.outputType;
    };
    CastExpression.prototype._toStringParameters = function (indent) {
        return [this.outputType];
    };
    CastExpression.prototype._calcChainableHelper = function (operandValue) {
        var outputType = this.outputType;
        var inputType = this.operand.type;
        if (outputType === inputType)
            return operandValue;
        var caster = CAST_TYPE_TO_FN[outputType];
        if (!caster)
            throw new Error("unsupported cast type in calc '" + outputType + "'");
        var castFn = caster[inputType] || caster['_'];
        if (!castFn)
            throw new Error("unsupported cast from " + inputType + " to '" + outputType + "'");
        return operandValue ? castFn(operandValue) : null;
    };
    CastExpression.prototype._getJSChainableHelper = function (operandJS) {
        var outputType = this.outputType;
        var inputType = this.operand.type;
        if (outputType === inputType)
            return operandJS;
        var castJS = CAST_TYPE_TO_JS[outputType];
        if (!castJS)
            throw new Error("unsupported cast type in getJS '" + outputType + "'");
        var js = castJS[inputType] || castJS['_'];
        if (!js)
            throw new Error("unsupported combo in getJS of cast action: " + inputType + " to " + outputType);
        return js(operandJS);
    };
    CastExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.castExpression(this.operand.type, operandSQL, this.outputType);
    };
    CastExpression.prototype.specialSimplify = function () {
        var _a = this, operand = _a.operand, outputType = _a.outputType;
        if (operand.type === outputType)
            return operand;
        return this;
    };
    CastExpression.op = "Cast";
    return CastExpression;
}(ChainableExpression));
export { CastExpression };
Expression.register(CastExpression);
