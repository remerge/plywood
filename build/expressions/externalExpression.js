import * as tslib_1 from "tslib";
import { External } from '../external/baseExternal';
import { Expression } from './baseExpression';
var ExternalExpression = (function (_super) {
    tslib_1.__extends(ExternalExpression, _super);
    function ExternalExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        var external = parameters.external;
        if (!external)
            throw new Error('must have an external');
        _this.external = external;
        _this._ensureOp('external');
        _this.type = external.mode === 'value' ? external.getValueType() : 'DATASET';
        _this.simple = true;
        return _this;
    }
    ExternalExpression.fromJS = function (parameters) {
        var value = {
            op: parameters.op
        };
        value.external = External.fromJS(parameters.external);
        return new ExternalExpression(value);
    };
    ExternalExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.external = this.external;
        return value;
    };
    ExternalExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.external = this.external.toJS();
        return js;
    };
    ExternalExpression.prototype.toString = function () {
        return "E:" + this.external;
    };
    ExternalExpression.prototype.getFn = function () {
        throw new Error('should not call getFn on External');
    };
    ExternalExpression.prototype.calc = function (datum) {
        throw new Error('should not call calc on External');
    };
    ExternalExpression.prototype.getJS = function (datumVar) {
        throw new Error('should not call getJS on External');
    };
    ExternalExpression.prototype.getSQL = function (dialect) {
        throw new Error('should not call getSQL on External');
    };
    ExternalExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.external.equals(other.external);
    };
    ExternalExpression.prototype.updateTypeContext = function (typeContext) {
        var external = this.external;
        if (external.mode !== 'value') {
            var newTypeContext = this.external.getFullType();
            newTypeContext.parent = typeContext;
            return newTypeContext;
        }
        return typeContext;
    };
    ExternalExpression.prototype.unsuppress = function () {
        var value = this.valueOf();
        value.external = this.external.show();
        return new ExternalExpression(value);
    };
    ExternalExpression.prototype.addExpression = function (expression) {
        var newExternal = this.external.addExpression(expression);
        if (!newExternal)
            return null;
        return new ExternalExpression({ external: newExternal });
    };
    ExternalExpression.prototype.prePush = function (expression) {
        var newExternal = this.external.prePush(expression);
        if (!newExternal)
            return null;
        return new ExternalExpression({ external: newExternal });
    };
    ExternalExpression.prototype.maxPossibleSplitValues = function () {
        return Infinity;
    };
    ExternalExpression.op = "external";
    return ExternalExpression;
}(Expression));
export { ExternalExpression };
Expression.register(ExternalExpression);
