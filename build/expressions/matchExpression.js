import * as tslib_1 from "tslib";
var REGEXP_SPECIAL = "\\^$.|?*+()[{";
import { Set } from '../datatypes/index';
import { ChainableExpression, Expression } from './baseExpression';
var MatchExpression = (function (_super) {
    tslib_1.__extends(MatchExpression, _super);
    function MatchExpression(parameters) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this._ensureOp("match");
        _this._checkOperandTypes('STRING');
        _this.regexp = parameters.regexp;
        _this.type = 'BOOLEAN';
        return _this;
    }
    MatchExpression.likeToRegExp = function (like, escapeChar) {
        if (escapeChar === void 0) { escapeChar = '\\'; }
        var regExp = ['^'];
        for (var i = 0; i < like.length; i++) {
            var char = like[i];
            if (char === escapeChar) {
                var nextChar = like[i + 1];
                if (!nextChar)
                    throw new Error("invalid LIKE string '" + like + "'");
                char = nextChar;
                i++;
            }
            else if (char === '%') {
                regExp.push('.*');
                continue;
            }
            else if (char === '_') {
                regExp.push('.');
                continue;
            }
            if (REGEXP_SPECIAL.indexOf(char) !== -1) {
                regExp.push('\\');
            }
            regExp.push(char);
        }
        regExp.push('$');
        return regExp.join('');
    };
    MatchExpression.fromJS = function (parameters) {
        var value = ChainableExpression.jsToValue(parameters);
        value.regexp = parameters.regexp;
        return new MatchExpression(value);
    };
    MatchExpression.prototype.valueOf = function () {
        var value = _super.prototype.valueOf.call(this);
        value.regexp = this.regexp;
        return value;
    };
    MatchExpression.prototype.toJS = function () {
        var js = _super.prototype.toJS.call(this);
        js.regexp = this.regexp;
        return js;
    };
    MatchExpression.prototype.equals = function (other) {
        return _super.prototype.equals.call(this, other) &&
            this.regexp === other.regexp;
    };
    MatchExpression.prototype._toStringParameters = function (indent) {
        return [this.regexp];
    };
    MatchExpression.prototype._calcChainableHelper = function (operandValue) {
        var re = new RegExp(this.regexp);
        if (operandValue == null)
            return null;
        return Set.crossUnaryBoolean(operandValue, function (a) { return re.test(a); });
    };
    MatchExpression.prototype._getJSChainableHelper = function (operandJS) {
        return "/" + this.regexp + "/.test(" + operandJS + ")";
    };
    MatchExpression.prototype._getSQLChainableHelper = function (dialect, operandSQL) {
        return dialect.regexpExpression(operandSQL, this.regexp);
    };
    MatchExpression.op = "Match";
    return MatchExpression;
}(ChainableExpression));
export { MatchExpression };
Expression.register(MatchExpression);
