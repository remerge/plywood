import { NamedArray } from 'immutable-class';
import { Set } from '../../datatypes';
import { CastExpression, ChainableExpression, ChainableUnaryExpression, ConcatExpression, IsExpression, ExtractExpression, FallbackExpression, LengthExpression, LiteralExpression, LookupExpression, NumberBucketExpression, OverlapExpression, ContainsExpression, RefExpression, SubstrExpression, TimeBucketExpression, TimeFloorExpression, TimePartExpression, TransformCaseExpression, MultiplyExpression, MatchExpression, AddExpression, SubtractExpression, DivideExpression, TimeShiftExpression, PowerExpression, LogExpression, AbsoluteExpression, AndExpression, OrExpression, NotExpression, ThenExpression, IndexOfExpression } from '../../expressions';
import { continuousFloorExpression } from '../../helper';
import { External } from '../baseExternal';
var DruidExpressionBuilder = (function () {
    function DruidExpressionBuilder(options) {
        this.version = options.version;
        this.rawAttributes = options.rawAttributes;
        this.timeAttribute = options.timeAttribute;
    }
    DruidExpressionBuilder.escape = function (str) {
        return str.replace(DruidExpressionBuilder.UNSAFE_CHAR, function (s) {
            return '\\u' + ('000' + s.charCodeAt(0).toString(16)).substr(-4);
        });
    };
    DruidExpressionBuilder.escapeVariable = function (name) {
        return "\"" + DruidExpressionBuilder.escape(name) + "\"";
    };
    DruidExpressionBuilder.escapeLiteral = function (x) {
        if (x == null)
            return 'null';
        if (x.toISOString) {
            return String(x.valueOf());
        }
        else if (typeof x === 'number') {
            return String(x);
        }
        else {
            return "'" + DruidExpressionBuilder.escape(String(x)) + "'";
        }
    };
    DruidExpressionBuilder.escapeLike = function (str) {
        return str.replace(/([%_~])/g, '~$1');
    };
    DruidExpressionBuilder.expressionTypeToOutputType = function (type) {
        switch (type) {
            case 'TIME':
            case 'TIME_RANGE':
                return 'LONG';
            case 'NUMBER':
            case 'NUMBER_RANGE':
                return 'FLOAT';
            default:
                return 'STRING';
        }
    };
    DruidExpressionBuilder.prototype.expressionToDruidExpression = function (expression) {
        var _this = this;
        if (expression instanceof LiteralExpression) {
            var literalValue = expression.getLiteralValue();
            if (literalValue === null) {
                return "null";
            }
            else {
                switch (typeof literalValue) {
                    case 'string':
                        return DruidExpressionBuilder.escapeLiteral(literalValue);
                    case 'number':
                        return String(literalValue);
                    case 'boolean':
                        return String(Number(literalValue));
                    default:
                        return "no_such_type";
                }
            }
        }
        else if (expression instanceof RefExpression) {
            if (expression.name === this.timeAttribute) {
                return '__time';
            }
            else {
                var exStr = DruidExpressionBuilder.escapeVariable(expression.name);
                var info = this.getAttributesInfo(expression.name);
                if (info) {
                    if (info.nativeType === 'STRING') {
                        if (info.type === 'TIME') {
                            exStr = this.castToType(exStr, info.nativeType, info.type);
                        }
                    }
                }
                return exStr;
            }
        }
        else if (expression instanceof ChainableExpression) {
            var myOperand = expression.operand;
            var ex1_1 = this.expressionToDruidExpression(myOperand);
            if (expression instanceof CastExpression) {
                return this.castToType(ex1_1, expression.operand.type, expression.outputType);
            }
            else if (expression instanceof SubstrExpression) {
                this.checkDruid11('substring');
                return "substring(" + ex1_1 + "," + expression.position + "," + expression.len + ")";
            }
            else if (expression instanceof ExtractExpression) {
                this.checkDruid11('regexp_extract');
                return "regexp_extract(" + ex1_1 + "," + DruidExpressionBuilder.escapeLiteral(expression.regexp) + ",1)";
            }
            else if (expression instanceof MatchExpression) {
                this.checkDruid11('regexp_extract');
                return "(regexp_extract(" + ex1_1 + "," + DruidExpressionBuilder.escapeLiteral(expression.regexp) + ")!='')";
            }
            else if (expression instanceof ContainsExpression) {
                var needle = expression.expression;
                if (needle instanceof LiteralExpression) {
                    var needleValue = DruidExpressionBuilder.escape(DruidExpressionBuilder.escapeLike(needle.value));
                    if (expression.compare === ContainsExpression.IGNORE_CASE) {
                        this.checkDruid11('lower');
                        return "like(lower(" + ex1_1 + "),'%" + needleValue.toLowerCase() + "%','~')";
                    }
                    else {
                        return "like(" + ex1_1 + ",'%" + needleValue + "%','~')";
                    }
                }
                else {
                    throw new Error("can not plan " + expression + " into Druid");
                }
            }
            else if (expression instanceof LengthExpression) {
                this.checkDruid11('strlen');
                return "strlen(" + ex1_1 + ")";
            }
            else if (expression instanceof NotExpression) {
                return "!" + ex1_1;
            }
            else if (expression instanceof AbsoluteExpression) {
                return "abs(" + ex1_1 + ")";
            }
            else if (expression instanceof NumberBucketExpression) {
                return continuousFloorExpression(ex1_1, 'floor', expression.size, expression.offset);
            }
            else if (expression instanceof TimePartExpression) {
                this.checkDruid11('timestamp_extract');
                var format = DruidExpressionBuilder.TIME_PART_TO_FORMAT[expression.part];
                if (!format)
                    throw new Error("can not convert " + expression.part + " to Druid expression format");
                return "timestamp_extract(" + ex1_1 + ",'" + format + "'," + DruidExpressionBuilder.escapeLiteral(expression.timezone.toString()) + ")";
            }
            else if (expression instanceof TimeFloorExpression || expression instanceof TimeBucketExpression) {
                this.checkDruid11('timestamp_floor');
                return "timestamp_floor(" + ex1_1 + ",'" + expression.duration + "',''," + DruidExpressionBuilder.escapeLiteral(expression.timezone.toString()) + ")";
            }
            else if (expression instanceof TimeShiftExpression) {
                this.checkDruid11('timestamp_shift');
                return "timestamp_shift(" + ex1_1 + ",'" + expression.duration + "'," + expression.step + "," + DruidExpressionBuilder.escapeLiteral(expression.timezone.toString()) + ")";
            }
            else if (expression instanceof LookupExpression) {
                this.checkDruid11('timestamp_lookup');
                return "lookup(" + ex1_1 + "," + DruidExpressionBuilder.escapeLiteral(expression.lookupFn) + ")";
            }
            else if (expression instanceof TransformCaseExpression) {
                if (expression.transformType === TransformCaseExpression.UPPER_CASE) {
                    this.checkDruid11('upper');
                    return "upper(" + ex1_1 + ")";
                }
                else {
                    this.checkDruid11('lower');
                    return "lower(" + ex1_1 + ")";
                }
            }
            else if (expression instanceof ChainableUnaryExpression) {
                var myExpression = expression.expression;
                if (expression instanceof ConcatExpression) {
                    this.checkDruid11('concat');
                    return 'concat(' + expression.getExpressionList().map(function (ex) { return _this.expressionToDruidExpression(ex); }).join(',') + ')';
                }
                var ex2 = this.expressionToDruidExpression(myExpression);
                if (expression instanceof AddExpression) {
                    return "(" + ex1_1 + "+" + ex2 + ")";
                }
                else if (expression instanceof SubtractExpression) {
                    return "(" + ex1_1 + "-" + ex2 + ")";
                }
                else if (expression instanceof MultiplyExpression) {
                    return "(" + ex1_1 + "*" + ex2 + ")";
                }
                else if (expression instanceof DivideExpression) {
                    if (myExpression instanceof LiteralExpression) {
                        return "(cast(" + ex1_1 + ",'DOUBLE')/" + ex2 + ")";
                    }
                    else {
                        var nullValue = 'null';
                        if (this.versionBefore('0.13.0')) {
                            nullValue = '0';
                        }
                        return "if(" + ex2 + "!=0,(cast(" + ex1_1 + ",'DOUBLE')/" + ex2 + ")," + nullValue + ")";
                    }
                }
                else if (expression instanceof PowerExpression) {
                    return "pow(" + ex1_1 + "," + ex2 + ")";
                }
                else if (expression instanceof LogExpression) {
                    var myLiteral = myExpression.getLiteralValue();
                    if (myLiteral === Math.E)
                        return "log(" + ex1_1 + ")";
                    if (myLiteral === 10)
                        return "log10(" + ex1_1 + ")";
                    return "log(" + ex1_1 + ")/log(" + ex2 + ")";
                }
                else if (expression instanceof ThenExpression) {
                    return "if(" + ex1_1 + "," + ex2 + ",'')";
                }
                else if (expression instanceof FallbackExpression) {
                    return "nvl(" + ex1_1 + "," + ex2 + ")";
                }
                else if (expression instanceof AndExpression) {
                    return "(" + ex1_1 + "&&" + ex2 + ")";
                }
                else if (expression instanceof OrExpression) {
                    return "(" + ex1_1 + "||" + ex2 + ")";
                }
                else if (expression instanceof IsExpression) {
                    var myLiteral = myExpression.getLiteralValue();
                    if (myLiteral instanceof Set) {
                        return '(' + myLiteral.elements.map(function (e) {
                            return ex1_1 + "==" + DruidExpressionBuilder.escapeLiteral(e);
                        }).join('||') + ')';
                    }
                    else {
                        return "(" + ex1_1 + "==" + ex2 + ")";
                    }
                }
                else if (expression instanceof OverlapExpression) {
                    var myExpressionType = myExpression.type;
                    switch (myExpressionType) {
                        case 'NUMBER_RANGE':
                        case 'TIME_RANGE':
                            if (myExpression instanceof LiteralExpression) {
                                var range = myExpression.value;
                                return this.overlapExpression(ex1_1, DruidExpressionBuilder.escapeLiteral(range.start), DruidExpressionBuilder.escapeLiteral(range.end), range.bounds);
                            }
                            throw new Error("can not convert " + expression + " to Druid expression");
                        case 'STRING_RANGE':
                            if (myExpression instanceof LiteralExpression) {
                                var stringRange = myExpression.value;
                                return this.overlapExpression(ex1_1, DruidExpressionBuilder.escapeLiteral(stringRange.start), DruidExpressionBuilder.escapeLiteral(stringRange.end), stringRange.bounds);
                            }
                            throw new Error("can not convert " + expression + " to Druid expression");
                        case 'SET/NUMBER_RANGE':
                        case 'SET/TIME_RANGE':
                            if (myExpression instanceof LiteralExpression) {
                                var setOfRange = myExpression.value;
                                return setOfRange.elements.map(function (range) {
                                    return _this.overlapExpression(ex1_1, DruidExpressionBuilder.escapeLiteral(range.start), DruidExpressionBuilder.escapeLiteral(range.end), range.bounds);
                                }).join('||');
                            }
                            throw new Error("can not convert " + expression + " to Druid expression");
                        default:
                            throw new Error("can not convert " + expression + " to Druid expression");
                    }
                }
                else if (expression instanceof IndexOfExpression) {
                    this.checkDruid12('strpos');
                    return "strpos(" + ex1_1 + "," + ex2 + ")";
                }
            }
        }
        throw new Error("can not convert " + expression + " to Druid expression");
    };
    DruidExpressionBuilder.prototype.castToType = function (operand, sourceType, destType) {
        switch (destType) {
            case 'TIME':
                if (sourceType === 'NUMBER') {
                    return "cast(" + operand + ",'LONG')";
                }
                else {
                    return "timestamp(" + operand + ")";
                }
            case 'STRING':
                return "cast(" + operand + ",'STRING')";
            case 'NUMBER':
                return "cast(" + operand + ",'DOUBLE')";
            default:
                throw new Error("cast to " + destType + " not implemented yet");
        }
    };
    DruidExpressionBuilder.prototype.overlapExpression = function (operand, start, end, bounds) {
        if (start === end && bounds === '[]')
            return "(" + operand + "==" + start + ")";
        var startExpression = null;
        if (start !== 'null') {
            startExpression = start + (bounds[0] === '[' ? '<=' : '<') + operand;
        }
        var endExpression = null;
        if (end !== 'null') {
            endExpression = operand + (bounds[1] === ']' ? '<=' : '<') + end;
        }
        if (startExpression) {
            return endExpression ? "(" + startExpression + " && " + endExpression + ")" : startExpression;
        }
        else {
            return endExpression ? endExpression : 'true';
        }
    };
    DruidExpressionBuilder.prototype.checkDruid12 = function (expr) {
        if (this.versionBefore('0.12.0')) {
            throw new Error("expression '" + expr + "' requires Druid 0.12.0 or newer");
        }
    };
    DruidExpressionBuilder.prototype.checkDruid11 = function (expr) {
        if (this.versionBefore('0.11.0')) {
            throw new Error("expression '" + expr + "' requires Druid 0.11.0 or newer");
        }
    };
    DruidExpressionBuilder.prototype.getAttributesInfo = function (attributeName) {
        return NamedArray.get(this.rawAttributes, attributeName);
    };
    DruidExpressionBuilder.prototype.versionBefore = function (neededVersion) {
        var version = this.version;
        return version && External.versionLessThan(version, neededVersion);
    };
    DruidExpressionBuilder.TIME_PART_TO_FORMAT = {
        SECOND_OF_MINUTE: "SECOND",
        MINUTE_OF_HOUR: "MINUTE",
        HOUR_OF_DAY: "HOUR",
        DAY_OF_WEEK: "DOW",
        DAY_OF_MONTH: "DAY",
        DAY_OF_YEAR: "DOY",
        WEEK_OF_YEAR: "WEEK",
        MONTH_OF_YEAR: "MONTH",
        YEAR: "YEAR"
    };
    DruidExpressionBuilder.UNSAFE_CHAR = /[^a-z0-9 ,._\-;:(){}\[\]<>!@#$%^&*`~?]/ig;
    return DruidExpressionBuilder;
}());
export { DruidExpressionBuilder };
