import { TimeRange } from '../../datatypes/index';
import { $, CastExpression, ChainableUnaryExpression, ConcatExpression, CustomTransformExpression, ExtractExpression, FallbackExpression, LengthExpression, LiteralExpression, LookupExpression, NumberBucketExpression, OverlapExpression, r, RefExpression, SubstrExpression, TimeBucketExpression, TimeFloorExpression, TimePartExpression, TransformCaseExpression } from '../../expressions';
import { External } from '../baseExternal';
var DruidExtractionFnBuilder = (function () {
    function DruidExtractionFnBuilder(options, allowJavaScript) {
        this.version = options.version;
        this.customTransforms = options.customTransforms;
        this.allowJavaScript = allowJavaScript;
    }
    DruidExtractionFnBuilder.composeFns = function (f, g) {
        if (!f || !g)
            return f || g;
        return {
            type: 'cascade',
            extractionFns: [].concat((f.type === 'cascade' ? f.extractionFns : f), (g.type === 'cascade' ? g.extractionFns : g))
        };
    };
    DruidExtractionFnBuilder.getLastFn = function (fn) {
        if (fn && fn.type === 'cascade') {
            var extractionFns = fn.extractionFns;
            return extractionFns[extractionFns.length - 1];
        }
        else {
            return fn;
        }
    };
    DruidExtractionFnBuilder.wrapFunctionTryCatch = function (lines) {
        return 'function(s){try{\n' + lines.filter(Boolean).join('\n') + '\n}catch(e){return null;}}';
    };
    DruidExtractionFnBuilder.prototype.expressionToExtractionFn = function (expression) {
        var extractionFn = this.expressionToExtractionFnPure(expression);
        if (extractionFn && extractionFn.type === 'cascade') {
            if (extractionFn.extractionFns.every(function (extractionFn) { return extractionFn.type === 'javascript'; })) {
                return this.expressionToJavaScriptExtractionFn(expression);
            }
        }
        return extractionFn;
    };
    DruidExtractionFnBuilder.prototype.expressionToExtractionFnPure = function (expression) {
        var freeReferences = expression.getFreeReferences();
        if (freeReferences.length > 1) {
            throw new Error("must have at most 1 reference (has " + freeReferences.length + "): " + expression);
        }
        if (expression instanceof LiteralExpression) {
            return this.literalToExtractionFn(expression);
        }
        else if (expression instanceof RefExpression) {
            return this.refToExtractionFn(expression);
        }
        else if (expression instanceof ConcatExpression) {
            return this.concatToExtractionFn(expression);
        }
        else if (expression instanceof CustomTransformExpression) {
            return this.customTransformToExtractionFn(expression);
        }
        else if (expression instanceof NumberBucketExpression) {
            return this.numberBucketToExtractionFn(expression);
        }
        else if (expression instanceof SubstrExpression) {
            return this.substrToExtractionFn(expression);
        }
        else if (expression instanceof TimeBucketExpression || expression instanceof TimeFloorExpression) {
            return this.timeFloorToExtractionFn(expression);
        }
        else if (expression instanceof TimePartExpression) {
            return this.timePartToExtractionFn(expression);
        }
        else if (expression instanceof TransformCaseExpression) {
            return this.transformCaseToExtractionFn(expression);
        }
        else if (expression instanceof LengthExpression) {
            return this.lengthToExtractionFn(expression);
        }
        else if (expression instanceof ExtractExpression) {
            return this.extractToExtractionFn(expression);
        }
        else if (expression instanceof LookupExpression) {
            return this.lookupToExtractionFn(expression);
        }
        else if (expression instanceof FallbackExpression) {
            return this.fallbackToExtractionFn(expression);
        }
        else if (expression instanceof CastExpression) {
            return this.castToExtractionFn(expression);
        }
        else if (expression instanceof OverlapExpression) {
            return this.overlapToExtractionFn(expression);
        }
        else {
            return this.expressionToJavaScriptExtractionFn(expression);
        }
    };
    DruidExtractionFnBuilder.prototype.literalToExtractionFn = function (expression) {
        return {
            type: "lookup",
            retainMissingValue: false,
            replaceMissingValueWith: expression.getLiteralValue(),
            lookup: {
                type: "map",
                map: {}
            }
        };
    };
    DruidExtractionFnBuilder.prototype.refToExtractionFn = function (expression) {
        if (expression.type === 'BOOLEAN') {
            return {
                type: "lookup",
                lookup: {
                    type: "map",
                    map: {
                        "0": "false",
                        "1": "true",
                        "false": "false",
                        "true": "true"
                    }
                }
            };
        }
        else {
            return null;
        }
    };
    DruidExtractionFnBuilder.prototype.concatToExtractionFn = function (expression) {
        var innerExpression = null;
        var format = expression.getExpressionList().map(function (ex) {
            if (ex instanceof LiteralExpression) {
                return ex.value.replace(/%/g, '\\%');
            }
            if (innerExpression) {
                throw new Error("can not have multiple expressions in concat '" + expression + "'");
            }
            innerExpression = ex;
            return '%s';
        }).join('');
        if (!innerExpression)
            throw new Error("invalid concat expression '" + expression + "'");
        return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(innerExpression), {
            type: 'stringFormat',
            format: format,
            nullHandling: 'returnNull'
        });
    };
    DruidExtractionFnBuilder.prototype.timeFloorToExtractionFn = function (expression) {
        var operand = expression.operand, duration = expression.duration;
        var timezone = expression.getTimezone();
        var myExtractionFn = {
            type: "timeFormat",
            granularity: {
                type: "period",
                period: duration.toString(),
                timeZone: timezone.toString()
            },
            format: "yyyy-MM-dd'T'HH:mm:ss'Z",
            timeZone: 'Etc/UTC'
        };
        return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), myExtractionFn);
    };
    DruidExtractionFnBuilder.prototype.timePartToExtractionFn = function (expression) {
        var operand = expression.operand, part = expression.part;
        var timezone = expression.getTimezone();
        var myExtractionFn;
        var format = DruidExtractionFnBuilder.TIME_PART_TO_FORMAT[part];
        if (format) {
            myExtractionFn = {
                type: "timeFormat",
                format: format,
                locale: "en-US",
                timeZone: timezone.toString()
            };
        }
        else {
            var expr = DruidExtractionFnBuilder.TIME_PART_TO_EXPR[part];
            if (!expr)
                throw new Error("can not part on " + part);
            myExtractionFn = {
                type: 'javascript',
                'function': DruidExtractionFnBuilder.wrapFunctionTryCatch([
                    'var d = new org.joda.time.DateTime(s);',
                    timezone.isUTC() ? null : "d = d.withZone(org.joda.time.DateTimeZone.forID(" + JSON.stringify(timezone) + "));",
                    "d = " + expr + ";",
                    'return d;'
                ])
            };
        }
        return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), myExtractionFn);
    };
    DruidExtractionFnBuilder.prototype.numberBucketToExtractionFn = function (expression) {
        var operand = expression.operand, size = expression.size, offset = expression.offset;
        var bucketExtractionFn = { type: "bucket" };
        if (size !== 1)
            bucketExtractionFn.size = size;
        if (offset !== 0)
            bucketExtractionFn.offset = offset;
        return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), bucketExtractionFn);
    };
    DruidExtractionFnBuilder.prototype.substrToExtractionFn = function (expression) {
        var operand = expression.operand, position = expression.position, len = expression.len;
        return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), {
            type: "substring",
            index: position,
            length: len
        });
    };
    DruidExtractionFnBuilder.prototype.transformCaseToExtractionFn = function (expression) {
        var operand = expression.operand, transformType = expression.transformType;
        var type = DruidExtractionFnBuilder.CASE_TO_DRUID[transformType];
        if (!type)
            throw new Error("unsupported case transformation '" + type + "'");
        return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), {
            type: type
        });
    };
    DruidExtractionFnBuilder.prototype.lengthToExtractionFn = function (expression) {
        var operand = expression.operand;
        return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), {
            type: 'strlen'
        });
    };
    DruidExtractionFnBuilder.prototype.extractToExtractionFn = function (expression) {
        var operand = expression.operand, regexp = expression.regexp;
        return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), {
            type: "regex",
            expr: regexp,
            replaceMissingValue: true
        });
    };
    DruidExtractionFnBuilder.prototype.lookupToExtractionFn = function (expression) {
        var operand = expression.operand, lookupFn = expression.lookupFn;
        var lookupExtractionFn = {
            type: "registeredLookup",
            lookup: lookupFn
        };
        return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), lookupExtractionFn);
    };
    DruidExtractionFnBuilder.prototype.fallbackToExtractionFn = function (expression) {
        var operand = expression.operand, fallback = expression.expression;
        if (operand instanceof ExtractExpression) {
            var extractExtractionFn = this.extractToExtractionFn(operand);
            var extractExtractionFnLast = DruidExtractionFnBuilder.getLastFn(extractExtractionFn);
            if (fallback.isOp("ref")) {
                delete extractExtractionFnLast.replaceMissingValue;
            }
            else if (fallback.isOp("literal")) {
                extractExtractionFnLast.replaceMissingValueWith = fallback.getLiteralValue();
            }
            else {
                throw new Error("unsupported fallback: " + expression);
            }
            return extractExtractionFn;
        }
        else if (operand instanceof LookupExpression) {
            var lookupExtractionFn = this.lookupToExtractionFn(operand);
            var lookupExtractionFnLast = DruidExtractionFnBuilder.getLastFn(lookupExtractionFn);
            if (fallback.isOp("ref")) {
                lookupExtractionFnLast.retainMissingValue = true;
            }
            else if (fallback.isOp("literal")) {
                lookupExtractionFnLast.replaceMissingValueWith = fallback.getLiteralValue();
            }
            else {
                throw new Error("unsupported fallback: " + expression);
            }
            return lookupExtractionFn;
        }
        if (fallback instanceof LiteralExpression) {
            return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), {
                type: "lookup",
                retainMissingValue: true,
                lookup: {
                    type: "map",
                    map: {
                        "": fallback.value
                    }
                }
            });
        }
        return this.expressionToJavaScriptExtractionFn(expression);
    };
    DruidExtractionFnBuilder.prototype.customTransformToExtractionFn = function (customTransform) {
        var operand = customTransform.operand, custom = customTransform.custom;
        var customExtractionFn = this.customTransforms[custom];
        if (!customExtractionFn)
            throw new Error("could not find extraction function: '" + custom + "'");
        var extractionFn = customExtractionFn.extractionFn;
        if (typeof extractionFn.type !== 'string')
            throw new Error("must have type in custom extraction fn '" + custom + "'");
        try {
            JSON.parse(JSON.stringify(customExtractionFn));
        }
        catch (e) {
            throw new Error("must have JSON extraction Fn '" + custom + "'");
        }
        return DruidExtractionFnBuilder.composeFns(this.expressionToExtractionFnPure(operand), extractionFn);
    };
    DruidExtractionFnBuilder.prototype.castToExtractionFn = function (cast) {
        if (cast.outputType === 'TIME') {
            return this.expressionToJavaScriptExtractionFn(cast);
        }
        return this.expressionToExtractionFnPure(cast.operand);
    };
    DruidExtractionFnBuilder.prototype.overlapToExtractionFn = function (expression) {
        var freeReferences = expression.operand.getFreeReferences();
        var rhsType = expression.expression.type;
        if (freeReferences[0] === '__time' &&
            expression.expression instanceof LiteralExpression &&
            (rhsType === 'TIME_RANGE' || rhsType === 'SET/TIME_RANGE')) {
            expression = expression.operand.cast('NUMBER').overlap(r(expression.expression.getLiteralValue().changeToNumber()));
        }
        return this.expressionToJavaScriptExtractionFn(expression);
    };
    DruidExtractionFnBuilder.prototype.expressionToJavaScriptExtractionFn = function (ex) {
        if (!this.allowJavaScript)
            throw new Error('avoiding javascript');
        var prefixFn = null;
        var jsExtractionFn = {
            type: "javascript",
            'function': null
        };
        if (ex.getFreeReferences()[0] === '__time') {
            ex = ex.substitute(function (e) {
                if (e instanceof LiteralExpression) {
                    if (e.value instanceof TimeRange) {
                        return r(e.value.changeToNumber());
                    }
                    else {
                        return null;
                    }
                }
                else if (e instanceof RefExpression) {
                    return $('__time');
                }
                else {
                    return null;
                }
            });
        }
        try {
            jsExtractionFn['function'] = ex.getJSFn('d');
        }
        catch (e) {
            if (ex instanceof ChainableUnaryExpression) {
                prefixFn = this.expressionToExtractionFnPure(ex.operand);
                jsExtractionFn['function'] = ex.getAction().getJSFn('d');
            }
            else {
                throw e;
            }
        }
        if (ex.isOp('concat'))
            jsExtractionFn.injective = true;
        return DruidExtractionFnBuilder.composeFns(prefixFn, jsExtractionFn);
    };
    DruidExtractionFnBuilder.prototype.versionBefore = function (neededVersion) {
        var version = this.version;
        return version && External.versionLessThan(version, neededVersion);
    };
    DruidExtractionFnBuilder.CASE_TO_DRUID = {
        upperCase: 'upper',
        lowerCase: 'lower'
    };
    DruidExtractionFnBuilder.TIME_PART_TO_FORMAT = {
        SECOND_OF_MINUTE: "s",
        MINUTE_OF_HOUR: "m",
        HOUR_OF_DAY: "H",
        DAY_OF_WEEK: "e",
        DAY_OF_MONTH: "d",
        DAY_OF_YEAR: "D",
        WEEK_OF_YEAR: "w",
        MONTH_OF_YEAR: "M",
        YEAR: "Y"
    };
    DruidExtractionFnBuilder.TIME_PART_TO_EXPR = {
        SECOND_OF_MINUTE: "d.getSecondOfMinute()",
        SECOND_OF_HOUR: "d.getSecondOfHour()",
        SECOND_OF_DAY: "d.getSecondOfDay()",
        SECOND_OF_WEEK: "d.getDayOfWeek()*86400 + d.getSecondOfMinute()",
        SECOND_OF_MONTH: "d.getDayOfMonth()*86400 + d.getSecondOfHour()",
        SECOND_OF_YEAR: "d.getDayOfYear()*86400 + d.getSecondOfDay()",
        MINUTE_OF_HOUR: "d.getMinuteOfHour()",
        MINUTE_OF_DAY: "d.getMinuteOfDay()",
        MINUTE_OF_WEEK: "d.getDayOfWeek()*1440 + d.getMinuteOfDay()",
        MINUTE_OF_MONTH: "d.getDayOfMonth()*1440 + d.getMinuteOfDay()",
        MINUTE_OF_YEAR: "d.getDayOfYear()*1440 + d.getMinuteOfDay()",
        HOUR_OF_DAY: "d.getHourOfDay()",
        HOUR_OF_WEEK: "d.getDayOfWeek()*24 + d.getHourOfDay()",
        HOUR_OF_MONTH: "d.getDayOfMonth()*24 + d.getHourOfDay()",
        HOUR_OF_YEAR: "d.getDayOfYear()*24 + d.getHourOfDay()",
        DAY_OF_WEEK: "d.getDayOfWeek()",
        DAY_OF_MONTH: "d.getDayOfMonth()",
        DAY_OF_YEAR: "d.getDayOfYear()",
        WEEK_OF_YEAR: "d.getWeekOfWeekyear()",
        MONTH_OF_YEAR: "d.getMonthOfYear()",
        YEAR: "d.getYearOfEra()",
        QUARTER: "Math.ceil((d.getMonthOfYear()) / 3)"
    };
    return DruidExtractionFnBuilder;
}());
export { DruidExtractionFnBuilder };
