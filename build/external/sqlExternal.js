import * as tslib_1 from "tslib";
import { Expression } from '../expressions/index';
import { External } from './baseExternal';
function getSplitInflaters(split) {
    return split.mapSplits(function (label, splitExpression) {
        var simpleInflater = External.getInteligentInflater(splitExpression, label);
        if (simpleInflater)
            return simpleInflater;
        return undefined;
    });
}
var SQLExternal = (function (_super) {
    tslib_1.__extends(SQLExternal, _super);
    function SQLExternal(parameters, dialect) {
        var _this = _super.call(this, parameters, dummyObject) || this;
        _this.dialect = dialect;
        return _this;
    }
    SQLExternal.prototype.canHandleFilter = function (filter) {
        return true;
    };
    SQLExternal.prototype.canHandleSort = function (sort) {
        return true;
    };
    SQLExternal.prototype.capability = function (cap) {
        if (cap === 'filter-on-attribute' || cap === 'shortcut-group-by')
            return true;
        return _super.prototype.capability.call(this, cap);
    };
    SQLExternal.prototype.sqlToQuery = function (sql) {
        return sql;
    };
    SQLExternal.prototype.getFrom = function () {
        var _a = this, source = _a.source, dialect = _a.dialect;
        var m = String(source).match(/^(\w+)\.(.+)$/);
        if (m) {
            return "FROM " + m[1] + '.' + dialect.escapeName(m[2]);
        }
        else {
            return "FROM " + dialect.escapeName(source);
        }
    };
    SQLExternal.prototype.getQueryAndPostTransform = function () {
        var _a = this, mode = _a.mode, applies = _a.applies, sort = _a.sort, limit = _a.limit, derivedAttributes = _a.derivedAttributes, dialect = _a.dialect;
        var query = ['SELECT'];
        var postTransform = null;
        var inflaters = [];
        var keys = null;
        var zeroTotalApplies = null;
        var from = this.getFrom();
        var filter = this.getQueryFilter();
        if (!filter.equals(Expression.TRUE)) {
            from += '\nWHERE ' + filter.getSQL(dialect);
        }
        var selectedAttributes = this.getSelectedAttributes();
        switch (mode) {
            case 'raw':
                selectedAttributes = selectedAttributes.map(function (a) { return a.dropOriginInfo(); });
                inflaters = selectedAttributes.map(function (attribute) {
                    var name = attribute.name, type = attribute.type;
                    switch (type) {
                        case 'BOOLEAN':
                            return External.booleanInflaterFactory(name);
                        case 'TIME':
                            return External.timeInflaterFactory(name);
                        case 'SET/STRING':
                            return External.setStringInflaterFactory(name);
                        default:
                            return null;
                    }
                }).filter(Boolean);
                query.push(selectedAttributes.map(function (a) {
                    var name = a.name;
                    if (derivedAttributes[name]) {
                        return Expression._.apply(name, derivedAttributes[name]).getSQL(dialect);
                    }
                    else {
                        return dialect.escapeName(name);
                    }
                }).join(', '), from);
                if (sort) {
                    query.push(sort.getSQL(dialect));
                }
                if (limit) {
                    query.push(limit.getSQL(dialect));
                }
                break;
            case 'value':
                query.push(this.toValueApply().getSQL(dialect), from, dialect.constantGroupBy());
                postTransform = External.valuePostTransformFactory();
                break;
            case 'total':
                zeroTotalApplies = applies;
                inflaters = applies.map(function (apply) {
                    var name = apply.name, expression = apply.expression;
                    return External.getSimpleInflater(expression.type, name);
                }).filter(Boolean);
                keys = [];
                query.push(applies.map(function (apply) { return apply.getSQL(dialect); }).join(',\n'), from, dialect.constantGroupBy());
                break;
            case 'split':
                var split = this.getQuerySplit();
                keys = split.mapSplits(function (name) { return name; });
                query.push(split.getSelectSQL(dialect)
                    .concat(applies.map(function (apply) { return apply.getSQL(dialect); }))
                    .join(',\n'), from, 'GROUP BY ' + (this.capability('shortcut-group-by') ? split.getShortGroupBySQL() : split.getGroupBySQL(dialect)).join(','));
                if (!(this.havingFilter.equals(Expression.TRUE))) {
                    query.push('HAVING ' + this.havingFilter.getSQL(dialect));
                }
                if (sort) {
                    query.push(sort.getSQL(dialect));
                }
                if (limit) {
                    query.push(limit.getSQL(dialect));
                }
                inflaters = getSplitInflaters(split);
                break;
            default:
                throw new Error("can not get query for mode: " + mode);
        }
        return {
            query: this.sqlToQuery(query.join('\n')),
            postTransform: postTransform || External.postTransformFactory(inflaters, selectedAttributes, keys, zeroTotalApplies)
        };
    };
    SQLExternal.type = 'DATASET';
    return SQLExternal;
}(External));
export { SQLExternal };
