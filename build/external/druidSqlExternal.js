import * as tslib_1 from "tslib";
import * as toArray from 'stream-to-array';
import { AttributeInfo } from '../datatypes';
import { DruidDialect } from '../dialect';
import { External } from './baseExternal';
import { SQLExternal } from './sqlExternal';
var DruidSQLExternal = (function (_super) {
    tslib_1.__extends(DruidSQLExternal, _super);
    function DruidSQLExternal(parameters) {
        var _this = _super.call(this, parameters, new DruidDialect()) || this;
        _this._ensureEngine("druidsql");
        return _this;
    }
    DruidSQLExternal.fromJS = function (parameters, requester) {
        var value = External.jsToValue(parameters, requester);
        return new DruidSQLExternal(value);
    };
    DruidSQLExternal.postProcessIntrospect = function (columns) {
        return columns.map(function (column) {
            var name = column.COLUMN_NAME;
            var type;
            var nativeType = column.DATA_TYPE;
            switch (nativeType) {
                case 'TIMESTAMP':
                case 'DATE':
                    type = 'TIME';
                    break;
                case 'VARCHAR':
                    type = 'STRING';
                    break;
                case 'DOUBLE':
                case 'FLOAT':
                case 'BIGINT':
                    type = 'NUMBER';
                    break;
                default:
                    type = 'NULL';
                    break;
            }
            return new AttributeInfo({
                name: name,
                type: type,
                nativeType: nativeType
            });
        }).filter(Boolean);
    };
    DruidSQLExternal.getSourceList = function (requester) {
        return toArray(requester({
            query: {
                query: "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid' AND TABLE_TYPE = 'TABLE'"
            }
        }))
            .then(function (sources) {
            if (!sources.length)
                return sources;
            return sources.map(function (s) { return s['TABLE_NAME']; }).sort();
        });
    };
    DruidSQLExternal.getVersion = function (requester) {
        return toArray(requester({
            query: {
                queryType: 'status'
            }
        }))
            .then(function (res) {
            return res[0].version;
        });
    };
    DruidSQLExternal.prototype.getIntrospectAttributes = function () {
        return toArray(this.requester({
            query: {
                query: "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = " + this.dialect.escapeLiteral(this.source)
            }
        }))
            .then(DruidSQLExternal.postProcessIntrospect);
    };
    DruidSQLExternal.prototype.sqlToQuery = function (sql) {
        return {
            query: sql,
        };
    };
    DruidSQLExternal.prototype.capability = function (cap) {
        if (cap === 'filter-on-attribute' || cap === 'shortcut-group-by')
            return false;
        return _super.prototype.capability.call(this, cap);
    };
    DruidSQLExternal.engine = 'druidsql';
    DruidSQLExternal.type = 'DATASET';
    return DruidSQLExternal;
}(SQLExternal));
export { DruidSQLExternal };
External.register(DruidSQLExternal);
