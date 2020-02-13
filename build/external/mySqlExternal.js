import * as tslib_1 from "tslib";
import * as toArray from 'stream-to-array';
import { AttributeInfo } from '../datatypes/index';
import { MySQLDialect } from '../dialect/mySqlDialect';
import { External } from './baseExternal';
import { SQLExternal } from './sqlExternal';
var MySQLExternal = (function (_super) {
    tslib_1.__extends(MySQLExternal, _super);
    function MySQLExternal(parameters) {
        var _this = _super.call(this, parameters, new MySQLDialect()) || this;
        _this._ensureEngine("mysql");
        return _this;
    }
    MySQLExternal.fromJS = function (parameters, requester) {
        var value = External.jsToValue(parameters, requester);
        return new MySQLExternal(value);
    };
    MySQLExternal.postProcessIntrospect = function (columns) {
        return columns.map(function (column) {
            var name = column.Field;
            var type;
            var nativeType = column.Type.toLowerCase();
            if (nativeType === "datetime" || nativeType === "timestamp") {
                type = 'TIME';
            }
            else if (nativeType.indexOf("varchar(") === 0 || nativeType.indexOf("blob") === 0) {
                type = 'STRING';
            }
            else if (nativeType.indexOf("int(") === 0 ||
                nativeType.indexOf("bigint(") === 0 ||
                nativeType.indexOf("decimal(") === 0 ||
                nativeType.indexOf("float") === 0 ||
                nativeType.indexOf("double") === 0) {
                type = 'NUMBER';
            }
            else if (nativeType.indexOf("tinyint(1)") === 0) {
                type = 'BOOLEAN';
            }
            else {
                return null;
            }
            return new AttributeInfo({
                name: name,
                type: type,
                nativeType: nativeType
            });
        }).filter(Boolean);
    };
    MySQLExternal.getSourceList = function (requester) {
        return toArray(requester({ query: "SHOW TABLES" }))
            .then(function (sources) {
            if (!Array.isArray(sources))
                throw new Error('invalid sources response');
            if (!sources.length)
                return sources;
            var key = Object.keys(sources[0])[0];
            if (!key)
                throw new Error('invalid sources response (no key)');
            return sources.map(function (s) { return s[key]; }).sort();
        });
    };
    MySQLExternal.getVersion = function (requester) {
        return toArray(requester({ query: 'SELECT @@version' }))
            .then(function (res) {
            if (!Array.isArray(res) || res.length !== 1)
                throw new Error('invalid version response');
            var key = Object.keys(res[0])[0];
            if (!key)
                throw new Error('invalid version response (no key)');
            return res[0][key];
        });
    };
    MySQLExternal.prototype.getIntrospectAttributes = function () {
        return toArray(this.requester({ query: "DESCRIBE " + this.dialect.escapeName(this.source) }))
            .then(MySQLExternal.postProcessIntrospect);
    };
    MySQLExternal.engine = 'mysql';
    MySQLExternal.type = 'DATASET';
    return MySQLExternal;
}(SQLExternal));
export { MySQLExternal };
External.register(MySQLExternal);
