import * as tslib_1 from "tslib";
import * as toArray from 'stream-to-array';
import { AttributeInfo } from '../datatypes/attributeInfo';
import { PostgresDialect } from '../dialect/postgresDialect';
import { External } from './baseExternal';
import { SQLExternal } from './sqlExternal';
var PostgresExternal = (function (_super) {
    tslib_1.__extends(PostgresExternal, _super);
    function PostgresExternal(parameters) {
        var _this = _super.call(this, parameters, new PostgresDialect()) || this;
        _this._ensureEngine("postgres");
        return _this;
    }
    PostgresExternal.fromJS = function (parameters, requester) {
        var value = External.jsToValue(parameters, requester);
        return new PostgresExternal(value);
    };
    PostgresExternal.postProcessIntrospect = function (columns) {
        return columns.map(function (column) {
            var name = column.name;
            var type;
            var nativeType = column.sqlType.toLowerCase();
            if (nativeType.indexOf('timestamp') !== -1) {
                type = 'TIME';
            }
            else if (nativeType === 'character varying') {
                type = 'STRING';
            }
            else if (nativeType === 'integer' || nativeType === 'bigint') {
                type = 'NUMBER';
            }
            else if (nativeType === 'double precision' || nativeType === 'float') {
                type = 'NUMBER';
            }
            else if (nativeType === 'boolean') {
                type = 'BOOLEAN';
            }
            else if (nativeType === 'array') {
                nativeType = column.arrayType.toLowerCase();
                if (nativeType === 'character') {
                    type = 'SET/STRING';
                }
                else if (nativeType === 'timestamp') {
                    type = 'SET/TIME';
                }
                else if (nativeType === 'integer' || nativeType === 'bigint' || nativeType === 'double precision' || nativeType === 'float') {
                    type = 'SET/NUMBER';
                }
                else if (nativeType === 'boolean') {
                    type = 'SET/BOOLEAN';
                }
                else {
                    return null;
                }
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
    PostgresExternal.getSourceList = function (requester) {
        return toArray(requester({
            query: "SELECT table_name AS \"tab\" FROM INFORMATION_SCHEMA.TABLES WHERE table_type = 'BASE TABLE' AND table_schema = 'public'"
        }))
            .then(function (sources) {
            if (!sources.length)
                return sources;
            return sources.map(function (s) { return s['tab']; }).sort();
        });
    };
    PostgresExternal.getVersion = function (requester) {
        return toArray(requester({ query: 'SELECT version()' }))
            .then(function (res) {
            if (!Array.isArray(res) || res.length !== 1)
                throw new Error('invalid version response');
            var key = Object.keys(res[0])[0];
            if (!key)
                throw new Error('invalid version response (no key)');
            var versionString = res[0][key];
            var match;
            if (match = versionString.match(/^PostgreSQL (\S+) on/))
                versionString = match[1];
            return versionString;
        });
    };
    PostgresExternal.prototype.getIntrospectAttributes = function () {
        return toArray(this.requester({
            query: "SELECT c.column_name as \"name\", c.data_type as \"sqlType\", e.data_type AS \"arrayType\"\n       FROM information_schema.columns c LEFT JOIN information_schema.element_types e\n       ON ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier)\n       = (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))\n       WHERE table_name = " + this.dialect.escapeLiteral(this.source)
        }))
            .then(PostgresExternal.postProcessIntrospect);
    };
    PostgresExternal.engine = 'postgres';
    PostgresExternal.type = 'DATASET';
    return PostgresExternal;
}(SQLExternal));
export { PostgresExternal };
External.register(PostgresExternal);
