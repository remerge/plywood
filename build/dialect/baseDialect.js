var SQLDialect = (function () {
    function SQLDialect() {
        this.escapedTableName = null;
    }
    SQLDialect.prototype.setTable = function (name) {
        if (name) {
            this.escapedTableName = this.escapeName(name);
        }
        else {
            this.escapedTableName = null;
        }
    };
    SQLDialect.prototype.nullConstant = function () {
        return 'NULL';
    };
    SQLDialect.prototype.constantGroupBy = function () {
        return "GROUP BY ''";
    };
    SQLDialect.prototype.escapeName = function (name) {
        name = name.replace(/"/g, '""');
        return '"' + name + '"';
    };
    SQLDialect.prototype.maybeNamespacedName = function (name) {
        var escapedName = this.escapeName(name);
        if (this.escapedTableName) {
            return this.escapedTableName + '.' + escapedName;
        }
        else {
            return escapedName;
        }
    };
    SQLDialect.prototype.escapeLiteral = function (name) {
        if (name === null)
            return this.nullConstant();
        name = name.replace(/'/g, "''");
        return "'" + name + "'";
    };
    SQLDialect.prototype.booleanToSQL = function (bool) {
        return ('' + bool).toUpperCase();
    };
    SQLDialect.prototype.numberOrTimeToSQL = function (x) {
        if (x === null)
            return this.nullConstant();
        if (x.toISOString) {
            return this.timeToSQL(x);
        }
        else {
            return this.numberToSQL(x);
        }
    };
    SQLDialect.prototype.numberToSQL = function (num) {
        if (num === null)
            return this.nullConstant();
        return '' + num;
    };
    SQLDialect.prototype.dateToSQLDateString = function (date) {
        return date.toISOString()
            .replace('T', ' ')
            .replace('Z', '')
            .replace(/\.000$/, '')
            .replace(/ 00:00:00$/, '');
    };
    SQLDialect.prototype.aggregateFilterIfNeeded = function (inputSQL, expressionSQL, elseSQL) {
        if (elseSQL === void 0) { elseSQL = null; }
        var whereIndex = inputSQL.indexOf(' WHERE ');
        if (whereIndex === -1)
            return expressionSQL;
        var filterSQL = inputSQL.substr(whereIndex + 7);
        return this.ifThenElseExpression(filterSQL, expressionSQL, elseSQL);
    };
    SQLDialect.prototype.concatExpression = function (a, b) {
        throw new Error('must implement');
    };
    SQLDialect.prototype.containsExpression = function (a, b) {
        throw new Error('must implement');
    };
    SQLDialect.prototype.substrExpression = function (a, position, length) {
        return "SUBSTR(" + a + "," + (position + 1) + "," + length + ")";
    };
    SQLDialect.prototype.coalesceExpression = function (a, b) {
        return "COALESCE(" + a + ", " + b + ")";
    };
    SQLDialect.prototype.ifThenElseExpression = function (a, b, c) {
        if (c === void 0) { c = null; }
        var elsePart = c != null ? " ELSE " + c : '';
        return "CASE WHEN " + a + " THEN " + b + elsePart + " END";
    };
    SQLDialect.prototype.isNotDistinctFromExpression = function (a, b) {
        var nullConst = this.nullConstant();
        if (a === nullConst)
            return b + " IS " + nullConst;
        if (b === nullConst)
            return a + " IS " + nullConst;
        return "(" + a + " IS NOT DISTINCT FROM " + b + ")";
    };
    SQLDialect.prototype.regexpExpression = function (expression, regexp) {
        return "(" + expression + " REGEXP '" + regexp + "')";
    };
    SQLDialect.prototype.inExpression = function (operand, start, end, bounds) {
        if (start === end && bounds === '[]')
            return operand + "=" + start;
        var startSQL = null;
        if (start !== this.nullConstant()) {
            startSQL = start + (bounds[0] === '[' ? '<=' : '<') + operand;
        }
        var endSQL = null;
        if (end !== this.nullConstant()) {
            endSQL = operand + (bounds[1] === ']' ? '<=' : '<') + end;
        }
        if (startSQL) {
            return endSQL ? "(" + startSQL + " AND " + endSQL + ")" : startSQL;
        }
        else {
            return endSQL ? endSQL : 'TRUE';
        }
    };
    SQLDialect.prototype.lengthExpression = function (a) {
        return "CHAR_LENGTH(" + a + ")";
    };
    return SQLDialect;
}());
export { SQLDialect };
