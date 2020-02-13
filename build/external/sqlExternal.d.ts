import { Attributes } from '../datatypes/attributeInfo';
import { SQLDialect } from '../dialect/baseDialect';
import { FilterExpression, SortExpression } from '../expressions/index';
import { External, ExternalValue, QueryAndPostTransform } from './baseExternal';
export declare abstract class SQLExternal extends External {
    static type: string;
    dialect: SQLDialect;
    constructor(parameters: ExternalValue, dialect: SQLDialect);
    canHandleFilter(filter: FilterExpression): boolean;
    canHandleSort(sort: SortExpression): boolean;
    protected capability(cap: string): boolean;
    protected sqlToQuery(sql: string): any;
    protected getFrom(): string;
    getQueryAndPostTransform(): QueryAndPostTransform<string>;
    protected abstract getIntrospectAttributes(): Promise<Attributes>;
}
