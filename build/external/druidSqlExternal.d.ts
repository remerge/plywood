import { PlywoodRequester } from 'plywood-base-api';
import { Attributes } from '../datatypes';
import { ExternalJS, ExternalValue } from './baseExternal';
import { SQLExternal } from './sqlExternal';
export interface DruidSQLDescribeRow {
    COLUMN_NAME: string;
    DATA_TYPE: string;
}
export declare class DruidSQLExternal extends SQLExternal {
    static engine: string;
    static type: string;
    static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any>): DruidSQLExternal;
    static postProcessIntrospect(columns: DruidSQLDescribeRow[]): Attributes;
    static getSourceList(requester: PlywoodRequester<any>): Promise<string[]>;
    static getVersion(requester: PlywoodRequester<any>): Promise<string>;
    constructor(parameters: ExternalValue);
    protected getIntrospectAttributes(): Promise<Attributes>;
    protected sqlToQuery(sql: string): any;
    protected capability(cap: string): boolean;
}
