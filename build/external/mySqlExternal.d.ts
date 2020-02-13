import { PlywoodRequester } from 'plywood-base-api';
import { Attributes } from '../datatypes/index';
import { ExternalJS, ExternalValue } from './baseExternal';
import { SQLExternal } from './sqlExternal';
export interface MySQLDescribeRow {
    Field: string;
    Type: string;
}
export declare class MySQLExternal extends SQLExternal {
    static engine: string;
    static type: string;
    static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any>): MySQLExternal;
    static postProcessIntrospect(columns: MySQLDescribeRow[]): Attributes;
    static getSourceList(requester: PlywoodRequester<any>): Promise<string[]>;
    static getVersion(requester: PlywoodRequester<any>): Promise<string>;
    constructor(parameters: ExternalValue);
    protected getIntrospectAttributes(): Promise<Attributes>;
}
