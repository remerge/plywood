import { PlywoodRequester } from 'plywood-base-api';
import { Attributes } from '../datatypes/attributeInfo';
import { ExternalJS, ExternalValue } from './baseExternal';
import { SQLExternal } from './sqlExternal';
export interface PostgresSQLDescribeRow {
    name: string;
    sqlType: string;
    arrayType?: string;
}
export declare class PostgresExternal extends SQLExternal {
    static engine: string;
    static type: string;
    static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any>): PostgresExternal;
    static postProcessIntrospect(columns: PostgresSQLDescribeRow[]): Attributes;
    static getSourceList(requester: PlywoodRequester<any>): Promise<string[]>;
    static getVersion(requester: PlywoodRequester<any>): Promise<string>;
    constructor(parameters: ExternalValue);
    protected getIntrospectAttributes(): Promise<Attributes>;
}
