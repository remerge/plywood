import { Datum, PlywoodValue } from '../datatypes/index';
import { ComputeOptions, Expression, ComputeContext } from '../expressions/baseExpression';
export interface Executor {
    (ex: Expression, opt?: ComputeOptions, computeContext?: ComputeContext): Promise<PlywoodValue>;
}
export interface BasicExecutorParameters {
    datasets: Datum;
}
export declare function basicExecutorFactory(parameters: BasicExecutorParameters): Executor;
