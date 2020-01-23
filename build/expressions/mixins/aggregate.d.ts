import { Expression } from '../baseExpression';
export declare class Aggregate {
    operand: Expression;
    isAggregate(): boolean;
    isNester(): boolean;
    fullyDefined(): boolean;
}
