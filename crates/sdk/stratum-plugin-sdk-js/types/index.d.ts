export type FieldType = "i32" | "i64" | "f32" | "f64" | "string" | "bool" | "bytes";

export interface FieldSpec {
    type: FieldType;
    nullable?: boolean;
}

export type InputSchema = Record<string, FieldType | FieldSpec>;
export type OutputSchema = Record<string, FieldType | FieldSpec>;

export interface TransformOpts<I, O> {
    version: string;
    input: InputSchema;
    output: FieldType;
    compute(input: I): O;
}
export function transform<I = any, O = any>(name: string, opts: TransformOpts<I, O>): void;

export interface FilterOpts<I> {
    version: string;
    input: InputSchema;
    evaluate(input: I): { pass: boolean; reason?: string };
}
export function filter<I = any>(name: string, opts: FilterOpts<I>): void;

export interface SourceOpts<C, R> {
    version: string;
    output: OutputSchema;
    readPage(config: C, cursor: string | null): {
        records: R[];
        next_cursor: string | null;
        has_more: boolean;
    };
}
export function source<C = any, R = any>(name: string, opts: SourceOpts<C, R>): void;

export interface SinkOpts<C, R> {
    version: string;
    input: InputSchema;
    writeBatch(config: C, batch: { records: R[] }): { rows_written: number };
    prepare?(config: C, schema: any): void;
    finalize?(): void;
}
export function sink<C = any, R = any>(name: string, opts: SinkOpts<C, R>): void;

export const http: {
    get(url: string, opts?: { headers?: Record<string, string> }): { status: number; headers: Record<string, string>; body: string };
    post(url: string, body: any, opts?: { headers?: Record<string, string> }): { status: number; headers: Record<string, string>; body: string };
    put(url: string, body: any, opts?: { headers?: Record<string, string> }): { status: number; headers: Record<string, string>; body: string };
};

export const log: {
    info(msg: string): void;
    warn(msg: string): void;
    error(msg: string): void;
    debug(msg: string): void;
};