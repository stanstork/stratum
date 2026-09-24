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

/** Instance-scoped scratch key-value store (gated by `allow_kv`; not persisted). */
export const kv: {
    /** Value for `key`, or null if absent or the capability is denied. */
    get(key: string): string | null;
    set(key: string, value: string): void;
};

/** Custom metrics (gated by `allow_metrics`). No-op when denied. */
export const metrics: {
    /** Add `value` (truncated to an integer) to a named counter. */
    counter(name: string, value: number): void;
    /** Set a named gauge to `value`. */
    gauge(name: string, value: number): void;
};

/** Environment variables granted via `allow_env`. */
export const env: {
    /** Value for `name`, or null if unset or not granted. */
    get(name: string): string | null;
};

/** File access within directories granted via `allow_fs_read` / `allow_fs_write`. */
export const fs: {
    /** File contents as text, or null if missing/unreadable/ungranted. */
    readText(path: string): string | null;
    /** Write text to a file; returns true on success. */
    writeText(path: string, contents: string): boolean;
};