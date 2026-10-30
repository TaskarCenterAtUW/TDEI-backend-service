/** A union 5.0 `config`, exactly as `content.tdei_union_dataset` accepts it. */
export interface UnionConfig {
  snap?: Record<string, unknown>;
  dedup?: Record<string, unknown>;
  connect?: Record<string, unknown>;
  extensions?: Record<string, unknown>;
}

export interface MountOptions {
  /** Settings to start from; omitted = the recommended settings. */
  initialConfig?: UnionConfig;
  /** Show the settings without allowing changes. */
  readOnly?: boolean;
  /** Called after every change with the full config, and whether every value is complete and in range. */
  onChange?: (config: UnionConfig, valid: boolean) => void;
}

export interface UnionConfigurator {
  readonly version: string;
  /** The current config. */
  getConfig(): UnionConfig;
  /** Replace the current settings. Throws if the value is not a union config. */
  setConfig(config: UnionConfig): void;
  /** Switch read-only on or off. */
  setReadOnly(readOnly: boolean): void;
  /** Remove the configurator and its listeners; call on unmount. */
  destroy(): void;
}

/** Mount the configurator inside `element`. */
export function mount(element: HTMLElement, options?: MountOptions): UnionConfigurator;
