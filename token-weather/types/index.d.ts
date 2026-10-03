export type Forecast = {
  /** Context tokens at the end of each recent turn, oldest first (at most 12). */
  samples: number[]
  /** The model's context window, in tokens. */
  window: number
  /** Tokens the last turn added (negative after a compaction). */
  delta: number
}

declare module 'claude-code' {
  interface PluginState {
    'token-weather': { forecast: Forecast | null }
  }
}
