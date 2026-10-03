import { atom, read, update } from 'claude-code'
import type { EngineInterface, Register } from 'claude-code'

import type { Forecast } from '../types'

const forecast = atom({ plugin: 'token-weather', key: 'forecast' } as const, null)

const HISTORY = 12
/** The latest forecast as plain text, for surfaces that do not draw the band. */
export const SNAPSHOT = '/tmp/token-weather/forecast.txt'
const BARS = '▁▂▃▄▅▆▇█'

type Sky = { icon: string; word: string; color: string }

export function sky(percent: number): Sky {
  if (percent < 25) return { icon: '☀', word: 'Clear', color: 'yellow' }
  if (percent < 50) return { icon: '☁', word: 'Cloudy', color: 'cyan' }
  if (percent < 75) return { icon: '☂', word: 'Showers', color: 'blue' }
  if (percent < 90) return { icon: '☇', word: 'Storm', color: 'magenta' }
  return { icon: '↯', word: 'Compact soon', color: 'red' }
}

/** 134400 → "134.4k", 200000 → "200k", 1000000 → "1M". */
export function tokens(n: number): string {
  const abs = Math.abs(n)
  if (abs >= 1_000_000) return `${trim(n / 1_000_000)}M`
  if (abs >= 1_000) return `${trim(n / 1_000)}k`
  return String(Math.round(n))
}

function trim(n: number): string {
  return n.toFixed(1).replace(/\.0$/, '')
}

/** One bar per sample, scaled against the whole window. */
export function chart(samples: number[], window: number): string {
  return samples
    .map(s => BARS[Math.min(BARS.length - 1, Math.max(0, Math.floor((s / window) * BARS.length)))])
    .join('')
}

/** The whole forecast as one plain line. */
export function describe(f: Forecast): string {
  const now = f.samples[f.samples.length - 1] ?? 0
  const percent = Math.round((now / f.window) * 100)
  const s = sky(percent)
  const delta = `${f.delta >= 0 ? '▲ +' : '▼ −'}${tokens(Math.abs(f.delta))} last turn`
  return `${s.icon} ${s.word} ${percent}% · ${tokens(now)} / ${tokens(f.window)} ${chart(f.samples, f.window)} ${delta}`
}

async function observe($: EngineInterface): Promise<void> {
  const { context } = await $.session.usage()
  if (context.tokens === undefined || context.window <= 0) return
  const now = context.tokens

  await update($, forecast, (prev): Forecast => {
    const samples = prev?.samples ?? []
    const last = samples[samples.length - 1]
    return {
      samples: [...samples, now].slice(-HISTORY),
      window: context.window,
      delta: last === undefined ? now : now - last,
    }
  })
}

export const register: Register = on => {
  on('session.start', async ($, e, next) => {
    await $.command.register({ name: 'weather', description: 'Show the context window forecast' })
    return next(e)
  })

  on('command.run', { command: 'weather' }, async $ => {
    if ((await read($, forecast)) === null) await observe($)
    const f = await read($, forecast)
    return { text: f === null ? 'No forecast yet: the window fills from the first response.' : describe(f) }
  })

  on('turn.complete', async ($, e, next) => {
    const result = await next(e)
    // Only the main thread's turns move the main window.
    if (e.agentId === undefined) {
      await observe($)
      const f = await read($, forecast)
      if (f !== null) {
        const surfaces = (await $.session.surfaces()).join(', ') || 'none'
        await $.fs.write(SNAPSHOT, `${describe(f)}\nsurfaces: ${surfaces}\n`).catch(() => {})
      }
    }
    return result
  })

  on('ui.render', { component: 'AbovePrompt' }, async ($, e, next) => {
    const f = await read($, forecast)
    const now = f?.samples[f.samples.length - 1]
    if (e.props.hasSurvey || f === null || now === undefined) return next(e)

    const { Box, Text } = $.ui.resolve(e)
    const percent = Math.round((now / f.window) * 100)
    const s = sky(percent)
    const isUp = f.delta >= 0

    return (
      <Box>
        <Text color={s.color} bold>
          {s.icon} {s.word}
        </Text>
        <Text> {percent}% · {tokens(now)} / {tokens(f.window)} </Text>
        <Text color={s.color}>
          {chart(f.samples, f.window)}
        </Text>
        <Text dimColor>
          {' '}
          {isUp ? '▲ +' : '▼ −'}
          {tokens(Math.abs(f.delta))} last turn
        </Text>
      </Box>
    )
  })
}
