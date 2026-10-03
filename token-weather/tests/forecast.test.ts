import { describe, expect, test } from 'claude-code/testing'
import type { RenderElement, SessionUsage } from 'claude-code'

const BAND = {
  component: 'AbovePrompt',
  props: {
    hasSurvey: false,
    isWorking: false,
    maxRows: 10,
    bodyColumns: 120,
    scroll: { bodyRows: 10, offset: 0 },
    view: {},
  },
} as const

const turn = (turnId: string) => ({
  answer: 'ok',
  durationMs: 10,
  isAborted: false,
  turnId,
  reason: 'answer' as const,
})

const usage = (tokens: number | undefined): SessionUsage => ({
  startedAt: 0,
  context:
    tokens === undefined
      ? { window: 200_000 }
      : { tokens, window: 200_000, percent: Math.round((tokens / 200_000) * 100) },
  rateLimits: [],
})

describe('token-weather', () => {
  test('forecasts the window after each turn', async ($, on) => {
    let used = 36_100
    let written = ''
    on('turn.complete', () => ({ text: '' }))
    on('command.run', () => ({ text: '' }))
    on('session.surfaces', () => ({ value: ['terminal'] as const }))
    on('fs.write', (_$, e) => {
      written = e.text
      return { value: undefined }
    })
    on('session.usage', () => ({ value: usage(used) }))

    await $.turn.complete(turn('t1'))
    used = 134_400
    await $.turn.complete(turn('t2'))

    for (const surface of ['terminal', 'desktop'] as const) {
      const ui = await $.ui.mount({ plugin: 'token-weather', surface, ...BAND } as never)
      const texts = (await ui.findAll({ type: 'Text' })).map(t => t.text)
      expect(texts).toEqual(['☂ Showers', ' 67% · 134.4k / 200k ', '▂▆', ' ▲ +98.3k last turn'])
      expect((await ui.find({ type: 'Text', text: '☂ Showers' }))?.props.color).toBe('blue')
      await ui.unmount()
    }

    const line = '☂ Showers 67% · 134.4k / 200k ▂▆ ▲ +98.3k last turn'
    expect(written).toBe(`${line}\nsurfaces: terminal\n`)
    expect((await $.command.run({ command: 'weather', args: '' } as never)).text).toBe(line)
  })

  test('stays quiet before the first response', async ($, on) => {
    on('turn.complete', () => ({ text: '' }))
    on('session.surfaces', () => ({ value: [] }))
    on('session.usage', () => ({ value: usage(undefined) }))
    on('ui.render', ($, e) => {
      const { Text } = $.ui.resolve(e)
      return h(Text, {}, 'engine band') as RenderElement
    })
    await $.turn.complete(turn('t1'))
    const ui = await $.ui.mount({ plugin: 'token-weather', surface: 'terminal', ...BAND } as never)
    expect(await ui.find({ type: 'Text', text: /Clear|Cloudy|Showers|Storm|Compact/ })).toBeUndefined()
    expect(await ui.find({ type: 'Text', text: 'engine band' })).toBeDefined()
    await ui.unmount()
  })

  test('keeps the last 12 turns and skips subagent turns', async ($, on) => {
    let used = 0
    on('turn.complete', () => ({ text: '' }))
    on('session.surfaces', () => ({ value: [] }))
    on('fs.write', () => ({ value: undefined }))
    on('session.usage', () => ({ value: usage(used) }))
    on('command.run', () => ({ text: '' }))

    for (let i = 1; i <= 14; i++) {
      used = i * 10_000
      await $.turn.complete(turn(`t${i}`))
    }
    used = 190_000
    await $.turn.complete({ ...turn('sub'), agentId: 'a1' })

    const text = (await $.command.run({ command: 'weather', args: '' } as never)).text
    expect(text).toBe('☂ Showers 70% · 140k / 200k ▂▂▃▃▃▄▄▅▅▅▆▆ ▲ +10k last turn')
  })
})
