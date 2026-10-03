import { describe, expect, test } from 'claude-code/testing'

import { chart, describe as line, sky, tokens } from '../hooks/register'

describe('weather bands', () => {
  test('each band starts where the forecast says', () => {
    const cases: [number, string, string][] = [
      [0, 'Clear', 'yellow'],
      [24, 'Clear', 'yellow'],
      [25, 'Cloudy', 'cyan'],
      [49, 'Cloudy', 'cyan'],
      [50, 'Showers', 'blue'],
      [74, 'Showers', 'blue'],
      [75, 'Storm', 'magenta'],
      [89, 'Storm', 'magenta'],
      [90, 'Compact soon', 'red'],
      [100, 'Compact soon', 'red'],
    ]
    for (const [percent, word, color] of cases) {
      expect(sky(percent)).toMatchObject({ word, color })
    }
    expect(sky(10).icon).toBe('☀')
    expect(sky(30).icon).toBe('☁')
    expect(sky(60).icon).toBe('☂')
    expect(sky(80).icon).toBe('☇')
    expect(sky(95).icon).toBe('↯')
  })
})

describe('token counts', () => {
  test('read as k and M with at most one decimal', () => {
    expect(tokens(950)).toBe('950')
    expect(tokens(134_400)).toBe('134.4k')
    expect(tokens(200_000)).toBe('200k')
    expect(tokens(98_300)).toBe('98.3k')
    expect(tokens(1_000_000)).toBe('1M')
    expect(tokens(1_250_000)).toBe('1.3M')
  })
})

describe('chart', () => {
  test('scales each bar against the whole window and clamps', () => {
    expect(chart([0, 25_000, 100_000, 199_999, 200_000, 400_000], 200_000)).toBe('▁▂▅███')
  })
})

describe('plain line', () => {
  test('shows a compaction as a drop', () => {
    expect(line({ samples: [180_000, 40_000], window: 200_000, delta: -140_000 })).toBe(
      '☀ Clear 20% · 40k / 200k █▂ ▼ −140k last turn',
    )
  })
})
