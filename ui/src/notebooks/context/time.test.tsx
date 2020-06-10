import React from 'react'

import {render} from '@testing-library/react'

import {
  TimeContext,
  TimeProvider,
  DEFAULT_STATE,
} from 'src/notebooks/context/time'

describe('Notebook Time Context', () => {
  /* eslint-disable @typescript-eslint/unbound-method */
  const originalError = console.error

  beforeAll(() => {
    console.error = (...args) => {
      if (/Warning.*not wrapped in act/.test(args[0])) {
        console.log('TRIGGERING')
        return
      }
      originalError.call(console, ...args)
    }
  })

  afterAll(() => {
    console.error = originalError
  })
  /* eslint-enable @typescript-eslint/unbound-method */

  it('registers a default context on add', () => {
    const contextCallback = jest.fn()

    render(
      <TimeProvider>
        <TimeContext.Consumer>{contextCallback}</TimeContext.Consumer>
      </TimeProvider>
    )

    let context = contextCallback.mock.calls[0][0]
    expect(context.timeContext).toEqual({})

    context.addTimeContext('sweet')

    context = contextCallback.mock.calls[1][0]
    expect(context.timeContext).toEqual({
      sweet: DEFAULT_STATE,
    })
  })

  it('allows overriding the intial state provided by add', () => {
    const contextCallback = jest.fn()
    const initial = {
      range: 'testing',
      refresh: 0,
    }

    render(
      <TimeProvider>
        <TimeContext.Consumer>{contextCallback}</TimeContext.Consumer>
      </TimeProvider>
    )

    let context = contextCallback.mock.calls[0][0]
    expect(context.timeContext).toEqual({})

    context.addTimeContext('sweet', initial)

    context = contextCallback.mock.calls[1][0]
    expect(context.timeContext).toEqual({
      sweet: initial,
    })
  })

  it('allows for multiple contexts', () => {
    const contextCallback = jest.fn()

    render(
      <TimeProvider>
        <TimeContext.Consumer>{contextCallback}</TimeContext.Consumer>
      </TimeProvider>
    )

    let context = contextCallback.mock.calls[0][0]
    expect(context.timeContext).toEqual({})

    context.addTimeContext('sweet')
    context.addTimeContext('dope')

    context = contextCallback.mock.calls[2][0]
    expect(context.timeContext).toEqual({
      sweet: DEFAULT_STATE,
      dope: DEFAULT_STATE,
    })
  })

  it('yells if you try to overwrite something', () => {
    const contextCallback = jest.fn()

    render(
      <TimeProvider>
        <TimeContext.Consumer>{contextCallback}</TimeContext.Consumer>
      </TimeProvider>
    )

    const context = contextCallback.mock.calls[0][0]
    expect(context.timeContext).toEqual({})

    context.addTimeContext('sweet')
    expect(() => {
      context.addTimeContext('sweet')
    }).toThrow('TimeContext[sweet] already exists: use updateContext instead')
  })

  it('yells if you try to delete nothing', () => {
    const contextCallback = jest.fn()

    render(
      <TimeProvider>
        <TimeContext.Consumer>{contextCallback}</TimeContext.Consumer>
      </TimeProvider>
    )

    const context = contextCallback.mock.calls[0][0]
    expect(context.timeContext).toEqual({})

    expect(() => {
      context.removeTimeContext('sweet')
    }).toThrow("TimeContext[sweet] doesn't exist")
  })

  it('totally chill with deleting', () => {
    const contextCallback = jest.fn()

    render(
      <TimeProvider>
        <TimeContext.Consumer>{contextCallback}</TimeContext.Consumer>
      </TimeProvider>
    )

    let context = contextCallback.mock.calls[0][0]
    expect(context.timeContext).toEqual({})

    context.addTimeContext('sweet')
    context.removeTimeContext('sweet')

    context = contextCallback.mock.calls[2][0]
    expect(context.timeContext).toEqual({})
  })

  it('loves to partial update', () => {
    const contextCallback = jest.fn()

    render(
      <TimeProvider>
        <TimeContext.Consumer>{contextCallback}</TimeContext.Consumer>
      </TimeProvider>
    )

    let context = contextCallback.mock.calls[0][0]
    expect(context.timeContext).toEqual({})

    context.addTimeContext('sweet')

    context = contextCallback.mock.calls[1][0]
    expect(context.timeContext).toEqual({
      sweet: DEFAULT_STATE,
    })

    context.updateTimeContext('sweet', {refresh: 'not today'})

    context = contextCallback.mock.calls[2][0]
    expect(context.timeContext).toEqual({
      sweet: {
        range: DEFAULT_STATE.range,
        refresh: 'not today',
      },
    })
  })
})
