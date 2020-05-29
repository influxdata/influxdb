import * as React from 'react'
import {render, fireEvent, wait} from 'react-testing-library'
import {LowercaseCheckStatusLevel} from 'src/types'

import {scaleLinear} from 'd3-scale'

describe('EventMarkers', () => {
  test('Visibility Toggles', async () => {
    const levels: LowercaseCheckStatusLevel[] = ['ok', 'ok', 'ok']

    const eventsArray = [
      [
        {
          checkID: '04c300ce06300000',
          checkName: 'Name this Check',
          level: levels[0],
          message: 'Check: Name this Check is: ok',
          result: '_result',
          table: 0,
          time: 1573673040145,
          _start: 1573673040000,
          _stop: 1573673041000,
        },
        {
          checkID: '04c300ce06300000',
          checkName: 'Name this Check',
          level: levels[1],
          message: 'Check: Name this Check is: ok',
          result: '_result',
          table: 0,
          time: 1573673040145,
          _start: 1573673040000,
          _stop: 1573673041000,
        },
      ],
      [
        {
          checkID: '04c300ce06300000',
          checkName: 'Name this Check',
          level: levels[2],
          message: 'Check: Name this Check is: crit',
          result: '_result',
          table: 0,
          time: 1573673640140,
          _start: 1573673640000,
          _stop: 1573673641000,
        },
      ],
    ]

    const xDomain = [1, 2]

    const xScale = scaleLinear()
      .domain(xDomain)
      .range([1, 5])

    const xFormatter = x => {
      return String(x)
    }

    const divFunction = () => {
      return <div data-testid="eventMarker" />
    }

    jest.mock('src/shared/components/EventMarker', () => {
      return divFunction
    })

    const EventMarkers = require('src/shared/components/EventMarkers.tsx')
      .default

    const {getByTestId, getAllByTestId, queryByTestId} = render(
      <EventMarkers
        eventsArray={eventsArray}
        xScale={xScale}
        xFormatter={xFormatter}
        xDomain={xDomain}
      />
    )

    expect(queryByTestId(`eventMarker`)).toBeNull()
    expect(getByTestId(`event-marker-vis-icon-ok`).className).toContain(
      'eye-closed'
    )

    const okSpan = getByTestId('event-marker-vis-toggle-ok')
    fireEvent.click(okSpan)

    expect(getByTestId(`event-marker-vis-icon-ok`).className).toContain(
      'eye-open'
    )

    await wait(() => getByTestId(`eventMarker`))
    expect(getAllByTestId(`eventMarker`)).toHaveLength(2)
  })
})
