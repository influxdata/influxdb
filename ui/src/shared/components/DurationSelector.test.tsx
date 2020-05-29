import React from 'react'
import DurationSelector from './DurationSelector'
import {render, getByText} from 'react-testing-library'

describe('DurationSelector', () => {
  test('should match selected duration to duration option', () => {
    const {getByTestId} = render(
      <DurationSelector
        selectedDuration="1h"
        onSelectDuration={() => {}}
        durations={[
          {duration: '1d', displayText: '1 Day'},
          {duration: '1h', displayText: '1 Hour'},
        ]}
      />
    )

    expect(
      getByText(getByTestId('duration-selector--button'), '1 Hour')
    ).toBeDefined()
  })

  test('should match selected duration to equivalent duration option', () => {
    const {getByTestId} = render(
      <DurationSelector
        selectedDuration="1h"
        onSelectDuration={() => {}}
        durations={[
          {duration: '1d', displayText: '1 Day'},
          {duration: '3600s', displayText: '1 Hour'},
        ]}
      />
    )

    expect(
      getByText(getByTestId('duration-selector--button'), '1 Hour')
    ).toBeDefined()
  })

  test('should be able to display any selected duration even if not passed as option', () => {
    const {getByTestId} = render(
      <DurationSelector
        selectedDuration="1h"
        onSelectDuration={() => {}}
        durations={[{duration: '1d', displayText: '1 Day'}]}
      />
    )

    expect(
      getByText(getByTestId('duration-selector--button'), '1h')
    ).toBeDefined()
  })
})
