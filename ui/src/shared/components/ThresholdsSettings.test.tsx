import * as React from 'react'
import {useState} from 'react'
import {render, fireEvent, wait} from 'react-testing-library'
import ThresholdsSettings from 'src/shared/components/ThresholdsSettings'
import {BASE_THRESHOLD_ID} from 'src/shared/constants/thresholds'
import {Color} from 'src/types'

describe('ThresholdSettings', () => {
  const getErrorMessage = (container, thresholdID) => {
    const node = container.querySelector(
      `.threshold-setting[data-test-id='${thresholdID}'] .threshold-setting--error`
    )

    return node ? node.textContent.trim() : null
  }

  const getInput = (container, thresholdID) =>
    container.querySelector(
      `.threshold-setting[data-test-id='${thresholdID}'] input`
    )

  test('making then correcting an error', () => {
    const thresholds: Color[] = [
      {
        id: BASE_THRESHOLD_ID,
        type: 'threshold',
        name: 'thunder',
        hex: '',
        value: null,
      },
      {id: '0', type: 'threshold', name: 'fire', hex: '', value: 30},
    ]

    const {container} = render(
      <ThresholdsSettings thresholds={thresholds} onSetThresholds={jest.fn()} />
    )

    // Enter an invalid value in the input
    fireEvent.change(getInput(container, '0'), {
      target: {value: 'baloney'},
    })

    // Blur the input
    fireEvent.blur(getInput(container, '0'))

    // Expect an error message to exist
    expect(getErrorMessage(container, '0')).toEqual(
      'Please enter a valid number'
    )

    // Enter a valid value in the input
    fireEvent.change(getInput(container, '0'), {
      target: {value: '9000'},
    })

    // Blur the input
    fireEvent.blur(getInput(container, '0'))

    // Expect there to be no error
    expect(getErrorMessage(container, '0')).toBeNull()
  })

  test('entering value less than min threshold shows error', () => {
    const thresholds: Color[] = [
      {id: '0', type: 'min', name: 'thunder', hex: '', value: 20},
      {id: '1', type: 'threshold', name: 'fire', hex: '', value: 30},
      {id: '2', type: 'max', name: 'ruby', hex: '', value: 60},
    ]

    const {container} = render(
      <ThresholdsSettings thresholds={thresholds} onSetThresholds={jest.fn()} />
    )

    // Enter a value in the input
    fireEvent.change(getInput(container, '1'), {
      target: {value: '10'},
    })

    // Blur the input
    fireEvent.blur(getInput(container, '1'))

    // Expect an error message to exist
    expect(getErrorMessage(container, '1')).toEqual(
      'Please enter a value greater than the minimum threshold'
    )
  })

  test('entering value greater than max threshold shows error', () => {
    const thresholds: Color[] = [
      {id: '0', type: 'min', name: 'thunder', hex: '', value: 20},
      {id: '1', type: 'threshold', name: 'fire', hex: '', value: 30},
      {id: '2', type: 'max', name: 'ruby', hex: '', value: 60},
    ]

    const {container} = render(
      <ThresholdsSettings thresholds={thresholds} onSetThresholds={jest.fn()} />
    )

    // Enter a value in the input
    fireEvent.change(getInput(container, '1'), {
      target: {value: '80'},
    })

    // Blur the input
    fireEvent.blur(getInput(container, '1'))

    // Expect an error message to be called
    expect(getErrorMessage(container, '1')).toEqual(
      'Please enter a value less than the maximum threshold'
    )
  })

  test('broadcasts edited thresholds only when changes are valid', async () => {
    const handleSetThresholdsSpy = jest.fn()

    const TestWrapper = () => {
      const [thresholds, setThresholds] = useState<Color[]>([
        {id: '0', type: 'min', name: 'thunder', hex: '', value: 20},
        {id: '1', type: 'threshold', name: 'fire', hex: '', value: 30},
        {id: '2', type: 'max', name: 'ruby', hex: '', value: 60},
      ])

      const [didRerender, setDidRerender] = useState(false)

      const handleSetThresholds = newThresholds => {
        setThresholds(newThresholds)
        setDidRerender(true)
        handleSetThresholdsSpy(newThresholds)
      }

      return (
        <>
          {didRerender && <div data-testid="did-rerender" />}
          <ThresholdsSettings
            thresholds={thresholds}
            onSetThresholds={handleSetThresholds}
          />
        </>
      )
    }

    const {container, getByTestId} = render(<TestWrapper />)

    // Enter an invalid value in the input
    fireEvent.change(getInput(container, '1'), {
      target: {value: 'baloney'},
    })

    // Blur the input
    fireEvent.blur(getInput(container, '1'))

    // Now enter a valid value
    fireEvent.change(getInput(container, '1'), {
      target: {value: '40'},
    })

    // Blur the input again
    fireEvent.blur(getInput(container, '1'))

    // Wait for the changes to propogate to the test component
    await wait(() => {
      getByTestId('did-rerender')
    })

    // Changed thresholds should only have emitted once
    expect(handleSetThresholdsSpy).toHaveBeenCalledTimes(1)

    // ...with the expected values
    expect(handleSetThresholdsSpy.mock.calls[0][0]).toEqual([
      {id: '0', type: 'min', name: 'thunder', hex: '', value: 20},
      {id: '1', type: 'threshold', name: 'fire', hex: '', value: 40},
      {id: '2', type: 'max', name: 'ruby', hex: '', value: 60},
    ])
  })
})
