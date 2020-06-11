import * as React from 'react'
import {useState} from 'react'
import {render, fireEvent, waitFor, getNodeText} from '@testing-library/react'
import ThresholdsSettings from 'src/shared/components/ThresholdsSettings'
import {BASE_THRESHOLD_ID} from 'src/shared/constants/thresholds'
import {Color} from 'src/types'

describe('ThresholdSettings', () => {
  test('making then correcting an error', () => {
    const thresholds: Color[] = [
      {
        id: BASE_THRESHOLD_ID,
        type: 'threshold',
        hex: '',
        name: 'thunder',
        value: 0,
      },
      {id: '0', type: 'threshold', name: 'fire', hex: '', value: 30},
    ]

    const {getByTestId} = render(
      <ThresholdsSettings thresholds={thresholds} onSetThresholds={jest.fn()} />
    )

    const inputElement = getByTestId(`threshold-${thresholds[1].id}-input`)

    // Enter an invalid value in the input
    fireEvent.change(inputElement, {
      target: {value: 'baloney'},
    })

    // Blur the input
    fireEvent.blur(inputElement)

    // Expect an error message to exist
    let thresholdsElementCount = getByTestId('threshold-settings')
      .childElementCount
    const errorElement = getByTestId(`threshold-${thresholds[1].id}-error`)
    const errorText = getNodeText(errorElement)
    expect(errorText).toEqual('Please enter a valid number')
    expect(thresholdsElementCount).toEqual(4)

    // Enter a valid value in the input
    fireEvent.change(inputElement, {
      target: {value: '9000'},
    })

    // Blur the input
    fireEvent.blur(inputElement)

    // Expect there to be no error
    thresholdsElementCount = getByTestId('threshold-settings').childElementCount
    expect(thresholdsElementCount).toEqual(3)
  })

  test('entering value less than min threshold shows error', () => {
    const thresholds: Color[] = [
      {id: '0', type: 'min', name: 'thunder', hex: '', value: 20},
      {id: '1', type: 'threshold', name: 'fire', hex: '', value: 30},
      {id: '2', type: 'max', name: 'ruby', hex: '', value: 60},
    ]

    const {getByTestId} = render(
      <ThresholdsSettings thresholds={thresholds} onSetThresholds={jest.fn()} />
    )

    const inputElement = getByTestId(`threshold-${thresholds[1].id}-input`)

    // Enter a value in the input
    fireEvent.change(inputElement, {
      target: {value: '10'},
    })

    // Blur the input
    fireEvent.blur(inputElement)

    // Expect an error message to exist
    const errorElement = getByTestId(`threshold-${thresholds[1].id}-error`)
    const errorText = getNodeText(errorElement)
    expect(errorText).toEqual(
      'Please enter a value greater than the minimum threshold'
    )
  })

  test('entering value greater than max threshold shows error', () => {
    const thresholds: Color[] = [
      {id: '0', type: 'min', name: 'thunder', hex: '', value: 20},
      {id: '1', type: 'threshold', name: 'fire', hex: '', value: 30},
      {id: '2', type: 'max', name: 'ruby', hex: '', value: 60},
    ]

    const {getByTestId} = render(
      <ThresholdsSettings thresholds={thresholds} onSetThresholds={jest.fn()} />
    )

    const inputElement = getByTestId(`threshold-${thresholds[1].id}-input`)

    // Enter a value in the input
    fireEvent.change(inputElement, {
      target: {value: '80'},
    })

    // Blur the input
    fireEvent.blur(inputElement)

    // Expect an error message to be called
    const errorElement = getByTestId(`threshold-${thresholds[1].id}-error`)
    const errorText = getNodeText(errorElement)
    expect(errorText).toEqual(
      'Please enter a value less than the maximum threshold'
    )
  })

  test('broadcasts edited thresholds only when changes are valid', async () => {
    const handleSetThresholdsSpy = jest.fn()
    const testShresholds: Color[] = [
      {id: '0', type: 'min', name: 'thunder', hex: '', value: 20},
      {id: '1', type: 'threshold', name: 'fire', hex: '', value: 30},
      {id: '2', type: 'max', name: 'ruby', hex: '', value: 60},
    ]

    const TestWrapper = () => {
      const [thresholds, setThresholds] = useState<Color[]>(testShresholds)

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

    const {getByTestId} = render(<TestWrapper />)

    const input1 = getByTestId(`threshold-${testShresholds[1].id}-input`)

    // Enter an invalid value in the input
    fireEvent.change(input1, {
      target: {value: 'baloney'},
    })

    // Blur the input
    fireEvent.blur(input1)

    // Now enter a valid value
    fireEvent.change(input1, {
      target: {value: '40'},
    })

    // Blur the input again
    fireEvent.blur(input1)

    // Wait for the changes to propogate to the test component
    await waitFor(() => {
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
