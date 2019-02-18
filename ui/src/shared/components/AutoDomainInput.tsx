// Libraries
import React, {useState, SFC, KeyboardEvent} from 'react'

// Components
import {Radio, ButtonShape, Input, Grid, Columns, Form} from 'src/clockface'

// Utils
import {useOneWayState} from 'src/shared/utils/useOneWayState'

interface MinMaxInputsProps {
  initialMin: string
  initialMax: string
  onSetMinMax: (minMax: [number, number]) => void
  onSetErrorMessage: (errorMessage: string) => void
}

const MinMaxInputs: SFC<MinMaxInputsProps> = ({
  initialMin,
  initialMax,
  onSetMinMax,
  onSetErrorMessage,
}) => {
  const [minInput, setMinInput] = useOneWayState(initialMin)
  const [maxInput, setMaxInput] = useOneWayState(initialMax)

  const emitIfValid = () => {
    const newMin = parseFloat(minInput)
    const newMax = parseFloat(maxInput)

    if (isNaN(newMin)) {
      onSetErrorMessage('Must supply a valid minimum value')
      return
    }

    if (isNaN(newMax)) {
      onSetErrorMessage('Must supply a valid maximum value')
      return
    }

    if (newMin >= newMax) {
      onSetErrorMessage('Minium value must be less than maximum')
      return
    }

    if (initialMin === minInput && initialMax === maxInput) {
      // Only emit the change event if an actual change has occurred
      return
    }

    onSetErrorMessage('')
    onSetMinMax([newMin, newMax])
  }

  const handleKeyPress = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      emitIfValid()
    }
  }

  return (
    <>
      <Grid.Column widthXS={Columns.Six}>
        <Form.Element label="Min">
          <Input
            value={minInput}
            onChange={e => setMinInput(e.target.value)}
            onBlur={emitIfValid}
            onKeyPress={handleKeyPress}
          />
        </Form.Element>
      </Grid.Column>
      <Grid.Column widthXS={Columns.Six}>
        <Form.Element label="Max">
          <Input
            value={maxInput}
            onChange={e => setMaxInput(e.target.value)}
            onBlur={emitIfValid}
            onKeyPress={handleKeyPress}
          />
        </Form.Element>
      </Grid.Column>
    </>
  )
}

interface AutoDomainInputProps {
  domain: [number, number]
  onSetDomain: (domain: [number, number]) => void
  label?: string
}

const AutoDomainInput: SFC<AutoDomainInputProps> = ({
  domain,
  onSetDomain,
  label = 'Set Domain',
}) => {
  const [showInputs, setShowInputs] = useState(!!domain)
  const [errorMessage, setErrorMessage] = useState('')

  const handleChooseAuto = () => {
    setShowInputs(false)
    setErrorMessage('')
    onSetDomain(null)
  }

  const handleChooseCustom = () => {
    setShowInputs(true)
    setErrorMessage('')
  }

  const initialMin = domain ? String(domain[0]) : ''
  const initialMax = domain ? String(domain[1]) : ''

  return (
    <Form.Element label={label} errorMessage={errorMessage}>
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Twelve}>
            <Radio shape={ButtonShape.StretchToFit}>
              <Radio.Button active={!showInputs} onClick={handleChooseAuto}>
                Auto
              </Radio.Button>
              <Radio.Button active={showInputs} onClick={handleChooseCustom}>
                Custom
              </Radio.Button>
            </Radio>
          </Grid.Column>
        </Grid.Row>
        <Grid.Row>
          {showInputs && (
            <MinMaxInputs
              initialMin={initialMin}
              initialMax={initialMax}
              onSetMinMax={onSetDomain}
              onSetErrorMessage={setErrorMessage}
            />
          )}
        </Grid.Row>
      </Grid>
    </Form.Element>
  )
}

export default AutoDomainInput
