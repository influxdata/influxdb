// Libraries
import React, {useState, SFC, KeyboardEvent} from 'react'

// Components
import {Form, Input, SelectGroup, Grid} from '@influxdata/clockface'

// Utils
import {useOneWayState} from 'src/shared/utils/useOneWayState'

// Types
import {Columns, ButtonShape} from '@influxdata/clockface'

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
    <Form.Element
      label={label}
      errorMessage={errorMessage}
      className="auto-domain-input"
    >
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Twelve}>
            <SelectGroup shape={ButtonShape.StretchToFit}>
              <SelectGroup.Option
                name="auto-domain"
                id="radio_auto"
                titleText="Auto"
                active={!showInputs}
                onClick={handleChooseAuto}
                value="Auto"
              >
                Auto
              </SelectGroup.Option>
              <SelectGroup.Option
                name="auto-domain"
                id="radio_custom"
                titleText="Custom"
                active={showInputs}
                onClick={handleChooseCustom}
                value="Custom"
              >
                Custom
              </SelectGroup.Option>
            </SelectGroup>
          </Grid.Column>
        </Grid.Row>
        {showInputs && (
          <Grid.Row className="auto-domain-input--custom">
            <MinMaxInputs
              initialMin={initialMin}
              initialMax={initialMax}
              onSetMinMax={onSetDomain}
              onSetErrorMessage={setErrorMessage}
            />
          </Grid.Row>
        )}
      </Grid>
    </Form.Element>
  )
}

export default AutoDomainInput
