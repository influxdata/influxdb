// Libraries
import React, {FunctionComponent, ChangeEvent} from 'react'

// Components
import {AutoInput, AutoInputMode, Input, InputType} from '@influxdata/clockface'

// Utils
import {convertUserInputToNumOrNaN} from 'src/shared/utils/convertUserInput'

// Actions
import {setBinCount} from 'src/timeMachine/actions'

interface Props {
  binCount?: number
  onSetBinCount: typeof setBinCount
}

const BinCountInput: FunctionComponent<Props> = ({binCount, onSetBinCount}) => {
  const handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    const binCount = convertUserInputToNumOrNaN(e)

    onSetBinCount(binCount)
  }

  const handleChangeMode = (mode: AutoInputMode): void => {
    if (mode === AutoInputMode.Auto) {
      onSetBinCount(null)
    } else {
      onSetBinCount(30)
    }
  }

  return (
    <AutoInput
      mode={
        typeof binCount === 'number' ? AutoInputMode.Custom : AutoInputMode.Auto
      }
      onChangeMode={handleChangeMode}
      inputComponent={
        <Input
          name="binCount"
          value={binCount}
          placeholder="Enter a number"
          type={InputType.Number}
          min={0}
          onChange={handleInputChange}
        />
      }
    />
  )
}

export default BinCountInput
