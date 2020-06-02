// Libraries
import React, {FC} from 'react'
import classnames from 'classnames'

// Components
import {Icon, IconFont} from '@influxdata/clockface'

// Types
import {RawDataSize} from 'src/notebooks/pipes/Query'

interface Props {
  resultsExist: boolean
  size: RawDataSize
  onUpdateSize: (size: RawDataSize) => void
}

const ResultsHeader: FC<Props> = ({resultsExist, size, onUpdateSize}) => {
  if (!resultsExist) {
    return (
      <div className="notebook-raw-data--header">
        <Icon glyph={IconFont.DashF} />
      </div>
    )
  }

  const handleClick = (newSize: RawDataSize) => (): void => {
    onUpdateSize(newSize)
  }

  const generateClassName = (buttonSize: RawDataSize): string => {
    return classnames('notebook-raw-data--size-toggle', {
      [`notebook-raw-data--size-toggle__${buttonSize}`]: buttonSize,
      'notebook-raw-data--size-toggle__active': buttonSize === size,
    })
  }

  return (
    <div className="notebook-raw-data--header">
      <div
        className={generateClassName('small')}
        onClick={handleClick('small')}
      />
      <div
        className={generateClassName('medium')}
        onClick={handleClick('medium')}
      />
      <div
        className={generateClassName('large')}
        onClick={handleClick('large')}
      />
    </div>
  )
}

export default ResultsHeader
