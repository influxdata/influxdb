// Libraries
import React, {FC, CSSProperties} from 'react'
import {range} from 'lodash'

const PLACEHOLDER_MIN_WIDTH = 80
const PLACEHOLDER_MAX_WIDTH = 140
const PLACEHOLDER_HEIGHT = 10
const RANDOM_NUMBERS = range(30).map(_ => Math.random())

interface Props {
  index: number
  style: CSSProperties
}

const LoadingRow: FC<Props> = ({index, style}) => {
  const randomNumber = RANDOM_NUMBERS[index % RANDOM_NUMBERS.length]

  const width = Math.floor(
    PLACEHOLDER_MIN_WIDTH +
      randomNumber * (PLACEHOLDER_MAX_WIDTH - PLACEHOLDER_MIN_WIDTH)
  )

  return (
    <div style={style}>
      <div className="event-loading-row">
        <div
          className="event-loading-row--placeholder"
          style={{
            width: `${width}px`,
            height: `${PLACEHOLDER_HEIGHT}px`,
            animationDelay: `${((index % 5) / 2) * 100}ms`,
          }}
        />
      </div>
    </div>
  )
}

export default LoadingRow
