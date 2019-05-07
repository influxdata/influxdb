// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {SquareButton, IconFont} from '@influxdata/clockface'

interface Props {
  collapsible: boolean
  onClick: () => void
}

const AddCardButton: FunctionComponent<Props> = ({onClick}) => {
  return (
    <SquareButton
      className="query-builder--add-card-button"
      onClick={onClick}
      icon={IconFont.Plus}
    />
  )
}

export default AddCardButton
