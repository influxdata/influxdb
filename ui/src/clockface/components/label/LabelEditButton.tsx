// Libraries
import React, {SFC} from 'react'

// Components
import {
  Button,
  ButtonShape,
  IconFont,
  ComponentColor,
} from '@influxdata/clockface'

// Styles
import 'src/clockface/components/label/LabelEditButton.scss'

interface Props {
  onClick: () => void
  resourceName: string
}

const LabelEditButton: SFC<Props> = ({onClick, resourceName}) => {
  return (
    <div className="label--edit-button">
      <Button
        color={ComponentColor.Primary}
        titleText={`Add labels to ${resourceName}`}
        onClick={onClick}
        shape={ButtonShape.Square}
        icon={IconFont.Plus}
      />
    </div>
  )
}

export default LabelEditButton
