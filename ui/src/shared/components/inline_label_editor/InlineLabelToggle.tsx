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
import 'src/shared/components/inline_label_editor/InlineLabelToggle.scss'

interface Props {
  onClick: () => void
}

const InlineLabelToggle: SFC<Props> = ({onClick}) => {
  return (
    <div className="inline-label-toggle">
      <Button
        color={ComponentColor.Secondary}
        titleText="Add labels"
        onClick={onClick}
        shape={ButtonShape.Square}
        icon={IconFont.Plus}
      />
    </div>
  )
}

export default InlineLabelToggle
