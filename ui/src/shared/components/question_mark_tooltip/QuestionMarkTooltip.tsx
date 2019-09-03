// Libraries
import React, {SFC, ReactChild} from 'react'

// Components
import {Popover, PopoverInteraction} from '@influxdata/clockface'

interface Props {
  testID: string
  tipContent: ReactChild
}

const QuestionMarkTooltip: SFC<Props> = ({testID, tipContent, children}) => {
  let trigger = <div className="question-mark-tooltip--icon" />

  if (children) {
    trigger = (
      <div className="question-mark-tooltip--children">
        {children}
        <div className="question-mark-tooltip--icon question-mark-tooltip--icon__adjacent" />
      </div>
    )
  }

  return (
    <Popover
      distanceFromTrigger={6}
      className="question-mark-tooltip"
      testID={testID}
      showEvent={PopoverInteraction.Hover}
      hideEvent={PopoverInteraction.Hover}
      contents={() => (
        <div className="question-mark-tooltip--contents">{tipContent}</div>
      )}
    >
      {trigger}
    </Popover>
  )
}

export default QuestionMarkTooltip
