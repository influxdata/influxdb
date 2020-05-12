// Libraries
import React, {FC} from 'react'

// Components
import {ComponentColor, QuestionMarkTooltip} from '@influxdata/clockface'

const GraphTips: FC = () => (
  <>
    <QuestionMarkTooltip
      diameter={18}
      color={ComponentColor.Primary}
      testID="graphtips-question-mark"
      tooltipContents={
        <>
          <h1>Graph Tips:</h1>
          <p>
            <code>Click + Drag</code> Zoom in (X or Y)
            <br />
            <code>Shift + Click</code> Pan Graph Window
            <br />
            <code>Double Click</code> Reset Graph Window
          </p>
          <h1>Static Legend Tips:</h1>
          <p>
            <code>Click</code>Focus on single Series
            <br />
            <code>Shift + Click</code> Show/Hide single Series
          </p>
        </>
      }
    />
  </>
)

export default GraphTips
