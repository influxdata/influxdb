import React, {SFC} from 'react'
import ReactTooltip from 'react-tooltip'

interface Props {
  tipID: string
  tipContent: string
}

const QuestionMarkTooltip: SFC<Props> = ({tipID, tipContent}) => (
  <div className="question-mark-tooltip">
    <div
      className="question-mark-tooltip--icon"
      data-for={`${tipID}-tooltip`}
      data-tip={tipContent}
    >
      ?
    </div>
    <ReactTooltip
      id={`${tipID}-tooltip`}
      effect="solid"
      html={true}
      place="bottom"
      class="influx-tooltip"
    />
  </div>
)

export default QuestionMarkTooltip
