import React, {PropTypes} from 'react'
import ReactTooltip from 'react-tooltip'

const QuestionMarkTooltip = ({tipID, tipContent}) =>
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

const {string} = PropTypes

QuestionMarkTooltip.propTypes = {
  tipID: string.isRequired,
  tipContent: string.isRequired,
}

export default QuestionMarkTooltip
