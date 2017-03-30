import React, {PropTypes} from 'react'
import ReactTooltip from 'react-tooltip'

const QuestionMarkTooltip = ({
  tipID,
  tipContent,
}) => (
  <div style={{display: "inline-block"}}>
    <div data-for={`${tipID}-tooltip`} data-tip={tipContent} style={{margin: "0 5px"}}>?</div>
    <ReactTooltip
      id={`${tipID}-tooltip`}
      effect="solid"
      html={true}
      offset={{top: 2}}
      place="bottom"
      class="influx-tooltip__hover place-bottom"
    />
  </div>
)

const {
  string,
} = PropTypes

QuestionMarkTooltip.propTypes = {
  tipID: string.isRequired,
  tipContent: string.isRequired,
}

export default QuestionMarkTooltip
