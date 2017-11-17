import React, {PropTypes} from 'react'
import EmptyEndpoint from 'src/kapacitor/components/EmptyEndpoint'

const TalkConfig = ({selectedEndpoint}) => {
  return selectedEndpoint.enabled
    ? <div className="rule-section--row rule-section--border-bottom">
        <p>Talk requires no additional alert parameters.</p>
      </div>
    : <EmptyEndpoint />
}

const {func, shape} = PropTypes

TalkConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default TalkConfig
