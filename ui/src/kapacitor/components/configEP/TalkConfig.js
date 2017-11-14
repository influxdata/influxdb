import React, {PropTypes} from 'react'

const TalkConfig = () => {
  return (
    <div className="rule-section--row rule-section--border-bottom">
      <p>Talk requires no additional alert parameters.</p>
    </div>
  )
}

const {func, shape} = PropTypes

TalkConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default TalkConfig
