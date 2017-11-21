import React, {PropTypes} from 'react'
import EmptyEndpoint from 'src/kapacitor/components/EmptyEndpoint'

const TalkConfig = ({selectedEndpoint, configLink}) => {
  return selectedEndpoint.enabled
    ? <div className="rule-section--row rule-section--border-bottom">
        <p>Talk requires no additional alert parameters.</p>
      </div>
    : <EmptyEndpoint configLink={configLink} />
}

const {func, shape, string} = PropTypes

TalkConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
  configLink: string,
}

export default TalkConfig
