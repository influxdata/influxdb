import React, {PropTypes} from 'react'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

const TalkHandler = ({selectedHandler, configLink}) => {
  return selectedHandler.enabled
    ? <div className="rule-section--row rule-section--border-bottom">
        <p>Talk requires no additional alert parameters.</p>
      </div>
    : <HandlerEmpty configLink={configLink} />
}

const {func, shape, string} = PropTypes

TalkHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  configLink: string,
}

export default TalkHandler
