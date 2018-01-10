import React, {PropTypes} from 'react'

const HandlerEmpty = ({onGoToConfig, validationError}) =>
  <div className="endpoint-tab-contents">
    <div className="endpoint-tab--parameters">
      <div className="endpoint-tab--parameters--empty">
        <p>This handler has not been configured</p>
        <div className="form-group-submit col-xs-12 text-center">
          <button
            className="btn btn-primary"
            type="submit"
            onClick={onGoToConfig}
          >
            {validationError
              ? 'Exit Rule and Configure this Alert Handler'
              : 'Save Rule and Configure this Alert Handler'}
          </button>
        </div>
      </div>
    </div>
  </div>

const {string, func} = PropTypes

HandlerEmpty.propTypes = {
  onGoToConfig: func.isRequired,
  validationError: string.isRequired,
}

export default HandlerEmpty
