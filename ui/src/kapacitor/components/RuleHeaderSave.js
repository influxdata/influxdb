import React, {PropTypes} from 'react'
import ReactTooltip from 'react-tooltip'
import SourceIndicator from 'shared/components/SourceIndicator'

const RuleHeaderSave = ({onSave, validationError}) =>
  <div className="page-header__right">
    <SourceIndicator />
    {validationError
      ? <button
          className="btn btn-success btn-sm disabled"
          data-for="save-kapacitor-tooltip"
          data-tip={validationError}
        >
          Save Rule
        </button>
      : <button className="btn btn-success btn-sm" onClick={onSave}>
          Save Rule
        </button>}
    <ReactTooltip
      id="save-kapacitor-tooltip"
      effect="solid"
      html={true}
      place="bottom"
      class="influx-tooltip kapacitor-tooltip"
    />
  </div>

const {func, string} = PropTypes

RuleHeaderSave.propTypes = {
  onSave: func.isRequired,
  validationError: string.isRequired,
}

export default RuleHeaderSave
