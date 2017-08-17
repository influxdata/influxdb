import React, {PropTypes} from 'react'
import ReactTooltip from 'react-tooltip'
import TimeRangeDropdown from 'shared/components/TimeRangeDropdown'
import SourceIndicator from 'shared/components/SourceIndicator'

const RuleHeaderSave = ({
  source,
  onSave,
  timeRange,
  validationError,
  onChooseTimeRange,
}) =>
  <div className="page-header__right">
    <SourceIndicator sourceName={source.name} />
    <TimeRangeDropdown
      onChooseTimeRange={onChooseTimeRange}
      selected={timeRange}
      preventCustomTimeRange={true}
    />
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
      offset={{bottom: 4}}
      place="bottom"
      class="influx-tooltip kapacitor-tooltip place-bottom"
    />
  </div>

const {func, shape, string} = PropTypes

RuleHeaderSave.propTypes = {
  source: shape({}).isRequired,
  onSave: func.isRequired,
  validationError: string.isRequired,
  onChooseTimeRange: func.isRequired,
  timeRange: shape({}).isRequired,
}

export default RuleHeaderSave
