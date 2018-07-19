import React, {SFC} from 'react'
import ReactTooltip from 'react-tooltip'

interface Props {
  onSave: () => void
  validationError?: string
}

const RuleHeaderSave: SFC<Props> = ({onSave, validationError}) => (
  <>
    {validationError ? (
      <button
        className="btn btn-success btn-sm disabled"
        data-for="save-kapacitor-tooltip"
        data-tip={validationError}
      >
        Save Rule
      </button>
    ) : (
      <button className="btn btn-success btn-sm" onClick={onSave}>
        Save Rule
      </button>
    )}
    <ReactTooltip
      id="save-kapacitor-tooltip"
      effect="solid"
      html={true}
      place="bottom"
      class="influx-tooltip kapacitor-tooltip"
    />
  </>
)

export default RuleHeaderSave
