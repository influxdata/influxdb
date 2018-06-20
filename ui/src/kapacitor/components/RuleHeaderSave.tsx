import React, {Component} from 'react'

import ReactTooltip from 'react-tooltip'
import SourceIndicator from 'src/shared/components/SourceIndicator'

import {ErrorHandling} from 'src/shared/decorators/errors'

import {Source} from 'src/types'

interface Props {
  onSave: () => void
  validationError: string
  source: Source
}

@ErrorHandling
class RuleHeaderSave extends Component<Props> {
  public render() {
    const {source} = this.props
    return (
      <div className="page-header__right">
        <SourceIndicator sourceOverride={source} />
        {this.saveRuleButton}
        <ReactTooltip
          id="save-kapacitor-tooltip"
          effect="solid"
          html={true}
          place="bottom"
          class="influx-tooltip kapacitor-tooltip"
        />
      </div>
    )
  }

  private get saveRuleButton() {
    const {onSave, validationError} = this.props
    if (validationError) {
      return (
        <button
          className="btn btn-success btn-sm disabled"
          data-for="save-kapacitor-tooltip"
          data-tip={validationError}
        >
          Save Rule
        </button>
      )
    } else {
      return (
        <button className="btn btn-success btn-sm" onClick={onSave}>
          Save Rule
        </button>
      )
    }
  }
}

export default RuleHeaderSave
