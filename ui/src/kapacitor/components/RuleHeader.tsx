import React, {Component} from 'react'

import RuleHeaderSave from 'src/kapacitor/components/RuleHeaderSave'

import {ErrorHandling} from 'src/shared/decorators/errors'

import {Source} from 'src/types'

interface Props {
  source: Source
  onSave: () => void
  validationError: string
}

@ErrorHandling
class RuleHeader extends Component<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {source, onSave, validationError} = this.props

    return (
      <div className="page-header">
        <div className="page-header__container">
          <div className="page-header__left">
            <h1 className="page-header__title">Alert Rule Builder</h1>
          </div>
          <RuleHeaderSave
            source={source}
            onSave={onSave}
            validationError={validationError}
          />
        </div>
      </div>
    )
  }
}

export default RuleHeader
