import React, {ChangeEvent, PureComponent} from 'react'
import _ from 'lodash'

import Input from 'src/shared/components/KapacitorFormInput'

import {NewService} from 'src/types'
import {FluxFormMode} from 'src/flux/constants/connection'

interface Props {
  service: NewService
  mode: FluxFormMode
  onSubmit: (e: ChangeEvent<HTMLFormElement>) => void
  onInputChange: (e: ChangeEvent<HTMLInputElement>) => void
}

class FluxForm extends PureComponent<Props> {
  public render() {
    const {service, onSubmit, onInputChange} = this.props
    const name = _.get(service, 'name', '')

    return (
      <form onSubmit={onSubmit}>
        <Input
          name="url"
          label="Flux URL"
          value={this.url}
          placeholder={this.url}
          onChange={onInputChange}
          customClass="col-sm-6"
        />
        <Input
          name="name"
          label="Name"
          value={name}
          placeholder={name}
          onChange={onInputChange}
          maxLength={33}
          customClass="col-sm-6"
        />
        <div className="form-group form-group-submit col-xs-12 text-center">
          {this.saveButton}
        </div>
      </form>
    )
  }

  private get saveButton(): JSX.Element {
    const {mode} = this.props

    let text = 'Connect'

    if (mode === FluxFormMode.EDIT) {
      text = 'Save Changes'
    }

    return (
      <button
        className="btn btn-success"
        type="submit"
        data-test="submit-button"
      >
        <span className="icon checkmark" />
        {text}
      </button>
    )
  }

  private get url(): string {
    const {service} = this.props
    return _.get(service, 'url', '')
  }
}

export default FluxForm
