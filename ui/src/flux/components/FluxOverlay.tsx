import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import FluxNew from 'src/flux/components/FluxNew'
import FluxEdit from 'src/flux/components/FluxEdit'

import {Service, Source, Notification} from 'src/types'

import {notify as notifyAction} from 'src/shared/actions/notifications'
import {
  updateServiceAsync,
  UpdateServiceAsync,
  createServiceAsync,
  CreateServiceAsync,
} from 'src/shared/actions/services'

interface Props {
  mode: string
  source?: Source
  service?: Service
  onDismiss?: () => void
  notify: (message: Notification) => void
  createService: CreateServiceAsync
  updateService: UpdateServiceAsync
}

class FluxOverlay extends PureComponent<Props> {
  public render() {
    return (
      <div className="flux-overlay">
        <div className="template-variable-manager--header">
          <div className="page-header--left">
            <h1 className="page-header--title">Connect to Flux</h1>
          </div>
          <div className="page-header--right">
            <span
              className="page-header__dismiss"
              onClick={this.props.onDismiss}
            />
          </div>
        </div>
        {this.form}
      </div>
    )
  }

  private get form(): JSX.Element {
    const {
      mode,
      source,
      service,
      notify,
      onDismiss,
      createService,
      updateService,
    } = this.props

    if (mode === 'new') {
      return (
        <FluxNew
          source={source}
          notify={notify}
          onDismiss={onDismiss}
          createService={createService}
        />
      )
    }

    return (
      <FluxEdit
        notify={notify}
        service={service}
        onDismiss={onDismiss}
        updateService={updateService}
      />
    )
  }
}

const mdtp = {
  notify: notifyAction,
  createService: createServiceAsync,
  updateService: updateServiceAsync,
}

export default connect(null, mdtp)(FluxOverlay)
