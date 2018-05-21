import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import IFQLNew from 'src/ifql/components/IFQLNew'
import IFQLEdit from 'src/ifql/components/IFQLEdit'

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
  onDismiss: () => void
  notify: (message: Notification) => void
  createService: CreateServiceAsync
  updateService: UpdateServiceAsync
}

class IFQLOverlay extends PureComponent<Props> {
  public render() {
    return (
      <div className="ifql-overlay">
        <div className="template-variable-manager--header">
          <div className="page-header__left">
            <h1 className="page-header__title">Connect to IFQL</h1>
          </div>
          <div className="page-header__right">
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
        <IFQLNew
          source={source}
          notify={notify}
          onDismiss={onDismiss}
          createService={createService}
        />
      )
    }

    return (
      <IFQLEdit
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

export default connect(null, mdtp)(IFQLOverlay)
