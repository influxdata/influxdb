import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

import IFQLForm from 'src/ifql/components/IFQLForm'

import {Service, Source, NewService, Notification} from 'src/types'

import {notify as notifyAction} from 'src/shared/actions/notifications'
import {
  updateServiceAsync,
  UpdateServiceAsync,
  createServiceAsync,
  CreateServiceAsync,
} from 'src/shared/actions/services'

import {
  ifqlCreated,
  ifqlNotCreated,
  ifqlNotUpdated,
} from 'src/shared/copy/notifications'

interface Props {
  mode: string
  source?: Source
  service?: Service
  onDismiss: () => void
  notify: (message: Notification) => void
  createService: CreateServiceAsync
  updateService: UpdateServiceAsync
}

interface State {
  service: Service
}

const port = 8093

class IFQLOverlay extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      service: this.defaultService,
    }
  }

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
        <IFQLForm
          service={this.state.service}
          onSubmit={this.handleSubmit}
          onInputChange={this.handleInputChange}
          exists={false}
        />
      </div>
    )
  }

  private get defaultService(): NewService {
    if (this.props.mode === 'edit') {
      return this.props.service
    }

    return {
      name: 'IFQL',
      url: this.url,
      username: '',
      insecureSkipVerify: false,
      type: 'ifql',
      active: true,
    }
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    const {value, name} = e.target
    const update = {[name]: value}

    this.setState({service: {...this.state.service, ...update}})
  }

  private handleSubmit = (e): void => {
    e.preventDefault()
    if (this.props.mode === 'edit') {
      this.handleEdit()
      return
    }

    this.handleCreate()
  }

  private handleEdit = async (): Promise<void> => {
    const {notify, onDismiss, updateService} = this.props
    const {service} = this.state

    try {
      await updateService(service)
    } catch (error) {
      notify(ifqlNotUpdated(error.message))
      return
    }

    notify(ifqlCreated)
    onDismiss()
  }

  private handleCreate = async (): Promise<void> => {
    const {notify, source, onDismiss, createService} = this.props

    const {service} = this.state

    try {
      await createService(source, service)
    } catch (error) {
      notify(ifqlNotCreated(error.message))
      return
    }

    notify(ifqlCreated)
    onDismiss()
  }

  private get url(): string {
    const parser = document.createElement('a')
    parser.href = this.props.source.url

    return `${parser.protocol}//${parser.hostname}:${port}`
  }
}

const mdtp = {
  notify: notifyAction,
  createService: createServiceAsync,
  updateService: updateServiceAsync,
}

export default connect(null, mdtp)(IFQLOverlay)
