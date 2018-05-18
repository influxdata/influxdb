import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

import IFQLForm from 'src/ifql/components/IFQLForm'

import {Source, NewService, Notification} from 'src/types'

import {notify as notifyAction} from 'src/shared/actions/notifications'
import {
  createServiceAsync,
  CreateServiceAsync,
} from 'src/shared/actions/services'
import {ifqlCreated, ifqlNotCreated} from 'src/shared/copy/notifications'

interface Props {
  source: Source
  onDismiss: () => void
  notify: (message: Notification) => void
  createService: CreateServiceAsync
}

interface State {
  service: NewService
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
      <IFQLForm
        service={this.state.service}
        onSubmit={this.handleSubmit}
        onInputChange={this.handleInputChange}
        exists={false}
      />
    )
  }

  private get defaultService(): NewService {
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

  private handleSubmit = async e => {
    e.preventDefault()
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
}

export default connect(null, mdtp)(IFQLOverlay)
