import React, {PureComponent, ChangeEvent, FormEvent} from 'react'

import FluxForm from 'src/flux/components/FluxForm'

import {NewService, Source, Notification} from 'src/types'
import {fluxCreated, fluxNotCreated} from 'src/shared/copy/notifications'
import {CreateServiceAsync} from 'src/shared/actions/services'

interface Props {
  source: Source
  onDismiss: () => void
  createService: CreateServiceAsync
  notify: (message: Notification) => void
}

interface State {
  service: NewService
}

const port = 8093

class FluxNew extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      service: this.defaultService,
    }
  }

  public render() {
    return (
      <FluxForm
        service={this.state.service}
        onSubmit={this.handleSubmit}
        onInputChange={this.handleInputChange}
        mode="new"
      />
    )
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    const {value, name} = e.target
    const update = {[name]: value}

    this.setState({service: {...this.state.service, ...update}})
  }

  private handleSubmit = async (
    e: FormEvent<HTMLFormElement>
  ): Promise<void> => {
    e.preventDefault()
    const {notify, source, onDismiss, createService} = this.props

    const {service} = this.state

    try {
      await createService(source, service)
    } catch (error) {
      notify(fluxNotCreated(error.message))
      return
    }

    notify(fluxCreated)
    onDismiss()
  }

  private get defaultService(): NewService {
    return {
      name: 'Flux',
      url: this.url,
      username: '',
      insecureSkipVerify: false,
      type: 'flux',
      active: true,
    }
  }

  private get url(): string {
    const parser = document.createElement('a')
    parser.href = this.props.source.url

    return `${parser.protocol}//${parser.hostname}:${port}`
  }
}

export default FluxNew
