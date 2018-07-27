import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import _ from 'lodash'

import FluxForm from 'src/flux/components/FluxForm'

import {Service, Notification} from 'src/types'
import {
  fluxUpdated,
  fluxNotUpdated,
  notifyFluxNameAlreadyTaken,
} from 'src/shared/copy/notifications'
import {UpdateServiceAsync} from 'src/shared/actions/services'
import {FluxFormMode} from 'src/flux/constants/connection'

interface Props {
  service: Service
  services: Service[]
  onDismiss?: () => void
  updateService: UpdateServiceAsync
  notify: (message: Notification) => void
}

interface State {
  service: Service
}

class FluxEdit extends PureComponent<Props, State> {
  public static getDerivedStateFromProps(nextProps: Props, prevState: State) {
    if (_.isEmpty(prevState.service) && !_.isEmpty(nextProps.service)) {
      return {service: nextProps.service}
    }
    return null
  }

  constructor(props: Props) {
    super(props)
    this.state = {
      service: this.props.service,
    }
  }

  public render() {
    return (
      <FluxForm
        service={this.state.service}
        onSubmit={this.handleSubmit}
        onInputChange={this.handleInputChange}
        mode={FluxFormMode.EDIT}
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
    const {notify, onDismiss, updateService, services} = this.props
    const {service} = this.state
    service.name = service.name.trim()
    let isNameTaken = false
    services.forEach(s => {
      if (s.name === service.name && s.id !== service.id) {
        isNameTaken = true
      }
    })

    if (isNameTaken) {
      notify(notifyFluxNameAlreadyTaken(service.name))
      return
    }

    try {
      await updateService(service)
    } catch (error) {
      notify(fluxNotUpdated(error.message))
      return
    }

    notify(fluxUpdated)
    if (onDismiss) {
      onDismiss()
    }
  }
}

export default FluxEdit
