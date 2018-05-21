import React, {PureComponent, ChangeEvent, FormEvent} from 'react'

import IFQLForm from 'src/ifql/components/IFQLForm'

import {Service, Notification} from 'src/types'
import {ifqlUpdated, ifqlNotUpdated} from 'src/shared/copy/notifications'
import {UpdateServiceAsync} from 'src/shared/actions/services'

interface Props {
  service: Service
  onDismiss: () => void
  updateService: UpdateServiceAsync
  notify: (message: Notification) => void
}

interface State {
  service: Service
}

class IFQLEdit extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      service: this.props.service,
    }
  }

  public render() {
    return (
      <IFQLForm
        service={this.state.service}
        onSubmit={this.handleSubmit}
        onInputChange={this.handleInputChange}
        mode="edit"
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
    const {notify, onDismiss, updateService} = this.props
    const {service} = this.state

    try {
      await updateService(service)
    } catch (error) {
      notify(ifqlNotUpdated(error.message))
      return
    }

    notify(ifqlUpdated)
    onDismiss()
  }
}

export default IFQLEdit
