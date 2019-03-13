// Libraries
import React, {PureComponent} from 'react'

// Styles
import 'src/organizations/components/CreateVariableOverlay.scss'

// Components
import {Overlay} from 'src/clockface'
import VariableForm from 'src/organizations/components/VariableForm'

// Types
import {Variable, Organization} from '@influxdata/influx'

interface Props {
  onCreateVariable: (variable: Variable) => void
  onHideOverlay: () => void
  orgs: Organization[]
  visible: boolean
  initialScript?: string
}

export default class CreateVariableOverlay extends PureComponent<Props> {
  public render() {
    const {
      onHideOverlay,
      onCreateVariable,
      initialScript,
      orgs,
      visible,
    } = this.props

    return (
      <Overlay visible={visible}>
        <Overlay.Container maxWidth={1000}>
          <Overlay.Heading title="Create Variable" onDismiss={onHideOverlay} />
          <Overlay.Body>
            <VariableForm
              onCreateVariable={onCreateVariable}
              onHideOverlay={onHideOverlay}
              orgID={orgs[0].id} //TODO: display a list of orgs and have the user pick one
              initialScript={initialScript}
            />
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }
}
