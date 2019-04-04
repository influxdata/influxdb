// Libraries
import React, {PureComponent} from 'react'

// Styles
import 'src/organizations/components/CreateVariableOverlay.scss'

// Components
import {Overlay} from 'src/clockface'
import VariableForm from 'src/organizations/components/VariableForm'

// Types
import {IVariable as Variable} from '@influxdata/influx'

interface Props {
  onCreateVariable: (variable: Variable) => void
  onHideOverlay: () => void
  orgID: string
  initialScript?: string
}

export default class CreateVariableOverlay extends PureComponent<Props> {
  public render() {
    const {onHideOverlay, onCreateVariable, orgID, initialScript} = this.props

    return (
      <Overlay.Container maxWidth={1000}>
        <Overlay.Heading title="Create Variable" onDismiss={onHideOverlay} />
        <Overlay.Body>
          <VariableForm
            onCreateVariable={onCreateVariable}
            onHideOverlay={onHideOverlay}
            orgID={orgID}
            initialScript={initialScript}
          />
        </Overlay.Body>
      </Overlay.Container>
    )
  }
}
