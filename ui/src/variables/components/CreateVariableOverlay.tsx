// Libraries
import React, {PureComponent} from 'react'

// Styles
import 'src/variables/components/CreateVariableOverlay.scss'

// Components
import {Overlay} from 'src/clockface'
import VariableForm from 'src/variables/components/VariableForm'

// Types
import {IVariable as Variable} from '@influxdata/influx'

interface Props {
  onCreateVariable: (variable: Pick<Variable, 'name' | 'arguments'>) => void
  onHideOverlay: () => void
  initialScript?: string
}

export default class CreateVariableOverlay extends PureComponent<Props> {
  public render() {
    const {onHideOverlay, onCreateVariable, initialScript} = this.props

    return (
      <Overlay.Container maxWidth={1000}>
        <Overlay.Heading title="Create Variable" onDismiss={onHideOverlay} />
        <Overlay.Body>
          <VariableForm
            onCreateVariable={onCreateVariable}
            onHideOverlay={onHideOverlay}
            initialScript={initialScript}
          />
        </Overlay.Body>
      </Overlay.Container>
    )
  }
}
