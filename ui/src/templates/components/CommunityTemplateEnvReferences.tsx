import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {Input, InputType, Table} from '@influxdata/clockface'

// Types
import {AppState} from 'src/types'

import {updateTemplateEnvReferences} from 'src/templates/actions/creators'

export type EnvRef = any

interface OwnProps {
  resource: any
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & OwnProps

class CommunityTemplateEnvReferencesUnconnected extends PureComponent<Props> {
  private handleChange = ref => {
    return event => {
      this.props.updateTemplateEnvReferences(
        ref.envRefKey,
        ref.resourceField,
        event.target.value,
        ref.valueType
      )
    }
  }

  private renderInputForEnvRef = (ref: EnvRef) => {
    switch (ref.valueType) {
      case 'float':
      case 'number':
      case 'integer': {
        return (
          <Input
            type={InputType.Number}
            value={
              this.props.stagedTemplateEnvReferences[ref.envRefKey].value as any
            }
            onChange={this.handleChange(ref)}
          />
        )
      }
      case 'string':
      default: {
        return (
          <Input
            type={InputType.Text}
            value={
              this.props.stagedTemplateEnvReferences[ref.envRefKey].value as any
            }
            onChange={this.handleChange(ref)}
          />
        )
      }
    }
  }

  private getFieldType(ref: EnvRef) {
    return ref.resourceField.split('.').pop()
  }

  render() {
    return (
      <Table>
        <Table.Header>
          <Table.Row>
            <Table.HeaderCell>Parameter</Table.HeaderCell>
            <Table.HeaderCell>Value</Table.HeaderCell>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {this.props.resource.envReferences.map(ref => {
            return (
              <Table.Row key={ref.envRefKey}>
                <Table.Cell>
                  <strong>{this.getFieldType(ref)}</strong>
                </Table.Cell>
                <Table.Cell>{this.renderInputForEnvRef(ref)}</Table.Cell>
              </Table.Row>
            )
          })}
        </Table.Body>
      </Table>
    )
  }
}

const mstp = (state: AppState) => {
  return {
    stagedTemplateEnvReferences:
      state.resources.templates.stagedTemplateEnvReferences,
  }
}

const mdtp = {
  updateTemplateEnvReferences,
}

const connector = connect(mstp, mdtp)

export const CommunityTemplateEnvReferences = connector(
  CommunityTemplateEnvReferencesUnconnected
)
