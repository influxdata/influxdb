import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {
  Input,
  InputType,
  Table,
  BorderType,
  ComponentSize,
} from '@influxdata/clockface'

import BucketsDropdown from 'src/dataLoaders/components/BucketsDropdown'

// Types
import {AppState, Bucket, ResourceType} from 'src/types'

// Redux
import {updateTemplateEnvReferences} from 'src/templates/actions/creators'
import {getAll} from 'src/resources/selectors'

export type EnvRef = any

interface OwnProps {
  resource: any
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & OwnProps

interface State {
  selectedBucketID: string
}

class CommunityTemplateEnvReferencesUnconnected extends PureComponent<
  Props,
  State
> {
  state = {
    selectedBucketID: 'default',
  }

  componentDidMount() {
    // for the case when the user selected a bucket, then minimized the section, then re-opened the section
    const previouslySelectedRef = this.props.resource.envReferences.find(
      ref => {
        if (this.getFieldType(ref) === 'bucket') {
          if (
            this.props.stagedTemplateEnvReferences[ref.envRefKey].value !==
            ref.defaultValue
          ) {
            return true
          }
        }
      }
    )

    if (previouslySelectedRef) {
      const previouslySelectedBucket = this.props.buckets.find(bucket => {
        if (
          bucket.name ===
          this.props.stagedTemplateEnvReferences[
            previouslySelectedRef.envRefKey
          ].value
        ) {
          return true
        }
      })

      if (previouslySelectedBucket) {
        this.setState({
          selectedBucketID: previouslySelectedBucket.id,
        })
      }
    }
  }

  render() {
    return (
      <Table borders={BorderType.None} cellPadding={ComponentSize.ExtraSmall}>
        <Table.Header>
          <Table.Row>
            <Table.HeaderCell>Parameter</Table.HeaderCell>
            <Table.HeaderCell>Value</Table.HeaderCell>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {this.props.resource.envReferences.map(ref => {
            const fieldType = this.getFieldType(ref)
            // this is a brittle way to do this, but it's the best we have now.
            // when we have the ability to distinguish types on the flux side, we can remove this.
            // this tech debt was brought to you by a higher up decision that we need this -now-.
            if (fieldType === 'bucket') {
              const {defaultValue} = ref
              const defaultValueAsBucket = {
                id: 'default',
                name: defaultValue,
              } as Bucket

              return (
                <Table.Row key={ref.envRefKey}>
                  <Table.Cell>
                    <strong>{fieldType}</strong>
                  </Table.Cell>
                  <Table.Cell>
                    <BucketsDropdown
                      selectedBucketID={this.state.selectedBucketID}
                      buckets={[defaultValueAsBucket, ...this.props.buckets]}
                      onSelectBucket={this.createRefBucketSelectHandler(ref)}
                      style={{width: '225px'}}
                    />
                  </Table.Cell>
                </Table.Row>
              )
            }

            return (
              <Table.Row key={ref.envRefKey}>
                <Table.Cell>
                  <strong>{fieldType}</strong>
                </Table.Cell>
                <Table.Cell>{this.renderInputForEnvRef(ref)}</Table.Cell>
              </Table.Row>
            )
          })}
        </Table.Body>
      </Table>
    )
  }

  private createRefBucketSelectHandler = ref => {
    return event => {
      let selectedBucketID = ''
      if (event.hasOwnProperty('id')) {
        selectedBucketID = event.id
      }

      this.setState({selectedBucketID})
      this.props.updateTemplateEnvReferences(
        ref.envRefKey,
        ref.resourceField,
        event.name,
        ref.valueType
      )
    }
  }

  private handleRefChange = ref => {
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
            onChange={this.handleRefChange(ref)}
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
            onChange={this.handleRefChange(ref)}
          />
        )
      }
    }
  }

  private getFieldType(ref: EnvRef) {
    return ref.resourceField.split('.').pop()
  }
}

const mstp = (state: AppState) => {
  return {
    stagedTemplateEnvReferences:
      state.resources.templates.stagedTemplateEnvReferences,
    buckets: getAll<Bucket>(state, ResourceType.Buckets),
  }
}

const mdtp = {
  updateTemplateEnvReferences,
}

const connector = connect(mstp, mdtp)

export const CommunityTemplateEnvReferences = connector(
  CommunityTemplateEnvReferencesUnconnected
)
