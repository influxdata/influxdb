import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Component
// import TemplatePreviewList from 'src/tempVars/components/TemplatePreviewList'
import {Grid, Form, TextArea, Dropdown} from '@influxdata/clockface'

// Utils
import {ErrorHandling} from 'src/shared/decorators/errors'
import {csvToMap, mapToCSV} from 'src/variables/utils/mapBuilder'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import {invalidMapType} from 'src/shared/copy/notifications'

type Values = {[key: string]: string}
interface OwnProps {
  values: Values
  onChange: (update: {values: Values; errors: string[]}) => void
  onSelectDefault: (selectedKey: string) => void
  selected: string[]
}

interface DispatchProps {
  notify: typeof notifyAction
}

type Props = DispatchProps & OwnProps

interface State {
  templateValuesString: string
}

@ErrorHandling
class MapVariableBuilder extends PureComponent<Props, State> {
  state: State = {
    templateValuesString: mapToCSV(this.props.values),
  }

  public render() {
    const {onSelectDefault, selected} = this.props
    const {templateValuesString} = this.state
    const {entries} = this

    return (
      <Form.Element label="Comma Separated Values">
        <Grid.Row>
          <Grid.Column>
            <TextArea
              value={templateValuesString}
              onChange={this.handleChange}
              onBlur={this.handleBlur}
            />
          </Grid.Column>
        </Grid.Row>
        <Grid.Row>
          <p>
            Mapping Contains <strong>{entries.length}</strong> key-value pair
            {this.pluralizer}
          </p>
          {entries.length > 0 && (
            <Form.Element label="Selected Default">
              <Dropdown
                selectedID={!!selected ? selected[0] : entries[0].key}
                onChange={onSelectDefault}
              >
                {entries.map(v => (
                  <Dropdown.Item key={v.key} id={v.key} value={v.key}>
                    <strong>{v.key}</strong> -> {v.value}
                  </Dropdown.Item>
                ))}
              </Dropdown>
            </Form.Element>
          )}
          <Grid.Column />
        </Grid.Row>
      </Form.Element>
    )
  }

  private get entries(): {key: string; value: string}[] {
    const {values} = this.props
    if (!values) {
      return []
    }

    return Object.entries(values).map(([key, value]) => ({
      key,
      value,
    }))
  }

  private get pluralizer(): string {
    return Object.keys(this.props.values).length === 1 ? '' : 's'
  }

  private handleBlur = (): void => {
    const {onChange} = this.props
    const {templateValuesString} = this.state

    const update = this.constructValuesFromString(templateValuesString)

    onChange(update)
  }

  private handleChange = (templateValuesString: string): void => {
    this.setState({templateValuesString})
  }

  private constructValuesFromString(templateValuesString: string) {
    const {notify} = this.props

    const {errors, values} = csvToMap(templateValuesString)

    if (errors.length > 0) {
      notify(invalidMapType())
    }

    return {values, errors}
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(MapVariableBuilder)
