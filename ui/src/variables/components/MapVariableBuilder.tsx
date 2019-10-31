import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Component
import {
  Grid,
  Form,
  TextArea,
  Dropdown,
  Columns,
  Icon,
  IconFont,
} from '@influxdata/clockface'

// Utils
import {ErrorHandling} from 'src/shared/decorators/errors'
import {csvToMap, mapToCSV} from 'src/variables/utils/mapBuilder'
import {pluralize} from 'src/shared/utils/pluralize'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import {invalidMapType} from 'src/shared/copy/notifications'

type Values = {[key: string]: string}
interface OwnProps {
  values: Values
  onChange: (update: {values: Values; errors: string[]}) => void
  onSelectDefault: (selectedKey: string) => void
  selected?: string[]
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
    const {onSelectDefault} = this.props
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
          <Grid.Column widthXS={Columns.Six}>
            <p>
              Mapping Contains <strong>{entries.length}</strong> key-value pair
              {pluralize(entries)}
            </p>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Six}>
            <Form.Element label="Select A Default">
              <Dropdown
                button={(active, onClick) => (
                  <Dropdown.Button active={active} onClick={onClick}>
                    {this.defaultID}
                  </Dropdown.Button>
                )}
                menu={onCollapse => (
                  <Dropdown.Menu onCollapse={onCollapse}>
                    {entries.map(v => (
                      <Dropdown.Item
                        key={v.key}
                        id={v.key}
                        value={v.key}
                        onClick={onSelectDefault}
                        selected={v.key === this.defaultID}
                      >
                        <strong>{v.key}</strong>{' '}
                        <Icon glyph={IconFont.CaretRight} /> {v.value}
                      </Dropdown.Item>
                    ))}
                  </Dropdown.Menu>
                )}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
      </Form.Element>
    )
  }

  private get defaultID(): string {
    const {selected} = this.props
    const {entries} = this
    const firstEntry = _.get(entries, '0.key', 'Enter values above')

    return _.get(selected, '0', firstEntry)
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

  private handleBlur = (): void => {
    const {onChange} = this.props
    const {templateValuesString} = this.state

    const update = this.constructValuesFromString(templateValuesString)

    onChange(update)
  }

  private handleChange = (e: ChangeEvent<HTMLTextAreaElement>): void => {
    const templateValuesString = e.target.value
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
