import React, {PureComponent} from 'react'
import Loadable from 'react-loadable'

// Components
import MapVariableBuilder from 'src/variables/components/MapVariableBuilder'
import CSVVariableBuilder from 'src/variables/components/CSVVariableBuilder'
import {Form, Grid} from '@influxdata/clockface'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

const spinner = <div />

const FluxEditor = Loadable({
  loader: () => import('src/shared/components/FluxEditor'),
  loading() {
    return spinner
  },
})

const FluxMonacoEditor = Loadable({
  loader: () => import('src/shared/components/FluxMonacoEditor'),
  loading() {
    return spinner
  },
})

// Types
import {KeyValueMap} from 'src/types'
import {VariableProperties as VariableArguments} from 'src/client'

interface Props {
  args: VariableArguments
  onChange: (update: {args: VariableArguments; isValid: boolean}) => void
  onSelectMapDefault: (selectedKey: string) => void
  selected: string[]
}

class VariableArgumentsEditor extends PureComponent<Props> {
  render() {
    const {args, onSelectMapDefault, selected} = this.props
    switch (args.type) {
      case 'query':
        return (
          <Form.Element label="Script">
            <Grid.Column>
              <div className="overlay-flux-editor">
                <FeatureFlag name="monacoEditor">
                  <FluxMonacoEditor
                    script={args.values.query}
                    onChangeScript={this.handleChangeQuery}
                  />
                </FeatureFlag>
                <FeatureFlag name="monacoEditor" equals={false}>
                  <FluxEditor
                    script={args.values.query}
                    onChangeScript={this.handleChangeQuery}
                    visibility="visible"
                    suggestions={[]}
                  />
                </FeatureFlag>
              </div>
            </Grid.Column>
          </Form.Element>
        )
      case 'map':
        return (
          <MapVariableBuilder
            onChange={this.handleChangeMap}
            values={args.values}
            onSelectDefault={onSelectMapDefault}
            selected={selected}
          />
        )
      case 'constant':
        return (
          <CSVVariableBuilder
            onChange={this.handleChangeCSV}
            values={args.values}
            onSelectDefault={onSelectMapDefault}
            selected={selected}
          />
        )
    }
  }

  private handleChangeCSV = (values: string[]) => {
    const {onChange} = this.props

    const updatedArgs = {type: 'constant' as 'constant', values}
    const isValid = values.length > 0

    onChange({args: updatedArgs, isValid})
  }

  private handleChangeQuery = (query: string) => {
    const {onChange} = this.props

    const values = {language: 'flux' as 'flux', query}
    const updatedArgs = {type: 'query' as 'query', values}

    const isValid = !query.match(/^\s*$/)

    onChange({args: updatedArgs, isValid})
  }

  private handleChangeMap = (update: {
    values: KeyValueMap
    errors: string[]
  }) => {
    const {onChange} = this.props

    const updatedArgs = {type: 'map' as 'map', values: update.values}

    const isValid =
      update.errors.length === 0 && Object.keys(update.values).length > 0

    onChange({args: updatedArgs, isValid})
  }
}

export default VariableArgumentsEditor
