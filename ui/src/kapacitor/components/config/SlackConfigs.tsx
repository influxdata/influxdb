import React, {PureComponent, MouseEvent} from 'react'
import _ from 'lodash'

import {ErrorHandling} from 'src/shared/decorators/errors'
import SlackConfig from 'src/kapacitor/components/config/SlackConfig'
import {SlackProperties} from 'src/types/kapacitor'

interface Config {
  options: {
    url: boolean
    channel: string
    workspace: string
  }
}

interface Props {
  configs: Config[]
  onSave: (
    properties: SlackProperties,
    isNewConfigInSection: boolean,
    specificConfig: string
  ) => void
  onDelete: (specificConfig: string) => void
  onTest: (e: MouseEvent<HTMLButtonElement>, specificConfig: string) => void
  onEnabled: (specificConfig: string) => boolean
}

interface State {
  configs: Config[]
}

@ErrorHandling
class SlackConfigs extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      configs: this.props.configs,
    }
  }

  public componentWillReceiveProps(nextProps) {
    this.setState({configs: nextProps.configs})
  }

  public render() {
    const {configs} = this.state
    const {onSave, onTest, onDelete, onEnabled} = this.props

    return (
      <div>
        {configs.map(config => {
          const workspace = _.get(config, ['options', 'workspace'], 'new')
          const isNewConfig = _.get(config, 'isNewConfig', false)
          const enabled = onEnabled(workspace)

          return (
            <SlackConfig
              key={workspace}
              onSave={onSave}
              config={config}
              onTest={onTest}
              onDelete={onDelete}
              enabled={enabled}
              isNewConfig={isNewConfig}
            />
          )
        })}
        <div className="form-group col-xs-12 text-center">
          <button className="btn btn-md btn-default" onClick={this.addConfig}>
            <span className="icon plus" /> Add Another Config
          </button>
        </div>
      </div>
    )
  }

  private get configs() {
    return this.state.configs
  }

  private addConfig = () => {
    const configs = this.configs
    const newConfig = {
      options: {
        url: false,
        channel: '',
        workspace: '',
      },
      isNewConfig: true,
    }
    this.setState({configs: [...configs, newConfig]})
  }
}

export default SlackConfigs
