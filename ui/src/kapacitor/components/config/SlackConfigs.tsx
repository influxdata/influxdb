import React, {PureComponent} from 'react'
import _ from 'lodash'

import {ErrorHandling} from 'src/shared/decorators/errors'
import SlackConfig from 'src/kapacitor/components/config/SlackConfig'

interface Properties {
  channel: string
  url: string
  workspace?: string
}

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
    properties: Properties,
    isNewConfigInSection: boolean,
    specificConfig: string
  ) => void
  onDelete: (specificConfig: string) => void
  onTest: (event: React.MouseEvent<HTMLButtonElement>) => void
  onEnabled: (specificConfig: string) => boolean
}

interface State {
  configs: any[]
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
        <button className="btn btn-md btn-default" onClick={this.addConfig}>
          <span className="icon plus" /> Add Another Config
        </button>
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
      },
      isNewConfig: true,
    }
    this.setState({configs: [...configs, newConfig]})
  }
}

export default SlackConfigs
