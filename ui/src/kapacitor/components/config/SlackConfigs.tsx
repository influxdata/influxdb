import React, {PureComponent, MouseEvent} from 'react'
import {get} from 'src/utils/wrappers'

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
  onTest: (
    e: MouseEvent<HTMLButtonElement>,
    specificConfigOptions: Partial<SlackProperties>
  ) => void
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
    const {onSave, onTest, onEnabled} = this.props

    return (
      <div>
        {configs.map(config => {
          const workspace = this.getWorkspace(config)
          const isNewConfig = this.isNewConfig(config)
          const enabled = onEnabled(workspace)
          const isDefaultConfig = this.isDefaultConfig(config)
          const workspaceID = this.getWorkspaceID(config)

          return (
            <SlackConfig
              key={workspace}
              onSave={onSave}
              config={config}
              onTest={onTest}
              onDelete={this.deleteConfig}
              enabled={enabled}
              isNewConfig={isNewConfig}
              isDefaultConfig={isDefaultConfig}
              workspaceID={workspaceID}
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

  private isNewConfig = (config: Config): boolean => {
    return get(config, 'isNewConfig', false)
  }

  private isDefaultConfig = (config: Config): boolean => {
    return this.getWorkspace(config) === '' && !this.isNewConfig(config)
  }

  private getWorkspace = (config: Config): string => {
    return get(config, 'options.workspace', 'new')
  }

  private getWorkspaceID = (config: Config): string => {
    if (this.isDefaultConfig(config)) {
      return 'default'
    }

    if (this.isNewConfig(config)) {
      return 'new'
    }

    return this.getWorkspace(config)
  }

  private addConfig = () => {
    const configs = this.configs
    const newConfig = {
      options: {
        url: false,
        channel: '',
        workspace: null,
      },
      isNewConfig: true,
    }
    this.setState({configs: [...configs, newConfig]})
  }

  private deleteConfig = (
    specificConfig: string,
    workspaceID: string
  ): void => {
    if (workspaceID === 'new') {
      const configs = this.configs.filter(
        c => this.getWorkspaceID(c) !== workspaceID
      )
      this.setState({configs})
    } else {
      this.props.onDelete(specificConfig)
    }
  }
}

export default SlackConfigs
