import React, {PureComponent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'
import SlackConfig from 'src/kapacitor/components/config/SlackConfig'

interface Properties {
  channel: string
  url: string
}

interface Config {
  options: {
    url: boolean
    channel: string
  }
}

interface Props {
  slackConfigs: any[]
  config: Config
  onSave: (properties: Properties) => void
  onTest: (event: React.MouseEvent<HTMLButtonElement>) => void
  enabled: boolean
}

interface State {
  slackConfigs: any[]
}

@ErrorHandling
class SlackConfigs extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      slackConfigs: this.props.slackConfigs,
    }
  }

  public render() {
    const {slackConfigs} = this.state
    const {onSave, onTest, enabled} = this.props
    return (
      <div>
        {slackConfigs.map((config, i) => (
          <SlackConfig
            key={i}
            onSave={onSave}
            config={config}
            onTest={onTest}
            enabled={enabled}
          />
        ))}
        <button className="btn btn-md btn-default" onClick={this.addConfig}>
          <span className="icon plus" /> Add Another Config
        </button>
      </div>
    )
  }

  private get slackConfigs() {
    return this.state.slackConfigs
  }

  private addConfig = () => {
    const configs = this.slackConfigs
    const newConfig = {
      options: {
        url: false,
        channel: '',
      },
    }
    this.setState({slackConfigs: [...configs, newConfig]})
  }
}

export default SlackConfigs
