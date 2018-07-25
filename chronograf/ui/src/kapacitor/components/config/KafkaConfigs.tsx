import React, {Component, MouseEvent} from 'react'
import _ from 'lodash'

import KafkaConfig from 'src/kapacitor/components/config/KafkaConfig'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {KafkaProperties} from 'src/types/kapacitor'
import {Notification, NotificationFunc} from 'src/types'

import {getDeep} from 'src/utils/wrappers'

const DEFAULT_CONFIG = {
  options: {
    id: '',
    brokers: [],
    timeout: '',
    'batch-size': 0,
    'batch-timeout': '',
    'use-ssl': false,
    'ssl-ca': '',
    'ssl-cert': '',
    'ssl-key': '',
    'insecure-skip-verify': false,
    enabled: false,
  },
  isNewConfig: true,
}

interface Config {
  options: KafkaProperties & {
    id: string
  }
  isNewConfig?: boolean
}

interface Props {
  configs: Config[]
  onSave: (properties: KafkaProperties) => void
  onDelete: (specificConfig: string) => void
  onTest: (
    e: MouseEvent<HTMLButtonElement>,
    specificConfigOptions: Partial<KafkaProperties> & {id: string}
  ) => void
  onEnabled: (specificConfig: string) => boolean
  notify: (message: Notification | NotificationFunc) => void
  isMultipleConfigsSupported: boolean
}

interface State {
  configs: Config[]
}

@ErrorHandling
class KafkaConfigs extends Component<Props, State> {
  public static getDerivedStateFromProps(nextProps: Props, prevState: State) {
    return {...prevState, configs: nextProps.configs}
  }
  public state: State = {configs: []}

  public render() {
    const {onSave, onDelete, onTest, notify} = this.props

    return (
      <div>
        {this.configs.map(c => {
          const enabled = getDeep<boolean>(c, 'options.enabled', false)
          const id = getDeep<string>(c, 'options.id', '')
          return (
            <KafkaConfig
              config={c}
              onSave={onSave}
              onTest={onTest}
              onDelete={onDelete}
              enabled={enabled}
              notify={notify}
              key={id}
              id={id}
            />
          )
        })}
        {this.isAddingConfigsAllowed && (
          <div className="form-group col-xs-12 text-center">
            <button
              className="btn btn-md btn-default"
              onClick={this.handleAddConfig}
            >
              <span className="icon plus" /> Add Another Config
            </button>
          </div>
        )}
      </div>
    )
  }
  private get configs(): Config[] {
    return _.sortBy(this.state.configs, c => {
      const id = getDeep<string>(c, 'options.id', '')
      const {isNewConfig} = c
      if (id === 'default') {
        return ''
      }
      if (isNewConfig) {
        return Infinity
      }
      return id
    })
  }

  private get isAddingConfigsAllowed() {
    const {isMultipleConfigsSupported} = this.props
    const isAllConfigsPersisted = _.every(this.configs, c => !c.isNewConfig)
    return isMultipleConfigsSupported && isAllConfigsPersisted
  }

  private handleAddConfig = (): void => {
    const {configs} = this.state
    const newConfig: Config = DEFAULT_CONFIG
    const updatedConfigs = [...configs, newConfig]
    this.setState({configs: updatedConfigs})
  }
}

export default KafkaConfigs
