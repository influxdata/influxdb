import React, {PureComponent} from 'react'

import TagInput from 'src/shared/components/TagInput'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {Notification, NotificationFunc} from 'src/types'

import {KafkaProperties} from 'src/types/kapacitor'
import {notifyInvalidBatchSizeValue} from 'src/shared/copy/notifications'

interface Config {
  options: KafkaProperties & {
    id: string
  }
}

interface Item {
  name?: string
}

interface Props {
  config: Config
  onSave: (properties: KafkaProperties) => void
  onTest: (event: React.MouseEvent<HTMLButtonElement>) => void
  enabled: boolean
  notify: (message: Notification | NotificationFunc) => void
}

interface State {
  currentBrokers: string[]
  testEnabled: boolean
}

@ErrorHandling
class KafkaConfig extends PureComponent<Props, State> {
  private id: HTMLInputElement
  private timeout: HTMLInputElement
  private batchSize: HTMLInputElement
  private batchTimeout: HTMLInputElement
  private useSSL: HTMLInputElement
  private sslCA: HTMLInputElement
  private sslCert: HTMLInputElement
  private sslKey: HTMLInputElement
  private insecureSkipVerify: HTMLInputElement

  constructor(props) {
    super(props)

    const {brokers} = props.config.options

    this.state = {
      currentBrokers: brokers || [],
      testEnabled: this.props.enabled,
    }
  }

  public render() {
    const {options} = this.props.config
    const id = options.id
    const timeout = options.timeout
    const batchSize = options['batch-size']
    const batchTimeout = options['batch-timeout']
    const useSSL = options['use-ssl']
    const sslCA = options['ssl-ca']
    const sslCert = options['ssl-cert']
    const sslKey = options['ssl-key']
    const insecureSkipVerify = options['insecure-skip-verify']

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12">
          <label htmlFor="id">ID</label>
          <input
            className="form-control"
            id="id"
            type="text"
            ref={r => (this.id = r)}
            defaultValue={id || ''}
            onChange={this.disableTest}
            readOnly={true}
          />
        </div>
        <TagInput
          title="Brokers"
          onAddTag={this.handleAddBroker}
          onDeleteTag={this.handleDeleteBroker}
          tags={this.currentBrokersForTags}
          disableTest={this.disableTest}
        />
        <div className="form-group col-xs-12">
          <label htmlFor="timeout">Timeout</label>
          <input
            className="form-control"
            id="timeout"
            type="text"
            ref={r => (this.timeout = r)}
            defaultValue={timeout || ''}
            onChange={this.disableTest}
          />
        </div>
        <div className="form-group col-xs-12">
          <label htmlFor="batchSize">Batch Size</label>
          <input
            className="form-control"
            id="batchSize"
            type="number"
            ref={r => (this.batchSize = r)}
            defaultValue={batchSize.toString() || '0'}
            onChange={this.disableTest}
          />
        </div>
        <div className="form-group col-xs-12">
          <label htmlFor="batchTimeout">Batch Timeout</label>
          <input
            className="form-control"
            id="batchTimeout"
            type="text"
            ref={r => (this.batchTimeout = r)}
            defaultValue={batchTimeout || ''}
            onChange={this.disableTest}
          />
        </div>
        <div className="form-group col-xs-12">
          <div className="form-control-static">
            <input
              id="useSSL"
              type="checkbox"
              defaultChecked={useSSL}
              ref={r => (this.useSSL = r)}
              onChange={this.disableTest}
            />
            <label htmlFor="useSSL">Use SSL</label>
          </div>
        </div>
        <div className="form-group col-xs-12">
          <label htmlFor="sslCA">SSL CA</label>
          <input
            className="form-control"
            id="sslCA"
            type="text"
            ref={r => (this.sslCA = r)}
            defaultValue={sslCA || ''}
            onChange={this.disableTest}
          />
        </div>
        <div className="form-group col-xs-12">
          <label htmlFor="sslCert">SSL Cert</label>
          <input
            className="form-control"
            id="sslCert"
            type="text"
            ref={r => (this.sslCert = r)}
            defaultValue={sslCert || ''}
            onChange={this.disableTest}
          />
        </div>
        <div className="form-group col-xs-12">
          <label htmlFor="sslKey">SSL Key</label>
          <input
            className="form-control"
            id="sslKey"
            type="text"
            ref={r => (this.sslKey = r)}
            defaultValue={sslKey || ''}
            onChange={this.disableTest}
          />
        </div>
        <div className="form-group col-xs-12">
          <div className="form-control-static">
            <input
              id="insecureSkipVerify"
              type="checkbox"
              defaultChecked={insecureSkipVerify}
              ref={r => (this.insecureSkipVerify = r)}
              onChange={this.disableTest}
            />
            <label htmlFor="insecureSkipVerify">Insecure Skip Verify</label>
          </div>
        </div>
        <div className="form-group form-group-submit col-xs-12 text-center">
          <button
            className="btn btn-primary"
            type="submit"
            disabled={this.state.testEnabled}
          >
            <span className="icon checkmark" />
            Save Changes
          </button>
          <button
            className="btn btn-primary"
            disabled={!this.state.testEnabled}
            onClick={this.props.onTest}
          >
            <span className="icon pulse-c" />
            Send Test Alert
          </button>
        </div>
      </form>
    )
  }

  private get currentBrokersForTags(): Item[] {
    const {currentBrokers} = this.state
    return currentBrokers.map(broker => ({name: broker}))
  }

  private handleSubmit = async e => {
    e.preventDefault()

    const batchSize = parseInt(this.batchSize.value, 10)
    if (isNaN(batchSize)) {
      this.props.notify(notifyInvalidBatchSizeValue())
      return
    }

    const properties = {
      brokers: this.state.currentBrokers,
      timeout: this.timeout.value,
      'batch-size': batchSize,
      'batch-timeout': this.batchTimeout.value,
      'use-ssl': this.useSSL.checked,
      'ssl-ca': this.sslCA.value,
      'ssl-cert': this.sslCert.value,
      'ssl-key': this.sslKey.value,
      'insecure-skip-verify': this.insecureSkipVerify.checked,
    }

    const success = await this.props.onSave(properties)
    if (success) {
      this.setState({testEnabled: true})
    }
  }

  private disableTest = () => {
    this.setState({testEnabled: false})
  }

  private handleAddBroker = broker => {
    this.setState({currentBrokers: this.state.currentBrokers.concat(broker)})
  }

  private handleDeleteBroker = broker => {
    this.setState({
      currentBrokers: this.state.currentBrokers.filter(t => t !== broker.name),
      testEnabled: false,
    })
  }
}

export default KafkaConfig
