import React, {PureComponent, MouseEvent, ChangeEvent} from 'react'

import TagInput from 'src/shared/components/TagInput'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {Notification, NotificationFunc} from 'src/types'

import {KafkaProperties} from 'src/types/kapacitor'
import {notifyInvalidBatchSizeValue} from 'src/shared/copy/notifications'

import {get} from 'src/utils/wrappers'

interface Config {
  options: KafkaProperties
  isNewConfig?: boolean
}

interface Item {
  name?: string
}

interface Props {
  config: Config
  onSave: (
    properties: KafkaProperties,
    isNewConfig: boolean,
    specificConfig: string
  ) => void
  onTest: (
    event: React.MouseEvent<HTMLButtonElement>,
    specificConfigOptions: Partial<KafkaProperties>
  ) => void
  enabled: boolean
  notify: (message: Notification | NotificationFunc) => void
  id: string
  onDelete: (specificConfig: string) => void
}

interface State {
  currentBrokers: string[]
  testEnabled: boolean
  enabled: boolean
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
      enabled: get(this.props, 'config.options.enabled', false),
    }
  }

  public render() {
    const {options} = this.props.config
    const {id: keyID} = this.props
    const id = options.id
    const timeout = options.timeout
    const batchSize = options['batch-size']
    const batchTimeout = options['batch-timeout']
    const useSSL = options['use-ssl']
    const sslCA = options['ssl-ca']
    const sslCert = options['ssl-cert']
    const sslKey = options['ssl-key']
    const insecureSkipVerify = options['insecure-skip-verify']
    const {enabled} = this.state

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12">
          <label htmlFor={`${keyID}-id`}>ID</label>
          <input
            className="form-control"
            id={`${keyID}-id`}
            type="text"
            ref={r => (this.id = r)}
            defaultValue={id || ''}
            onChange={this.disableTest}
            readOnly={!this.isNewConfig}
          />
        </div>
        <TagInput
          title="Brokers"
          inputID={`${keyID}-brokers`}
          onAddTag={this.handleAddBroker}
          onDeleteTag={this.handleDeleteBroker}
          tags={this.currentBrokersForTags}
          disableTest={this.disableTest}
        />
        <div className="form-group col-xs-12">
          <label htmlFor={`${keyID}-timeout`}>Timeout</label>
          <input
            className="form-control"
            id={`${keyID}-timeout`}
            type="text"
            ref={r => (this.timeout = r)}
            defaultValue={timeout || ''}
            onChange={this.disableTest}
          />
        </div>
        <div className="form-group col-xs-12">
          <label htmlFor={`${keyID}-batchsize`}>Batch Size</label>
          <input
            className="form-control"
            id={`${keyID}-batchsize`}
            type="number"
            ref={r => (this.batchSize = r)}
            defaultValue={batchSize.toString() || '0'}
            onChange={this.disableTest}
          />
        </div>
        <div className="form-group col-xs-12">
          <label htmlFor={`${keyID}-batch-timeout`}>Batch Timeout</label>
          <input
            className="form-control"
            id={`${keyID}-batch-timeout`}
            type="text"
            ref={r => (this.batchTimeout = r)}
            defaultValue={batchTimeout || ''}
            onChange={this.disableTest}
          />
        </div>
        <div className="form-group col-xs-12">
          <div className="form-control-static">
            <input
              id={`${keyID}-useSSL`}
              type="checkbox"
              defaultChecked={useSSL}
              ref={r => (this.useSSL = r)}
              onChange={this.disableTest}
            />
            <label htmlFor={`${keyID}-useSSL`}>Use SSL</label>
          </div>
        </div>
        <div className="form-group col-xs-12">
          <label htmlFor={`${keyID}-sslCA`}>SSL CA</label>
          <input
            className="form-control"
            id={`${keyID}-sslCA`}
            type="text"
            ref={r => (this.sslCA = r)}
            defaultValue={sslCA || ''}
            onChange={this.disableTest}
          />
        </div>
        <div className="form-group col-xs-12">
          <label htmlFor={`${keyID}-sslCert`}>SSL Cert</label>
          <input
            className="form-control"
            id={`${keyID}-sslCert`}
            type="text"
            ref={r => (this.sslCert = r)}
            defaultValue={sslCert || ''}
            onChange={this.disableTest}
          />
        </div>
        <div className="form-group col-xs-12">
          <label htmlFor={`${keyID}-sslKey`}>SSL Key</label>
          <input
            className="form-control"
            id={`${keyID}-sslKey`}
            type="text"
            ref={r => (this.sslKey = r)}
            defaultValue={sslKey || ''}
            onChange={this.disableTest}
          />
        </div>
        <div className="form-group col-xs-12">
          <div className="form-control-static">
            <input
              id={`${keyID}-insecureSkipVerify`}
              type="checkbox"
              defaultChecked={insecureSkipVerify}
              ref={r => (this.insecureSkipVerify = r)}
              onChange={this.disableTest}
            />
            <label htmlFor={`${keyID}-insecureSkipVerify`}>
              Insecure Skip Verify
            </label>
          </div>
        </div>
        <div className="form-group col-xs-12">
          <div className="form-control-static">
            <input
              type="checkbox"
              id={`${keyID}-enabled`}
              checked={enabled}
              onChange={this.handleEnabledChange}
            />
            <label htmlFor={`${keyID}-enabled`}>Configuration Enabled</label>
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
            onClick={this.handleTest}
          >
            <span className="icon pulse-c" />
            Send Test Alert
          </button>
          {this.deleteButton}
          <hr />
        </div>
      </form>
    )
  }

  private get currentBrokersForTags(): Item[] {
    const {currentBrokers} = this.state
    return currentBrokers.map(broker => ({name: broker}))
  }

  private get configID(): string {
    const {
      config: {
        options: {id},
      },
    } = this.props
    return id
  }

  private get isNewConfig(): boolean {
    return get(this.props, 'config.isNewConfig', false)
  }

  private get isDefaultConfig(): boolean {
    return this.configID === 'default'
  }

  private get deleteButton(): JSX.Element {
    if (this.isDefaultConfig) {
      return null
    }

    return (
      <button className="btn btn-danger" onClick={this.handleDelete}>
        <span className="icon trash" />
        Delete
      </button>
    )
  }

  private handleSubmit = async e => {
    e.preventDefault()

    const batchSize = parseInt(this.batchSize.value, 10)
    if (isNaN(batchSize)) {
      this.props.notify(notifyInvalidBatchSizeValue())
      return
    }

    const properties: KafkaProperties = {
      brokers: this.state.currentBrokers,
      timeout: this.timeout.value,
      'batch-size': batchSize,
      'batch-timeout': this.batchTimeout.value,
      'use-ssl': this.useSSL.checked,
      'ssl-ca': this.sslCA.value,
      'ssl-cert': this.sslCert.value,
      'ssl-key': this.sslKey.value,
      'insecure-skip-verify': this.insecureSkipVerify.checked,
      enabled: this.state.enabled,
    }

    if (this.isNewConfig) {
      properties.id = this.id.value
    }

    const success = await this.props.onSave(
      properties,
      this.isNewConfig,
      this.configID
    )
    if (success) {
      this.setState({testEnabled: true})
    }
  }

  private handleEnabledChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({enabled: e.target.checked})
    this.disableTest()
  }

  private handleTest = (e: MouseEvent<HTMLButtonElement>): void => {
    this.props.onTest(e, {id: this.configID})
  }

  private handleDelete = (e: MouseEvent<HTMLButtonElement>): void => {
    e.preventDefault()
    this.props.onDelete(this.configID)
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
