import React, {PureComponent, FocusEvent, MouseEvent, ChangeEvent} from 'react'
import classnames from 'classnames'
import {connect} from 'react-redux'
import _ from 'lodash'

import {insecureSkipVerifyText} from 'src/shared/copy/tooltipText'

import {SUPERADMIN_ROLE} from 'src/auth/Authorized'
import {Source, Me} from 'src/types'

interface Props {
  me: Me
  source: Partial<Source>
  editMode: boolean
  isUsingAuth: boolean
  gotoPurgatory: () => void
  isInitialSource: boolean
  onSubmit: (e: MouseEvent<HTMLFormElement>) => void
  onInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  onBlurSourceURL: (e: FocusEvent<HTMLInputElement>) => void
}

export class SourceForm extends PureComponent<Props> {
  public render() {
    const {
      source,
      onSubmit,
      isUsingAuth,
      onInputChange,
      gotoPurgatory,
      onBlurSourceURL,
      isInitialSource,
    } = this.props
    return (
      <div className="panel-body">
        {isUsingAuth && isInitialSource && this.authIndicatior}
        <form onSubmit={onSubmit}>
          <div className="form-group col-xs-12 col-sm-6">
            <label htmlFor="connect-string">Connection String</label>
            <input
              type="text"
              name="url"
              className="form-control"
              id="connect-string"
              placeholder="Address of InfluxDB"
              onChange={onInputChange}
              value={source.url}
              onBlur={onBlurSourceURL}
              required={true}
            />
          </div>
          <div className="form-group col-xs-12 col-sm-6">
            <label htmlFor="name">Name</label>
            <input
              type="text"
              name="name"
              className="form-control"
              id="name"
              placeholder="Name this source"
              onChange={onInputChange}
              value={source.name}
              required={true}
            />
          </div>
          <div className="form-group col-xs-12 col-sm-6">
            <label htmlFor="username">Username</label>
            <input
              type="text"
              name="username"
              className="form-control"
              id="username"
              onChange={onInputChange}
              value={source.username}
            />
          </div>
          <div className="form-group col-xs-12 col-sm-6">
            <label htmlFor="password">Password</label>
            <input
              type="password"
              name="password"
              className="form-control"
              id="password"
              onChange={onInputChange}
              value={source.password}
            />
          </div>
          {this.isEnterprise && (
            <div className="form-group col-xs-12">
              <label htmlFor="meta-url">Meta Service Connection URL</label>
              <input
                type="text"
                name="metaUrl"
                className="form-control"
                id="meta-url"
                placeholder="http://localhost:8091"
                onChange={onInputChange}
                value={source.metaUrl}
              />
            </div>
          )}
          <div className="form-group col-xs-12 col-sm-6">
            <label htmlFor="telegraf">Telegraf Database</label>
            <input
              type="text"
              name="telegraf"
              className="form-control"
              id="telegraf"
              onChange={onInputChange}
              value={source.telegraf}
            />
          </div>
          <div className="form-group col-xs-12 col-sm-6">
            <label htmlFor="defaultRP">Default Retention Policy</label>
            <input
              type="text"
              name="defaultRP"
              className="form-control"
              id="defaultRP"
              onChange={onInputChange}
              value={source.defaultRP}
            />
          </div>
          <div className="form-group col-xs-12">
            <div className="form-control-static">
              <input
                type="checkbox"
                id="defaultConnectionCheckbox"
                name="default"
                checked={source.default}
                onChange={onInputChange}
              />
              <label htmlFor="defaultConnectionCheckbox">
                Make this the default connection
              </label>
            </div>
          </div>
          {this.isHTTPS && (
            <div className="form-group col-xs-12">
              <div className="form-control-static">
                <input
                  type="checkbox"
                  id="insecureSkipVerifyCheckbox"
                  name="insecureSkipVerify"
                  checked={source.insecureSkipVerify}
                  onChange={onInputChange}
                />
                <label htmlFor="insecureSkipVerifyCheckbox">Unsafe SSL</label>
              </div>
              <label className="form-helper">{insecureSkipVerifyText}</label>
            </div>
          )}
          <div className="form-group form-group-submit text-center col-xs-12 col-sm-6 col-sm-offset-3">
            <button className={this.submitClass} type="submit">
              <span className={this.submitIconClass} />
              {this.submitText}
            </button>

            <br />
            {isUsingAuth && (
              <button className="btn btn-link btn-sm" onClick={gotoPurgatory}>
                <span className="icon shuffle" /> Switch Orgs
              </button>
            )}
          </div>
        </form>
      </div>
    )
  }

  private get authIndicatior(): JSX.Element {
    const {me} = this.props
    return (
      <div className="text-center">
        {me.role.name === SUPERADMIN_ROLE ? (
          <h3>
            <strong>{me.currentOrganization.name}</strong> has no connections
          </h3>
        ) : (
          <h3>
            <strong>{me.currentOrganization.name}</strong> has no connections
            available to <em>{me.role}s</em>
          </h3>
        )}
        <h6>Add a Connection below:</h6>
      </div>
    )
  }

  private get submitText(): string {
    const {editMode} = this.props
    if (editMode) {
      return 'Save Changes'
    }

    return 'Add Connection'
  }

  private get submitIconClass(): string {
    const {editMode} = this.props
    return `icon ${editMode ? 'checkmark' : 'plus'}`
  }

  private get submitClass(): string {
    const {editMode} = this.props
    return classnames('btn btn-block', {
      'btn-primary': editMode,
      'btn-success': !editMode,
    })
  }

  private get isEnterprise(): boolean {
    const {source} = this.props
    return _.get(source, 'type', '').includes('enterprise')
  }

  private get isHTTPS(): boolean {
    const {source} = this.props
    return _.get(source, 'url', '').startsWith('https')
  }
}

const mapStateToProps = ({auth: {isUsingAuth, me}}) => ({isUsingAuth, me})

export default connect(mapStateToProps)(SourceForm)
