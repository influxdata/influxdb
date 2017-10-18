import React, {PropTypes, Component} from 'react'
import classnames from 'classnames'
import {insecureSkipVerifyText} from 'shared/copy/tooltipText'
import _ from 'lodash'

class SourceForm extends Component {
  constructor(props) {
    super(props)
  }

  handleBlurSourceURL = () => {
    const url = this.props.source.url.trim()

    if (!url) {
      return
    }

    const newSource = {
      ...this.props.source,
      url,
    }

    this.props.onBlurSourceURL(newSource)
  }

  render() {
    const {source, editMode, onSubmit, onInputChange} = this.props

    return (
      <div className="panel-body">
        <h4 className="text-center">Connection Details</h4>
        <br />

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
              onBlur={this.handleBlurSourceURL}
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
          {_.get(source, 'type', '').includes('enterprise')
            ? <div className="form-group col-xs-12">
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
            : null}
          <div className="form-group col-xs-12">
            <label htmlFor="telegraf">Telegraf database</label>
            <input
              type="text"
              name="telegraf"
              className="form-control"
              id="telegraf"
              onChange={onInputChange}
              value={source.telegraf}
            />
          </div>
          <div className="form-group col-xs-12">
            <div className="form-control-static">
              <input
                type="checkbox"
                id="defaultSourceCheckbox"
                name="default"
                checked={source.default}
                onChange={onInputChange}
              />
              <label htmlFor="defaultSourceCheckbox">
                Make this the default source
              </label>
            </div>
          </div>
          {_.get(source, 'url', '').startsWith('https')
            ? <div className="form-group col-xs-12">
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
                <label className="form-helper">
                  {insecureSkipVerifyText}
                </label>
              </div>
            : null}
          <div className="form-group form-group-submit col-xs-12 col-sm-6 col-sm-offset-3 text-center">
            <button
              className={classnames('btn btn-block', {
                'btn-primary': editMode,
                'btn-success': !editMode,
              })}
              type="submit"
            >
              {editMode ? 'Save Changes' : 'Add Source'}
            </button>
            <br />
          </div>
        </form>
      </div>
    )
  }
}

const {bool, func, shape, string} = PropTypes

SourceForm.propTypes = {
  source: shape({
    url: string.isRequired,
    name: string.isRequired,
    username: string.isRequired,
    password: string.isRequired,
    telegraf: string.isRequired,
    insecureSkipVerify: bool.isRequired,
    default: bool.isRequired,
    metaUrl: string.isRequired,
  }).isRequired,
  editMode: bool.isRequired,
  onInputChange: func.isRequired,
  onSubmit: func.isRequired,
  onBlurSourceURL: func.isRequired,
}

export default SourceForm
