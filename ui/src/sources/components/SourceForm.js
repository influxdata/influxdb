import React, {PropTypes} from 'react'
import classnames from 'classnames'
import {connect} from 'react-redux'

import {insecureSkipVerifyText} from 'shared/copy/tooltipText'
import _ from 'lodash'

const SourceForm = ({
  source,
  editMode,
  onSubmit,
  onInputChange,
  onBlurSourceURL,
  isUsingAuth,
  me,
}) =>
  <div className="panel-body">
    {isUsingAuth
      ? <div className="text-center">
          <h3>
            No sources connected to{' '}
            <strong>{me.currentOrganization.name}</strong>
          </h3>
          <h6>Add a Source below:</h6>
        </div>
      : <h4 className="text-center">Connection Details</h4>}

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
  me: shape({
    currentOrganization: shape({
      id: string.isRequired,
      name: string.isRequired,
    }),
  }),
  isUsingAuth: bool,
}

const mapStateToProps = ({auth: {isUsingAuth, me}}) => ({isUsingAuth, me})

export default connect(mapStateToProps)(SourceForm)
