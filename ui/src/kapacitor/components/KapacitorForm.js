import React, {PropTypes} from 'react'
import AlertOutputs from './AlertOutputs'

const {
  func,
  shape,
  string,
  bool,
} = PropTypes

const KapacitorForm = React.createClass({
  propTypes: {
    onSubmit: func.isRequired,
    onInputChange: func.isRequired,
    onReset: func.isRequired,
    kapacitor: shape({
      url: string.isRequired,
      name: string.isRequired,
      username: string,
      password: string,
    }).isRequired,
    source: shape({}).isRequired,
    addFlashMessage: func.isRequired,
    exists: bool.isRequired,
  },

  render() {
    const {onInputChange, onReset, kapacitor, source, onSubmit} = this.props
    const {url, name, username, password} = kapacitor

    return (
      <div className="page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1>
                Configure Kapacitor
              </h1>
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-8 col-md-offset-2">
                <div className="panel panel-minimal">
                  <div className="panel-body">
                    <p className="no-user-select">
                      Kapacitor is used as the monitoring and alerting agent.
                      This page will let you configure which Kapacitor to use and
                      set up alert end points like email, Slack, and others.
                    </p>
                    <hr/>
                    <h4 className="text-center no-user-select">Connect Kapacitor to Source</h4>
                    <h4 className="text-center">{source.url}</h4>
                    <br/>
                    <form onSubmit={onSubmit}>
                      <div>
                        <div className="form-group col-xs-12 col-sm-8 col-sm-offset-2 col-md-4 col-md-offset-2">
                          <label htmlFor="url">Kapacitor URL</label>
                          <input
                            className="form-control"
                            id="url"
                            name="url"
                            placeholder={url}
                            value={url}
                            onChange={onInputChange}>
                          </input>
                        </div>
                        <div className="form-group col-xs-12 col-sm-8 col-sm-offset-2 col-md-4 col-md-offset-0">
                          <label htmlFor="name">Name</label>
                          <input
                            className="form-control"
                            id="name"
                            name="name"
                            placeholder={name}
                            value={name}
                            onChange={onInputChange}>
                          </input>
                        </div>
                        <div className="form-group col-xs-12 col-sm-4 col-sm-offset-2 col-md-4 col-md-offset-2">
                          <label htmlFor="username">Username</label>
                          <input
                            className="form-control"
                            id="username"
                            name="username"
                            placeholder="username"
                            value={username}
                            onChange={onInputChange}>
                          </input>
                        </div>
                        <div className="form-group col-xs-12 col-sm-4 col-md-4">
                          <label htmlFor="password">Password</label>
                          <input
                            className="form-control"
                            id="password"
                            type="password"
                            name="password"
                            placeholder="password"
                            value={password}
                            onChange={onInputChange}
                          />
                        </div>
                      </div>

                      <div className="form-group form-group-submit col-xs-12 text-center">
                        <button className="btn btn-info" type="button" onClick={onReset}>Reset to Default</button>
                        <button className="btn btn-success" type="submit">Connect Kapacitor</button>
                      </div>
                    </form>
                  </div>
                </div>
              </div>
            </div>
            <div className="row">
              <div className="col-md-8 col-md-offset-2">
                {this.renderAlertOutputs()}
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  },

  // TODO: move these to another page.  they dont belong on this page
  renderAlertOutputs() {
    const {exists, kapacitor, addFlashMessage, source} = this.props

    if (exists) {
      return <AlertOutputs source={source} kapacitor={kapacitor} addFlashMessage={addFlashMessage} />
    }

    return (
      <div className="panel panel-minimal">
        <div className="panel-body">
          <h4 className="text-center">Configure Alert Endpoints</h4>
          <br/>
          <p className="text-center">Set your Kapacitor connection info to configure alerting endpoints.</p>
        </div>
      </div>
    )
  },
})

export default KapacitorForm
