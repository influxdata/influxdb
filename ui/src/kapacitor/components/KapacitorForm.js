import React, {Component, PropTypes} from 'react'
import AlertTabs from './AlertTabs'

class KapacitorForm extends Component {
  render() {
    const {onInputChange, onReset, kapacitor, onSubmit} = this.props
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
              <div className="col-md-3">
                <div className="panel panel-minimal">
                  <div className="panel-heading u-flex u-ai-center u-jc-space-between">
                    <h2 className="panel-title">Connection Details</h2>
                  </div>
                  <div className="panel-body">
                    <form onSubmit={onSubmit}>
                      <div>
                        <div className="form-group">
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
                        <div className="form-group">
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
                        <div className="form-group">
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
                        <div className="form-group">
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
                        <button className="btn btn-info" type="button" onClick={onReset}>Reset</button>
                        <button className="btn btn-success" type="submit">Connect</button>
                      </div>
                    </form>
                  </div>
                </div>
              </div>
              <div className="col-md-9">
                {this.renderAlertOutputs()}
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  }

  // TODO: move these to another page.  they dont belong on this page
  renderAlertOutputs() {
    const {exists, kapacitor, addFlashMessage, source} = this.props

    if (exists) {
      return <AlertTabs source={source} kapacitor={kapacitor} addFlashMessage={addFlashMessage} />
    }

    return (
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">Configure Alert Endpoints</h2>
        </div>
        <div className="panel-body">
          <br/>
          <p className="text-center">Set your Kapacitor connection info to configure alerting endpoints.</p>
        </div>
      </div>
    )
  }
}

const {
  func,
  shape,
  string,
  bool,
} = PropTypes

KapacitorForm.propTypes = {
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
}

export default KapacitorForm
