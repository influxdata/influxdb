import React, {Component, PropTypes} from 'react'

import AlertTabs from 'src/kapacitor/components/AlertTabs'
import FancyScrollbar from 'shared/components/FancyScrollbar'

class KapacitorForm extends Component {
  render() {
    const {onInputChange, onReset, kapacitor, onSubmit, exists} = this.props
    const {url: kapaUrl, name, username, password} = kapacitor
    return (
      <div className="page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">{`${exists
                ? 'Configure'
                : 'Add a New'} Kapacitor Connection`}</h1>
            </div>
          </div>
        </div>
        <FancyScrollbar className="page-contents">
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
                          <label htmlFor="kapaUrl">Kapacitor URL</label>
                          <input
                            className="form-control"
                            id="kapaUrl"
                            name="kapaUrl"
                            placeholder={kapaUrl}
                            value={kapaUrl}
                            onChange={onInputChange}
                            spellCheck="false"
                          />
                        </div>
                        <div className="form-group">
                          <label htmlFor="name">Name</label>
                          <input
                            className="form-control"
                            id="name"
                            name="name"
                            placeholder={name}
                            value={name}
                            onChange={onInputChange}
                            spellCheck="false"
                            maxLength="33"
                          />
                        </div>
                        <div className="form-group">
                          <label htmlFor="username">Username</label>
                          <input
                            className="form-control"
                            id="username"
                            name="username"
                            placeholder="username"
                            value={username || ''}
                            onChange={onInputChange}
                            spellCheck="false"
                          />
                        </div>
                        <div className="form-group">
                          <label htmlFor="password">Password</label>
                          <input
                            className="form-control"
                            id="password"
                            type="password"
                            name="password"
                            placeholder="password"
                            value={password || ''}
                            onChange={onInputChange}
                            spellCheck="false"
                          />
                        </div>
                      </div>

                      <div className="form-group form-group-submit col-xs-12 text-center">
                        <button
                          className="btn btn-default"
                          type="button"
                          onClick={onReset}
                        >
                          Reset
                        </button>
                        <button className="btn btn-success" type="submit">
                          {exists ? 'Update' : 'Connect'}
                        </button>
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
        </FancyScrollbar>
      </div>
    )
  }

  // TODO: move these to another page.  they dont belong on this page
  renderAlertOutputs() {
    const {
      exists,
      kapacitor,
      addFlashMessage,
      source,
      routerLocation,
    } = this.props

    if (exists) {
      return (
        <AlertTabs
          source={source}
          kapacitor={kapacitor}
          addFlashMessage={addFlashMessage}
          routerLocation={routerLocation}
        />
      )
    }

    return (
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">Configure Alert Endpoints</h2>
        </div>
        <div className="panel-body">
          <div className="generic-empty-state">
            <h4 className="no-user-select">
              Connect to an active Kapacitor instance to configure alerting
              endpoints
            </h4>
          </div>
        </div>
      </div>
    )
  }
}

const {func, shape, string, bool} = PropTypes

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
  routerLocation: shape({pathname: string, hash: string}).isRequired,
}

export default KapacitorForm
