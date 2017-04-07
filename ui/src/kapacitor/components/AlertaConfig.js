import React, {PropTypes} from 'react'

const AlertaConfig = React.createClass({
  propTypes: {
    config: PropTypes.shape({
      options: PropTypes.shape({
        environment: PropTypes.string,
        origin: PropTypes.string,
        token: PropTypes.bool,
        url: PropTypes.string,
      }).isRequired,
    }).isRequired,
    onSave: PropTypes.func.isRequired,
  },

  handleSaveAlert(e) {
    e.preventDefault()

    const properties = {
      environment: this.environment.value,
      origin: this.origin.value,
      token: this.token.value,
      url: this.url.value,
    }

    this.props.onSave(properties)
  },

  render() {
    const {environment, origin, token, url} = this.props.config.options

    return (
      <div className="col-xs-12">
        <h4 className="text-center no-user-select">Alerta Alert</h4>
        <br/>
        <form onSubmit={this.handleSaveAlert}>
          <p className="no-user-select">
            Have alerts sent to Alerta
          </p>

          <div className="form-group col-xs-12">
            <label htmlFor="environment">Environment</label>
            <input className="form-control" id="environment" type="text" ref={(r) => this.environment = r} defaultValue={environment || ''}></input>
          </div>

          <div className="form-group col-xs-12">
            <label htmlFor="origin">Origin</label>
            <input className="form-control" id="origin" type="text" ref={(r) => this.origin = r} defaultValue={origin || ''}></input>
          </div>

          <div className="form-group col-xs-12">
            <label htmlFor="token">Token</label>
            <input className="form-control" id="token" type="text" ref={(r) => this.token = r} defaultValue={token || ''}></input>
            <span>Note: a value of <code>true</code> indicates the Alerta Token has been set</span>
          </div>

          <div className="form-group col-xs-12">
            <label htmlFor="url">User</label>
            <input className="form-control" id="url" type="text" ref={(r) => this.url = r} defaultValue={url || ''}></input>
          </div>

          <div className="form-group-submit col-xs-12 col-sm-6 col-sm-offset-3">
            <button className="btn btn-block btn-primary" type="submit">Save</button>
          </div>
        </form>
      </div>
    )
  },
})

export default AlertaConfig
