import React, {PropTypes} from 'react'

const HipchatConfig = React.createClass({
  propTypes: {
    config: PropTypes.shape({
      options: PropTypes.shape({
        room: PropTypes.string.isRequired,
        token: PropTypes.bool.isRequired,
        url: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
    onSave: PropTypes.func.isRequired,
  },

  handleSaveAlert(e) {
    e.preventDefault()

    const properties = {
      room: this.room.value,
      url: this.url.value,
      token: this.token.value,
    }

    this.props.onSave(properties)
  },

  render() {
    const {options} = this.props.config
    const {url, room, token} = options

    return (
      <div>
        <h4 className="text-center no-user-select">HipChat Alert</h4>
        <br/>
        <p className="no-user-select">Have alerts sent to HipChat.</p>
        <form onSubmit={this.handleSaveAlert}>
          <div className="form-group col-xs-12">
            <label htmlFor="url">HipChat URL</label>
            <input className="form-control" id="url" type="text" ref={(r) => this.url = r} defaultValue={url || ''}></input>
          </div>

          <div className="form-group col-xs-12">
            <label htmlFor="room">Room</label>
            <input className="form-control" id="room" type="text" ref={(r) => this.room = r} defaultValue={room || ''}></input>
          </div>

          <div className="form-group col-xs-12">
            <label htmlFor="token">Token</label>
            <input className="form-control" id="token" type="text" ref={(r) => this.token = r} defaultValue={token || ''}></input>
            <label className="form-helper">Note: a value of <code>true</code> indicates the HipChat token has been set</label>
          </div>

          <div className="form-group form-group-submit col-xs-12 col-sm-6 col-sm-offset-3">
            <button className="btn btn-block btn-primary" type="submit">Save</button>
          </div>
        </form>
      </div>
    )
  },
})

export default HipchatConfig
