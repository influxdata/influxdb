import React, {PropTypes} from 'react'
import QuestionMarkTooltip from 'src/shared/components/QuestionMarkTooltip'
import {HIPCHAT_TOKEN_TIP} from 'src/kapacitor/copy'

const {
  bool,
  func,
  shape,
  string,
} = PropTypes

const HipchatConfig = React.createClass({
  propTypes: {
    config: shape({
      options: shape({
        room: string.isRequired,
        token: bool.isRequired,
        url: string.isRequired,
      }).isRequired,
    }).isRequired,
    onSave: func.isRequired,
  },

  handleSaveAlert(e) {
    e.preventDefault()

    const properties = {
      room: this.room.value,
      url: `https://${this.url.value}.hipchat.com/v2/room`,
      token: this.token.value,
    }

    this.props.onSave(properties)
  },

  render() {
    const {options} = this.props.config
    const {url, room, token} = options

    const subdomain = url.replace('https://', '').replace('.hipchat.com/v2/room', '')

    return (
      <form onSubmit={this.handleSaveAlert}>
        <div className="form-group col-xs-12">
          <label htmlFor="url">Subdomain</label>
          <input
            className="form-control"
            id="url"
            type="text"
            placeholder="your-subdomain"
            ref={(r) => this.url = r}
            defaultValue={subdomain && subdomain.length ? subdomain : ''}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="room">Room</label>
          <input
            className="form-control"
            id="room"
            type="text"
            placeholder="your-hipchat-room"
            ref={(r) => this.room = r}
            defaultValue={room || ''}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="token">
            Token
            <QuestionMarkTooltip
              tipID="token"
              tipContent={HIPCHAT_TOKEN_TIP}
            />
          </label>
          <input
            className="form-control"
            id="token"
            type="text"
            placeholder="your-hipchat-token"
            ref={(r) => this.token = r}
            defaultValue={token || ''}
          />
          <label className="form-helper">Note: a value of <code>true</code> indicates the HipChat token has been set</label>
        </div>

        <div className="form-group form-group-submit col-xs-12 col-sm-6 col-sm-offset-3">
          <button className="btn btn-block btn-primary" type="submit">Save</button>
        </div>
      </form>
    )
  },
})

export default HipchatConfig
