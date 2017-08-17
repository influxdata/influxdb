import React, {PropTypes, Component} from 'react'

import QuestionMarkTooltip from 'shared/components/QuestionMarkTooltip'
import {HIPCHAT_TOKEN_TIP} from 'src/kapacitor/copy'
import RedactedInput from './RedactedInput'

class HipchatConfig extends Component {
  constructor(props) {
    super(props)
  }

  handleSaveAlert = e => {
    e.preventDefault()

    const properties = {
      room: this.room.value,
      url: `https://${this.url.value}.hipchat.com/v2/room`,
      token: this.token.value,
    }

    this.props.onSave(properties)
  }

  handleTokenRef = r => (this.token = r)

  render() {
    const {options} = this.props.config
    const {url, room, token} = options

    const subdomain = url
      .replace('https://', '')
      .replace('.hipchat.com/v2/room', '')

    return (
      <form onSubmit={this.handleSaveAlert}>
        <div className="form-group col-xs-12">
          <label htmlFor="url">Subdomain</label>
          <input
            className="form-control"
            id="url"
            type="text"
            placeholder="your-subdomain"
            ref={r => (this.url = r)}
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
            ref={r => (this.room = r)}
            defaultValue={room || ''}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="token">
            Token
            <QuestionMarkTooltip tipID="token" tipContent={HIPCHAT_TOKEN_TIP} />
          </label>
          <RedactedInput
            defaultValue={token}
            id="token"
            refFunc={this.handleTokenRef}
          />
        </div>

        <div className="form-group-submit col-xs-12 text-center">
          <button className="btn btn-primary" type="submit">
            Update HipChat Config
          </button>
        </div>
      </form>
    )
  }
}

const {bool, func, shape, string} = PropTypes

HipchatConfig.propTypes = {
  config: shape({
    options: shape({
      room: string.isRequired,
      token: bool.isRequired,
      url: string.isRequired,
    }).isRequired,
  }).isRequired,
  onSave: func.isRequired,
}

export default HipchatConfig
