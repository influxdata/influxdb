import React, {PropTypes, Component} from 'react'
import QuestionMarkTooltip from 'shared/components/QuestionMarkTooltip'
import {TELEGRAM_CHAT_ID_TIP, TELEGRAM_TOKEN_TIP} from 'src/kapacitor/copy'

import RedactedInput from './RedactedInput'

class TelegramConfig extends Component {
  constructor(props) {
    super(props)
  }

  handleSaveAlert = e => {
    e.preventDefault()

    let parseMode
    if (this.parseModeHTML.checked) {
      parseMode = 'HTML'
    }
    if (this.parseModeMarkdown.checked) {
      parseMode = 'Markdown'
    }

    const properties = {
      'chat-id': this.chatID.value,
      'disable-notification': this.disableNotification.checked,
      'disable-web-page-preview': this.disableWebPagePreview.checked,
      'parse-mode': parseMode,
      token: this.token.value,
    }

    this.props.onSave(properties)
  }

  handleTokenRef = r => (this.token = r)

  render() {
    const {options} = this.props.config
    const {token} = options
    const chatID = options['chat-id']
    const disableNotification = options['disable-notification']
    const disableWebPagePreview = options['disable-web-page-preview']
    const parseMode = options['parse-mode']

    return (
      <form onSubmit={this.handleSaveAlert}>
        <div className="form-group col-xs-12">
          <div className="alert alert-warning alert-icon no-user-select">
            <span className="icon triangle" />
            You need a{' '}
            <a
              href="https://docs.influxdata.com/kapacitor/latest/guides/event-handler-setup/#telegram-setup"
              target="_blank"
            >
              Telegram Bot
            </a>{' '}
            to use this endpoint
          </div>
        </div>
        <div className="form-group col-xs-12">
          <label htmlFor="token">
            Token
            <QuestionMarkTooltip
              tipID="token"
              tipContent={TELEGRAM_TOKEN_TIP}
            />
          </label>
          <RedactedInput
            defaultValue={token}
            id="token"
            refFunc={this.handleTokenRef}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="chat-id">
            Chat ID
            <QuestionMarkTooltip
              tipID="chat-id"
              tipContent={TELEGRAM_CHAT_ID_TIP}
            />
          </label>
          <input
            className="form-control"
            id="chat-id"
            type="text"
            placeholder="your-telegram-chat-id"
            ref={r => (this.chatID = r)}
            defaultValue={chatID || ''}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="parseMode">Select the alert message format</label>
          <div className="form-control-static">
            <div className="radio-item">
              <input
                id="parseModeMarkdown"
                type="radio"
                name="parseMode"
                value="markdown"
                defaultChecked={parseMode !== 'HTML'}
                ref={r => (this.parseModeMarkdown = r)}
              />
              <label htmlFor="parseModeMarkdown">Markdown</label>
            </div>
            <div className="radio-item">
              <input
                id="parseModeHTML"
                type="radio"
                name="parseMode"
                value="html"
                defaultChecked={parseMode === 'HTML'}
                ref={r => (this.parseModeHTML = r)}
              />
              <label htmlFor="parseModeHTML">HTML</label>
            </div>
          </div>
        </div>

        <div className="form-group col-xs-12">
          <div className="form-control-static">
            <input
              id="disableWebPagePreview"
              type="checkbox"
              defaultChecked={disableWebPagePreview}
              ref={r => (this.disableWebPagePreview = r)}
            />
            <label htmlFor="disableWebPagePreview">
              Disable{' '}
              <a href="https://telegram.org/blog/link-preview" target="_blank">
                link previews
              </a>{' '}
              in alert messages.
            </label>
          </div>
        </div>

        <div className="form-group col-xs-12">
          <div className="form-control-static">
            <input
              id="disableNotification"
              type="checkbox"
              defaultChecked={disableNotification}
              ref={r => (this.disableNotification = r)}
            />
            <label htmlFor="disableNotification">
              Disable notifications on iOS devices and disable sounds on Android
              devices. Android users continue to receive notifications.
            </label>
          </div>
        </div>

        <div className="form-group-submit col-xs-12 text-center">
          <button className="btn btn-primary" type="submit">
            Update Telegram Config
          </button>
        </div>
      </form>
    )
  }
}

const {bool, func, shape, string} = PropTypes

TelegramConfig.propTypes = {
  config: shape({
    options: shape({
      'chat-id': string.isRequired,
      'disable-notification': bool.isRequired,
      'disable-web-page-preview': bool.isRequired,
      'parse-mode': string.isRequired,
      token: bool.isRequired,
    }).isRequired,
  }).isRequired,
  onSave: func.isRequired,
}

export default TelegramConfig
