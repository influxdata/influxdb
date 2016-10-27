import React, {PropTypes} from 'react';

const TelegramConfig = React.createClass({
  propTypes: {
    config: PropTypes.shape({
      options: PropTypes.shape({
        'chat-id': PropTypes.string.isRequired,
        'disable-notification': PropTypes.bool.isRequired,
        'disable-web-page-preview': PropTypes.bool.isRequired,
        global: PropTypes.bool.isRequired,
        'parse-mode': PropTypes.string.isRequired,
        'state-changes-only': PropTypes.bool.isRequired,
        token: PropTypes.bool.isRequired,
        url: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
    onSave: PropTypes.func.isRequired,
  },

  handleSaveAlert(e) {
    e.preventDefault();

    const properties = {
      chatID: this.chatID.value,
      disableNotification: this.disableNotification.checked,
      disableWebPagePreview: this.disableWebPagePreview.checked,
      global: this.global.checked,
      parseMode: this.parseMode.checked,
      stateChangesOnly: this.stateChangesOnly.checked,
      token: this.token.value,
      url: this.url.value,
    };

    this.props.onSave(properties);
  },

  render() {
    const {options} = this.props.config;
    const {global, url, token} = options;
    const chatID = options['chat-id'];
    const disableNotification = options['chat-id'];
    const disableWebPagePreview = options['disable-web-page-preview'];
    const parseMode = options['parse-mode'];
    const stateChangesOnly = options['state-changes-only'];

    return (
      <div className="panel-body">
        <h4 className="text-center">Telegram Alert</h4>
        <br/>
        <form onSubmit={this.handleSaveAlert}>
          <div className="row">
            <div className="col-xs-7 col-sm-8 col-sm-offset-2">
              <p>
                You can have alerts sent to Telegram by entering info below.
              </p>

              <div className="form-group">
                <label htmlFor="url">Telegram URL</label>
                <input className="form-control" id="url" type="text" ref={(r) => this.url = r} defaultValue={url || ''}></input>
              </div>

              <div className="form-group">
                <label htmlFor="token">Token</label>
                <input className="form-control" id="token" type="text" ref={(r) => this.token = r} defaultValue={token || ''}></input>
                <span>Note: a value of <code>true</code> indicates the Telegram token has been set</span>
              </div>

              <div className="form-group">
                <label htmlFor="chat-id">Chat ID</label>
                <input className="form-control" id="chat-id" type="text" ref={(r) => this.chatID = r} defaultValue={chatID || ''}></input>
              </div>

              <div className="form-group col-xs-12">
                <div className="checkbox">
                  <label>
                    <input type="checkbox" defaultChecked={parseMode} ref={(r) => this.parseMode = r} />
                    Enable Parse Mode
                  </label>
                </div>
              </div>

              <div className="form-group col-xs-12">
                <div className="checkbox">
                  <label>
                    <input type="checkbox" defaultChecked={disableWebPagePreview} ref={(r) => this.disableWebPagePreview = r} />
                    Disable Web Page Preview
                  </label>
                </div>
              </div>

              <div className="form-group col-xs-12">
                <div className="checkbox">
                  <label>
                    <input type="checkbox" defaultChecked={disableNotification} ref={(r) => this.disableNotification = r} />
                    Disable Notification
                  </label>
                </div>
              </div>

              <div className="form-group col-xs-12">
                <div className="checkbox">
                  <label>
                    <input type="checkbox" defaultChecked={global} ref={(r) => this.global = r} />
                    Send all alerts without marking them explicitly in TICKscript
                  </label>
                </div>
              </div>

              <div className="form-group col-xs-12">
                <div className="checkbox">
                  <label>
                    <input type="checkbox" defaultChecked={stateChangesOnly} ref={(r) => this.stateChangesOnly = r} />
                    Send alerts on state change only
                  </label>
                </div>
              </div>

            </div>
          </div>

          <hr />
          <div className="row">
            <div className="form-group col-xs-5 col-sm-3 col-sm-offset-2">
              <button className="btn btn-block btn-primary" type="submit">Save</button>
            </div>
          </div>
        </form>
      </div>
    );
  },
});

export default TelegramConfig;
