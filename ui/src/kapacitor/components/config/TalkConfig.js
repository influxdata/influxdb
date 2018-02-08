import React, {PropTypes, Component} from 'react'

import RedactedInput from './RedactedInput'

class TalkConfig extends Component {
  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
    }
  }

  handleSubmit = async e => {
    e.preventDefault()

    const properties = {
      url: this.url.value,
      author_name: this.author.value,
    }

    const success = await this.props.onSave(properties)
    if (success) {
      this.setState({testEnabled: true})
    }
  }

  disableTest = () => {
    this.setState({testEnabled: false})
  }

  handleUrlRef = r => (this.url = r)

  render() {
    const {url, author_name: author} = this.props.config.options

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12">
          <label htmlFor="url">URL</label>
          <RedactedInput
            defaultValue={url}
            id="url"
            refFunc={this.handleUrlRef}
            disableTest={this.disableTest}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="author">Author Name</label>
          <input
            className="form-control"
            id="author"
            type="text"
            ref={r => (this.author = r)}
            defaultValue={author || ''}
            onChange={this.disableTest}
          />
        </div>

        <div className="form-group-submit col-xs-12 text-center">
          <button
            className="btn btn-primary"
            type="submit"
            disabled={this.state.testEnabled}
          >
            <span className="icon checkmark" />
            Save Changes
          </button>
          <button
            className="btn btn-primary"
            disabled={!this.state.testEnabled}
            onClick={this.props.onTest}
          >
            <span className="icon pulse-c" />
            Send Test Alert
          </button>
        </div>
      </form>
    )
  }
}

const {bool, string, shape, func} = PropTypes

TalkConfig.propTypes = {
  config: shape({
    options: shape({
      url: bool.isRequired,
      author_name: string.isRequired,
    }).isRequired,
  }).isRequired,
  onSave: func.isRequired,
  onTest: func.isRequired,
  enabled: bool.isRequired,
}

export default TalkConfig
