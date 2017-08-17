import React, {PropTypes, Component} from 'react'

import RedactedInput from './RedactedInput'

class TalkConfig extends Component {
  constructor(props) {
    super(props)
  }

  handleSaveAlert = e => {
    e.preventDefault()

    const properties = {
      url: this.url.value,
      author_name: this.author.value,
    }

    this.props.onSave(properties)
  }

  handleUrlRef = r => (this.url = r)

  render() {
    const {url, author_name: author} = this.props.config.options

    return (
      <form onSubmit={this.handleSaveAlert}>
        <div className="form-group col-xs-12">
          <label htmlFor="url">URL</label>
          <RedactedInput
            defaultValue={url}
            id="url"
            refFunc={this.handleUrlRef}
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
          />
        </div>

        <div className="form-group-submit col-xs-12 text-center">
          <button className="btn btn-primary" type="submit">
            Update Talk Config
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
}

export default TalkConfig
