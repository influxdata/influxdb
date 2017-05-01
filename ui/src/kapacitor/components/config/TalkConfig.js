import React, {PropTypes} from 'react'

import RedactedInput from './RedactedInput'

const {bool, string, shape, func} = PropTypes

const TalkConfig = React.createClass({
  propTypes: {
    config: shape({
      options: shape({
        url: bool.isRequired,
        author_name: string.isRequired,
      }).isRequired,
    }).isRequired,
    onSave: func.isRequired,
  },

  handleSaveAlert(e) {
    e.preventDefault()

    const properties = {
      url: this.url.value,
      author_name: this.author.value,
    }

    this.props.onSave(properties)
  },

  render() {
    const {url, author_name: author} = this.props.config.options

    return (
      <form onSubmit={this.handleSaveAlert}>
        <div className="form-group col-xs-12">
          <label htmlFor="url">URL</label>
          <RedactedInput
            defaultValue={url}
            id="url"
            refFunc={r => (this.url = r)}
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

        <div className="form-group form-group-submit col-xs-12 col-sm-6 col-sm-offset-3">
          <button className="btn btn-block btn-primary" type="submit">
            Save
          </button>
        </div>
      </form>
    )
  },
})

export default TalkConfig
