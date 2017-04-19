import React, {PropTypes} from 'react'

const {
  bool,
  string,
  shape,
  func,
} = PropTypes

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
      <div>
        <h4 className="text-center no-user-select">Talk Alert</h4>
        <br/>
        <p className="no-user-select">Have alerts sent to Talk.</p>
        <form onSubmit={this.handleSaveAlert}>
          <div className="form-group col-xs-12">
            <label htmlFor="url">URL</label>
            <input className="form-control" id="url" type="text" ref={(r) => this.url = r} defaultValue={url || ''}></input>
            <label className="form-helper">Note: a value of <code>true</code> indicates that the Talk URL has been set</label>
          </div>

          <div className="form-group col-xs-12">
            <label htmlFor="author">Author Name</label>
            <input className="form-control" id="author" type="text" ref={(r) => this.author = r} defaultValue={author || ''}></input>
          </div>

          <div className="form-group form-group-submit col-xs-12 col-sm-6 col-sm-offset-3">
            <button className="btn btn-block btn-primary" type="submit">Save</button>
          </div>
        </form>
      </div>
    )
  },
})

export default TalkConfig
