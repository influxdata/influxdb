import React, {PropTypes} from 'react'

const SensuConfig = React.createClass({
  propTypes: {
    config: PropTypes.shape({
      options: PropTypes.shape({
        source: PropTypes.string.isRequired,
        addr: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
    onSave: PropTypes.func.isRequired,
  },

  handleSaveAlert(e) {
    e.preventDefault()

    const properties = {
      source: this.source.value,
      addr: this.addr.value,
    }

    this.props.onSave(properties)
  },

  render() {
    const {source, addr} = this.props.config.options

    return (
      <form onSubmit={this.handleSaveAlert}>
        <div className="form-group col-xs-12 col-md-6">
          <label htmlFor="source">Source</label>
          <input
            className="form-control"
            id="source"
            type="text"
            ref={r => (this.source = r)}
            defaultValue={source || ''}
          />
        </div>

        <div className="form-group col-xs-12 col-md-6">
          <label htmlFor="address">Address</label>
          <input
            className="form-control"
            id="address"
            type="text"
            ref={r => (this.addr = r)}
            defaultValue={addr || ''}
          />
        </div>

        <div className="form-group-submit col-xs-12 text-center">
          <button className="btn btn-primary" type="submit">
            Update Sensu Config
          </button>
        </div>
      </form>
    )
  },
})

export default SensuConfig
