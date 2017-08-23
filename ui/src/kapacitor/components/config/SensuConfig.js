import React, {PropTypes, Component} from 'react'

class SensuConfig extends Component {
  constructor(props) {
    super(props)
  }

  handleSaveAlert = e => {
    e.preventDefault()

    const properties = {
      source: this.source.value,
      addr: this.addr.value,
    }

    this.props.onSave(properties)
  }

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
  }
}

const {func, shape, string} = PropTypes

SensuConfig.propTypes = {
  config: shape({
    options: shape({
      source: string.isRequired,
      addr: string.isRequired,
    }).isRequired,
  }).isRequired,
  onSave: func.isRequired,
}

export default SensuConfig
