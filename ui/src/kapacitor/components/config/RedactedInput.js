import React, {Component, PropTypes} from 'react'

class RedactedInput extends Component {
  constructor(props) {
    super(props)
    this.state = {
      editing: false,
    }
  }

  render() {
    const {defaultValue, id, refFunc} = this.props
    const {editing} = this.state

    if (defaultValue === true && !editing) {
      return (
        <div className="alert-value-set">
          <span>
            value set
            <a
              href="#"
              onClick={() => {
                this.setState({editing: true})
              }}
            >
              (change it)
            </a>
          </span>
          <input
            className="form-control"
            id={id}
            type="hidden"
            ref={refFunc}
            defaultValue={defaultValue}
          />
        </div>
      )
    }

    return (
      <input
        className="form-control"
        id={id}
        type="text"
        ref={refFunc}
        defaultValue={''}
      />
    )
  }
}

const {bool, func, string} = PropTypes

RedactedInput.propTypes = {
  id: string.isRequired,
  defaultValue: bool,
  refFunc: func.isRequired,
}

export default RedactedInput
