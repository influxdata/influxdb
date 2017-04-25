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

    let component = null
    if (defaultValue === true && !editing) {
      component = (
        <div>
          <span>
            Value set
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
            defaultValue={defaultValue || ''}
          />
        </div>
      )
    } else {
      component = (
        <input
          className="form-control"
          id={id}
          type="text"
          ref={refFunc}
          defaultValue={''}
        />
      )
    }
    return component
  }
}

const {bool, func, string} = PropTypes

RedactedInput.propTypes = {
  id: string.isRequired,
  defaultValue: bool.isRequired,
  refFunc: func.isRequired,
}

export default RedactedInput
