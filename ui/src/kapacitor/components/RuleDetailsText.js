import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class RuleDetailsText extends Component {
  constructor(props) {
    super(props)
  }

  handleUpdateDetails = e => {
    const {rule, updateDetails} = this.props
    updateDetails(rule.id, e.target.value)
  }

  render() {
    const {rule} = this.props
    return (
      <div className="rule-builder--details">
        <textarea
          className="form-control form-malachite monotype"
          onChange={this.handleUpdateDetails}
          placeholder="Enter the body for your email here. Can contain html"
          value={rule.details}
          spellCheck={false}
        />
      </div>
    )
  }
}

const {shape, func} = PropTypes

RuleDetailsText.propTypes = {
  rule: shape().isRequired,
  updateDetails: func.isRequired,
}

export default RuleDetailsText
