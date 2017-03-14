import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import {loadDBsAndRPsAsync} from 'src/admin/actions'
import DatabaseManager from 'src/admin/components/DatabaseManager'

class DatabaseManagerPage extends Component {
  constructor(props) {
    super(props)
  }

  componentDidMount() {
    const {source: {links: {proxy}}, loadDBsAndRPs} = this.props

    loadDBsAndRPs(proxy)
  }

  render() {
    const {databases, retentionPolicies} = this.props

    return <DatabaseManager databases={databases} retentionPolicies={retentionPolicies} />
  }
}

const {
  array,
  arrayOf,
  func,
  shape,
  string,
} = PropTypes

DatabaseManagerPage.propTypes = {
  source: shape({
    links: shape({
      proxy: string,
    }),
  }),
  databases: arrayOf(string),
  retentionPolicies: array,
  loadDBsAndRPs: func,
}

const mapStateToProps = ({admin: {databases, retentionPolicies}}) => ({
  databases,
  retentionPolicies,
})

const mapDispatchToProps = (dispatch) => ({
  loadDBsAndRPs: bindActionCreators(loadDBsAndRPsAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(DatabaseManagerPage)
