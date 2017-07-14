import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import Tickscript from 'src/kapacitor/components/Tickscript'
import * as kapactiorActionCreators from 'src/kapacitor/actions/view'
import {getActiveKapacitor} from 'src/shared/apis'
import {errorThrown as errorAction} from 'shared/actions/errors'

class TickscriptPage extends Component {
  constructor(props) {
    super(props)
    this.state = {
      kapacitor: {},
      task: {
        id: 'testing',
        status: 'enabled',
        script: '',
        dbsrps: [
          {
            db: '_internal',
            rp: 'monitor',
          },
        ],
        type: 'stream',
      },
      validation: '',
    }
  }

  async componentDidMount() {
    const {source, errorThrown} = this.props
    const kapacitor = await getActiveKapacitor(source)

    if (!kapacitor) {
      errorThrown('We could not find a configured Kapacitor for this source')
    }

    this.setState({kapacitor})
  }

  async handleSave() {
    const {kapacitor, task} = this.state
    const {source, router, kapactiorActions: {createTask}} = this.props

    const response = await createTask(kapacitor, task)
    if (response && response.error) {
      return this.setState({validation: response.error})
    }

    router.push(`/sources/${source.id}/alert-rules`)
  }

  handleChangeScript(script) {
    this.setState({task: {...this.state.task, script}})
  }

  render() {
    const {source} = this.props
    const {task, validation} = this.state

    return (
      <Tickscript
        task={task}
        source={source}
        onSave={::this.handleSave}
        onChangeScript={::this.handleChangeScript}
        validation={validation}
      />
    )
  }
}

const {func, shape, string} = PropTypes

TickscriptPage.propTypes = {
  source: shape({
    name: string,
  }),
  errorThrown: func.isRequired,
  kapactiorActions: shape({
    createTask: func.isRequired,
  }),
  router: shape({
    push: func.isRequired,
  }).isRequired,
}

const mapStateToProps = () => {
  return {}
}

const mapDispatchToProps = dispatch => ({
  errorThrown: bindActionCreators(errorAction, dispatch),
  kapactiorActions: bindActionCreators(kapactiorActionCreators, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(TickscriptPage)
