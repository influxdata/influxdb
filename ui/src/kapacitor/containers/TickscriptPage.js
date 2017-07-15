import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import Tickscript from 'src/kapacitor/components/Tickscript'
import * as kapactiorActionCreators from 'src/kapacitor/actions/view'
import * as errorActionCreators from 'shared/actions/errors'
import {getActiveKapacitor} from 'src/shared/apis'

// TODO: collect dbsrps, stream, and name for tasks (needs design)
class TickscriptPage extends Component {
  constructor(props) {
    super(props)
    this.state = {
      kapacitor: {},
      task: {
        id: 'testing',
        status: 'enabled',
        script: '',
        dbrps: [
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
    const {
      source,
      errorActions,
      kapacitorActions,
      params: {ruleID},
    } = this.props

    const kapacitor = await getActiveKapacitor(source)
    if (!kapacitor) {
      errorActions.errorThrown(
        'We could not find a configured Kapacitor for this source'
      )
    }

    if (this.isEditing()) {
      await kapacitorActions.getRule(kapacitor, ruleID)
      const activeRule = this.props.rules.find(r => r.id === ruleID)
      this.setState({task: {...this.state.task, script: activeRule.tickscript}})
    }

    this.setState({kapacitor})
  }

  async handleSave() {
    const {kapacitor, task} = this.state
    const {
      source,
      router,
      kapacitorActions: {createTask, updateTask},
      params: {ruleID},
    } = this.props

    let response
    if (this.isEditing()) {
      response = await updateTask(kapacitor, task, ruleID)
    } else {
      response = await createTask(kapacitor, task)
    }

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

  isEditing() {
    const {params} = this.props
    return params.ruleID && params.ruleID !== 'new'
  }
}

const {arrayOf, func, shape, string} = PropTypes

TickscriptPage.propTypes = {
  source: shape({
    name: string,
  }),
  errorActions: shape({
    errorThrown: func.isRequired,
  }).isRequired,
  kapacitorActions: shape({
    updateTask: func.isRequired,
    createTask: func.isRequired,
    getRule: func.isRequired,
  }),
  router: shape({
    push: func.isRequired,
  }).isRequired,
  params: shape({
    ruleID: string.isRequired,
  }).isRequired,
  rules: arrayOf(shape()),
}

const mapStateToProps = state => {
  return {
    rules: Object.values(state.rules),
  }
}

const mapDispatchToProps = dispatch => ({
  kapacitorActions: bindActionCreators(kapactiorActionCreators, dispatch),
  errorActions: bindActionCreators(errorActionCreators, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(TickscriptPage)
