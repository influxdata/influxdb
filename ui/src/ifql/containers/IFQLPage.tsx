import React, {PureComponent} from 'react'

import {connect} from 'react-redux'

import TimeMachine from 'src/ifql/components/TimeMachine'

import {getSuggestions} from 'src/ifql/apis'

interface Links {
  self: string
  suggestions: string
}

interface Props {
  links: Links
}

interface State {
  funcs: string[]
}

export class IFQLPage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      funcs: [],
    }
  }

  public async componentDidMount() {
    const {suggestions} = this.props.links

    try {
      const results = await getSuggestions(suggestions)
      const funcs = results.map(s => s.name)
      this.setState({funcs})
    } catch (error) {
      console.error('Could not get function suggestions: ', error)
    }
  }

  public render() {
    return (
      <div className="page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">Time Machine</h1>
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <TimeMachine funcs={this.state.funcs} />
          </div>
        </div>
      </div>
    )
  }
}

const mapStateToProps = ({links}) => {
  return {links: links.ifql}
}

export default connect(mapStateToProps, null)(IFQLPage)
