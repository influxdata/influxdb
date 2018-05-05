import React, {PureComponent} from 'react'

import RedactedInput from 'src/kapacitor/components/config/RedactedInput'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Properties {
  url: string
  author_name: string
}

interface Config {
  options: {
    url: boolean
    author_name: string
  }
}

interface State {
  testEnabled: boolean
}

interface Props {
  config: Config
  onSave: (properties: Properties) => void
  onTest: (event: React.MouseEvent<HTMLButtonElement>) => void
  enabled: boolean
}

@ErrorHandling
class TalkConfig extends PureComponent<Props, State> {
  private url: HTMLInputElement
  private author: HTMLInputElement

  constructor(props) {
    super(props)
    this.state = {
      testEnabled: this.props.enabled,
    }
  }

  public render() {
    const {url, author_name: author} = this.props.config.options
    const {testEnabled} = this.state

    return (
      <form onSubmit={this.handleSubmit}>
        <div className="form-group col-xs-12">
          <label htmlFor="url">URL</label>
          <RedactedInput
            defaultValue={url}
            id="url"
            refFunc={this.handleUrlRef}
            disableTest={this.disableTest}
            isFormEditing={!testEnabled}
          />
        </div>

        <div className="form-group col-xs-12">
          <label htmlFor="author">Author Name</label>
          <input
            className="form-control"
            id="author"
            type="text"
            ref={r => (this.author = r)}
            defaultValue={author || ''}
            onChange={this.disableTest}
          />
        </div>

        <div className="form-group form-group-submit col-xs-12 text-center">
          <button
            className="btn btn-primary"
            type="submit"
            disabled={this.state.testEnabled}
          >
            <span className="icon checkmark" />
            Save Changes
          </button>
          <button
            className="btn btn-primary"
            disabled={!this.state.testEnabled}
            onClick={this.props.onTest}
          >
            <span className="icon pulse-c" />
            Send Test Alert
          </button>
        </div>
      </form>
    )
  }

  private handleSubmit = async e => {
    e.preventDefault()

    const properties = {
      url: this.url.value,
      author_name: this.author.value,
    }

    const success = await this.props.onSave(properties)
    if (success) {
      this.setState({testEnabled: true})
    }
  }

  private disableTest = () => {
    this.setState({testEnabled: false})
  }

  private handleUrlRef = r => (this.url = r)
}

export default TalkConfig
