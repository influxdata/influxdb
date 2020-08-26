// Libraries
import React, {PureComponent} from 'react'
import {MarkdownRenderer} from 'src/shared/components/views/MarkdownRenderer'
import {fetchReadMe} from 'src/templates/api'
import {reportError} from 'src/shared/utils/errors'

interface Props {
  directory: string
}

const cloudImageRenderer = (): any =>
  "We don't support images in markdown for security purposes"

export class CommunityTemplateReadme extends PureComponent<Props> {
  state = {readMeData: ''}
  componentDidMount = async () => {
    try {
      const response = await fetchReadMe(this.props.directory)
      this.setState({readMeData: response})
    } catch (error) {
      reportError(error, {
        name: 'The community template fetch github readme failed',
      })

      this.setState({
        readMeData: '## We cant find the readme associated with this template',
      })
    }
  }

  render = () => {
    return (
      <MarkdownRenderer
        text={this.state.readMeData}
        className="markdown-format"
        cloudRenderers={{
          image: cloudImageRenderer,
          imageReference: cloudImageRenderer,
        }}
      />
    )
  }
}
