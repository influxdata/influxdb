// Libraries
import React, {FC, useContext} from 'react'
import {useParams} from 'react-router-dom'

// Contexts
import {WriteDataDetailsContext} from 'src/writeData/components/WriteDataDetailsContext'

// Components
import CodeSnippet from 'src/shared/components/CodeSnippet'

// Utils
import {event} from 'src/cloud/utils/reporting'

interface Props {
  code: string
  language: string
}

// NOTE: this is just a simplified form of the resig classic:
// https://johnresig.com/blog/javascript-micro-templating/
function transform(template, vars) {
  const output = new Function(
    'vars',
    'var output=' +
      JSON.stringify(template).replace(
        /<%=(.+?)%>/g,
        '"+(vars["$1".trim()])+"'
      ) +
      ';return output;'
  )
  return output(vars)
}

const WriteDataCodeSnippet: FC<Props> = ({code, language}) => {
  const {contentID} = useParams()
  const {bucket, token, origin, organization} = useContext(
    WriteDataDetailsContext
  )

  const vars = {
    token: token ? token.token : '<TOKEN GOES HERE>',
    bucket: bucket.name,
    server: origin,
    org: organization.name,
  }

  const copyText = transform(code, vars)

  const sendCopyEvent = (): void => {
    event('dataSources_copyCode', {dataSourceName: `${contentID}`})
  }

  return (
    <CodeSnippet copyText={copyText} label={language} onClick={sendCopyEvent} />
  )
}

export default WriteDataCodeSnippet
