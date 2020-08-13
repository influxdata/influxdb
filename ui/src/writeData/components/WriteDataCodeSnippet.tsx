// Libraries
import React, {FC, useContext} from 'react'

// Contexts
import {WriteDataDetailsContext} from 'src/writeData/components/WriteDataDetailsContext'

// Components
import CodeSnippet from 'src/shared/components/CodeSnippet'

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
  const {bucket, token, origin, organization} = useContext(
    WriteDataDetailsContext
  )

  const vars = {
    token: token.token,
    bucket: bucket.name,
    server: origin,
    org: organization.name,
  }

  const copyText = transform(code, vars)

  return <CodeSnippet copyText={copyText} label={language} />
}

export default WriteDataCodeSnippet
