import React, {SFC} from 'react'

interface Props {
  title: string
}

const PageTitle: SFC<Props> = ({title}) => (
  <h1 className="page--title">{title}</h1>
)

export default PageTitle
