const Conditional = ({
  isRendered,
  children,
}: {
  isRendered: boolean
  children: JSX.Element
}): JSX.Element => {
  if (isRendered) {
    return children
  }

  return null
}

export default Conditional
