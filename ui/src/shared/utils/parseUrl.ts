export const urlIsValid = (url: string) => {
  try {
    new URL(url)
    return true
  } catch {
    return false
  }
}

export const parseUrl = (url: string) => {
  if (!url) {
    url = window.location.href
  }
  if (urlIsValid(url)) {
    const uri = new URL(url)
    return `${uri.host}${uri.pathname}`
  }
  return url
}
