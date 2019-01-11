export const downloadTextFile = (
  text: string,
  filename: string,
  mimeType: string = 'text/plain'
) => {
  const blob = new Blob([text], {type: mimeType})
  const a = document.createElement('a')

  a.href = window.URL.createObjectURL(blob)
  a.target = '_blank'
  a.download = filename

  document.body.appendChild(a)
  a.click()
  a.parentNode.removeChild(a)
}
